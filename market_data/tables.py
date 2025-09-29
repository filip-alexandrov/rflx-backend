from databento import Historical, SType
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import HTTPException
from fastapi.responses import JSONResponse
import pandas as pd 
from py_vollib.black_scholes.implied_volatility import implied_volatility
from scipy.optimize import brentq, minimize_scalar

# calculate chart settings (ranges of the axis)
def solve_minx(x_anchor, y_anchor, x_max, y_min, y_max):
    """Solve for min_x given y_max (from anchor equation)."""
    return x_anchor - (x_max) * (y_anchor - y_min) / (y_max - y_min)

def ratio(y_max, x_anchor, y_anchor, x_max, y_min):
    """Compute ratio for given y_max, using implied min_x."""
    min_x = solve_minx(x_anchor, y_anchor, x_max, y_min, y_max)
    if min_x <= 0 or min_x >= x_anchor:
        return None, None
    r = (x_max - x_anchor) / (x_max - min_x)
    return r, min_x

def find_solution(x_anchor, y_anchor, x_max, y_min, target=0.75):
    # Define function for root-finding
    def f(y_max):
        r, min_x = ratio(y_max, x_anchor, y_anchor, x_max, y_min)
        if r is None:  # infeasible
            return 1e6
        return r - target

    try:
        y_max = brentq(f, 1e-6, 20.0)  # bounded interval
        r, min_x = ratio(y_max, x_anchor, y_anchor, x_max, y_min)
        return min_x, y_max, r
    except ValueError:
        # No exact root → find closest solution with minimize_scalar
        def loss(y_max):
            r, _ = ratio(y_max, x_anchor, y_anchor, x_max, y_min)
            if r is None:
                return 1e6
            return (r - target) ** 2

        res = minimize_scalar(loss, bounds=(1e-6, 20.0), method="bounded")
        y_max = res.x
        r, min_x = ratio(y_max, x_anchor, y_anchor, x_max, y_min)
        return min_x, y_max, r

def date_and_interval_validation(startDate, endDate, interval):
    # validate dates and interval
    try:
        start_date = datetime.strptime(startDate, "%Y-%m-%d %H:%M")
        end_date = datetime.strptime(endDate, "%Y-%m-%d %H:%M")
    except ValueError:
        # try again
        try: 
            start_date = datetime.strptime(startDate, "%Y-%m-%d")
            end_date = datetime.strptime(endDate, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD HH:MM.")
    
    if start_date >= end_date:
        raise HTTPException(status_code=400, detail="Start date must be before end date.")
    
    interval = interval.lower()
    if interval not in ["d", "h", "m", "s"]:
        raise HTTPException(status_code=400, detail="Invalid interval. Use D, H, M, or S.")
    
    # start / end date must be in UTC, but user input is in New York time
    nyc_tz = ZoneInfo("America/New_York")
    utc_tz = ZoneInfo("UTC")

    start_date = start_date.replace(tzinfo=nyc_tz)
    start_date = start_date.astimezone(utc_tz)
    
    end_date = end_date.replace(tzinfo=nyc_tz)
    end_date = end_date.astimezone(utc_tz)
    
    # check how many intervals are in the range
    delta = end_date - start_date
    if interval == "d":
        num_intervals = delta.days
    elif interval == "h":
        num_intervals = delta.days * 24 + delta.seconds // 3600
    elif interval == "m":
        num_intervals = delta.days * 24 * 60 + delta.seconds // 60
    elif interval == "s":
        num_intervals = delta.days * 24 * 60 * 60 + delta.seconds
    if num_intervals > 10000:
        raise HTTPException(status_code=400, detail="Too many intervals. Limit is 10,000.")
    
    return start_date, end_date, interval

def decode_option_ticker(option_ticker): 
    option_ticker = option_ticker.upper()
    
    underlying_ticker = option_ticker[:6].strip()

    y = option_ticker[6:8]
    m = option_ticker[8:10]
    d = option_ticker[10:12]
    t = option_ticker[12:13] # C or P
    dollars = option_ticker[13:18]
    cents = option_ticker[18:]
    
    # construct date object 
    expiration_date = f"20{y}-{m}-{d} 16:00:00" # expiration is at 4pm eastern time
    # ensure date is in eastern time
    expiration_date = datetime.strptime(expiration_date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("America/New_York"))
    
    # strike price 
    strike_price = int(dollars) + int(cents) / 1000
    
    return option_ticker, underlying_ticker, expiration_date, strike_price, t

def fetch_multi_iv(raw_opt_tickers, start_date, end_date): 
    client = Historical("db-UVPEpgVxgduVKFnQ9tgEp6cSTL65H") 

    underlying_ticker = ""
    option_tickers_parsed = []

    for raw_ticker in raw_opt_tickers:
        option_ticker, underlying_ticker, expiration_date, strike_price, t = decode_option_ticker(raw_ticker)

        option_tickers_parsed.append({
            "option_ticker": option_ticker,
            "trace_name": f"{expiration_date.strftime('%Y-%m-%d')} {t} {strike_price}",
            "underlying_ticker": underlying_ticker,
            "expiration_date": expiration_date,
            "strike_price": strike_price,
            "type": t
        })


    # move to UTC 
    nyc_tz = ZoneInfo("America/New_York")
    utc_tz = ZoneInfo("UTC")

    # convert to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M")
    end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M")

    start_date = start_date.replace(tzinfo=nyc_tz)
    start_date = start_date.astimezone(utc_tz)

    end_date = end_date.replace(tzinfo=nyc_tz)
    end_date = end_date.astimezone(utc_tz)

    # max 5 days
    if end_date - start_date > pd.Timedelta(days=5):
        raise HTTPException(status_code=400, detail="Maximum range is 30 minutes.")
    
    df_underlying = client.timeseries.get_range(
        dataset="XNAS.ITCH",
        schema=f"trades",
        symbols=underlying_ticker,
        start=start_date,
        end=end_date,
    ).to_df()

    full_data = {"options": [], "underlying": []}

    for row in df_underlying.itertuples():
        row_date = row.ts_event # in UTC
        row_date = row_date.replace(tzinfo=ZoneInfo("UTC"))
        
        # convert to eastern time
        row_date = row_date.astimezone(ZoneInfo("America/New_York"))
        row_date_str = row_date.strftime("%Y-%m-%d %H:%M:%S.%f")

        full_data["underlying"].append({
            "ts_event": row_date_str,
            "price": row.price,
            "size": row.size,
        })
    

    for option_ticker_dict in option_tickers_parsed:
        option_ticker = option_ticker_dict["option_ticker"]

        df_option = client.timeseries.get_range(
            dataset="OPRA.PILLAR",
            schema="trades",
            symbols=option_ticker,
            start=start_date,
            end=end_date).to_df()
        
        df3 = pd.merge_asof(
            df_option,
            df_underlying,
            on='ts_event',
            direction='nearest'
        )

        
        opt_pricing_data = []

        for row in df3.itertuples():
            row_date = row.ts_event
            row_date = row_date.replace(tzinfo=ZoneInfo("UTC"))
            row_date = row_date.astimezone(ZoneInfo("America/New_York"))
            
            # include milliseconds
            row_date_str = row_date.strftime("%Y-%m-%d %H:%M:%S.%f")
            
            expiration_date = option_ticker_dict["expiration_date"]
            hours_until_expiration = (expiration_date - row_date).total_seconds() / 3600
            
            years_until_expiration = hours_until_expiration / (24 * 365.25) 
            
            if pd.isna(row.price_x) or pd.isna(row.price_y):
                continue
                
            try:
                iv = implied_volatility(
                    row.price_x,
                    row.price_y,
                    option_ticker_dict["strike_price"],
                    years_until_expiration,
                    0.04,
                    option_ticker_dict['type'].lower())
            except Exception as e:
                iv = 0

            opt_pricing_data.append({
                                "ts_event": row_date_str,
                                "price": row.price_x,
                                "size": row.size_x,
                                "underlying_price": row.price_y,
                                "iv": iv})

        full_data["options"].append({"contract": option_ticker_dict['trace_name'], "data": opt_pricing_data})

    return full_data


# Opt. NBBO HF Underlying + IV
def fetch_hf_iv(option_ticker, startDate, endDate): 
    client = Historical("db-UVPEpgVxgduVKFnQ9tgEp6cSTL65H") 
    
    # get the underlying 
    option_ticker, underlying_ticker, expiration_date, strike_price, t = decode_option_ticker(option_ticker)
    
    start_date, end_date, _ = date_and_interval_validation(startDate, endDate, "m")
    
    # max 30 minutes 
    if end_date - start_date > pd.Timedelta(minutes=30):
        raise HTTPException(status_code=400, detail="Maximum range is 30 minutes.")
    
    global_data = {
        "expiration_date" : expiration_date.strftime("%Y-%m-%d %H:%M:%S"), 
        "underlying_ticker": underlying_ticker,
        "option_ticker": option_ticker, 
        "strike_price": format(strike_price, ".2f")
    }

    try: 
        df_underlying = client.timeseries.get_range(
            dataset="XNAS.ITCH",
            schema=f"mbp-1",
            symbols=underlying_ticker,
            start=start_date,
            end=end_date,
        ).to_df()
        
        df_option = client.timeseries.get_range(
            dataset="OPRA.PILLAR",
            schema=f"cmbp-1",
            symbols=option_ticker,
            start=start_date,
            end=end_date,
        ).to_df()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from Databento: {e}")
        
    # data
    opt_bid = []
    opt_ask = []
    opt_trades = []
    
    und_bid = []
    und_ask = []
    und_trades = []
    
    opt_iv = []
    
    for row in df_underlying.itertuples():
        row_date = row.ts_event # in UTC
        row_date = row_date.replace(tzinfo=ZoneInfo("UTC"))
        
        # convert to eastern time
        row_date = row_date.astimezone(ZoneInfo("America/New_York"))
        row_date_str = row_date.strftime("%Y-%m-%d %H:%M:%S.%f")

        if row.action == 'T' and pd.isna(row.price) == False:
            und_trades.append({
                "ts_event": row_date_str,
                "price": row.price,
                "size": row.size,
            })
        else: 
            if pd.isna(row.bid_px_00) == False:
                und_bid.append({
                    "ts_event": row_date_str,
                    "price": row.bid_px_00,
                    "size": row.bid_sz_00,
                })
                
            if pd.isna(row.ask_px_00) == False:
                und_ask.append({
                    "ts_event": row_date_str,
                    "price": row.ask_px_00,
                    "size": row.ask_sz_00,
                })
                
    for row in df_option.itertuples():
        row_date = row.ts_event
        row_date = row_date.replace(tzinfo=ZoneInfo("UTC"))
        
        row_date = row_date.astimezone(ZoneInfo("America/New_York"))
        row_date_str = row_date.strftime("%Y-%m-%d %H:%M:%S.%f")

        if row.action == 'T':
            continue
        else: 
            if pd.isna(row.bid_px_00) == False:
                opt_bid.append({
                    "ts_event": row_date_str,
                    "price": row.bid_px_00,
                    "size": row.bid_sz_00,
                })

            if pd.isna(row.ask_px_00) == False:
                opt_ask.append({
                    "ts_event": row_date_str,
                    "price": row.ask_px_00,
                    "size": row.ask_sz_00,
                })
            
        
    # select only trades
    df_underlying_trades = df_underlying[df_underlying['action'] == 'T']
    df_option_trades = df_option[df_option['action'] == 'T']
        
    df3 = pd.merge_asof(
        df_option_trades,
        df_underlying_trades,
        on='ts_event',
        direction='nearest'
    )
        
    for row in df3.itertuples():
        row_date = row.ts_event
        row_date = row_date.replace(tzinfo=ZoneInfo("UTC"))
        row_date = row_date.astimezone(ZoneInfo("America/New_York"))
        
        # include milliseconds
        row_date_str = row_date.strftime("%Y-%m-%d %H:%M:%S.%f")
        
        hours_until_expiration = (expiration_date - row_date).total_seconds() / 3600
        
        years_until_expiration = hours_until_expiration / (24 * 365.25) 
        
        if pd.isna(row.price_x) or pd.isna(row.price_y):
            continue
        
        try:
            iv = implied_volatility(
                row.price_x,
                row.price_y,
                strike_price,
                years_until_expiration,
                0.04,
                t.lower())
        except Exception as e:
            iv = 0
        
        opt_trades.append({"ts_event": row_date_str,
                           "price": row.price_x,
                           "size": row.size_x,
                           "underlying_price": row.price_y,
                           "iv": iv})
        
        opt_iv.append({"ts_event": row_date_str,
                       "iv": iv})
        
    # calculate chart settings
    min_iv = 0 # const 
    max_iv = max([float(x['iv']) for x in opt_iv]) if len(opt_iv) > 0 else 1
    
    min_opt_price = min([float(x['price']) for x in opt_bid]) if len(opt_bid) > 0 else 0
    max_opt_price = max([float(x['price']) for x in opt_ask]) * 1.1 if len(opt_ask) > 0 else 1
    
    chart_opt_price_min, chart_iv_max, r = find_solution(
        x_anchor = min_opt_price,
        y_anchor = max_iv,
        x_max = max_opt_price,
        y_min = min_iv,
        target = 0.7
    )
    
        
    return JSONResponse(content={
        "opt_chart_settings": { 
            "chart_opt_price_min": chart_opt_price_min, 
            "chart_opt_price_max": max_opt_price,
            "chart_iv_min": min_iv,
            "chart_iv_max": chart_iv_max,
        }, 
        "global_data": global_data,
        "option_bid": opt_bid,
        "option_ask": opt_ask,
        "option_trades": opt_trades,
        "underlying_bid": und_bid,
        "underlying_ask": und_ask,
        "underlying_trades": und_trades,
        "option_iv": opt_iv
    })
    

# Eq. OHLCV LF
def equity_lf(ticker, startDate, endDate, interval): 
    client = Historical("db-UVPEpgVxgduVKFnQ9tgEp6cSTL65H") 
    
    ticker = ticker.upper()
    
    start_date, end_date, interval = date_and_interval_validation(startDate, endDate, interval)
    
    if len(ticker) > 4: 
        try:
            df = client.timeseries.get_range(
                dataset="OPRA.PILLAR",
                schema=f"ohlcv-1{interval}",
                symbols=ticker,
                start=start_date,
                end=end_date,
            ).to_df()
            
            # if multiple equal timestamps, find their high and low 
            # set open to the average of the open 
            # set close to the average of the close
            df = df.groupby(df.index).agg({
                'open': 'mean',
                'high': 'max',
                'low': 'min',
                'close': 'mean',
                'volume': 'sum'
            })
         
            
        except Exception as e:
            print(e)
            raise HTTPException(status_code=404, detail=f"Bento error.")
    else:
        try:
            df = client.timeseries.get_range(
                dataset="XNAS.ITCH",
                schema=f"ohlcv-1{interval}",
                symbols=ticker,
                start=start_date,
                end=end_date
            ).to_df()     
        except Exception as e:
            print(e)
            raise HTTPException(status_code=404, detail=f"Bento error.")
        
        
    # return in form {"open": [...], "high": [...], "low": [...], "close": [...], "volume": [...]}
    chart_data = {
        "open": [],
        "high": [],
        "low": [],
        "close": [],
        "volume": [],
        "x": []
    }
    
    for row in df.itertuples():
        # get current eastern time date 
        row_date = row.Index # in UTC
        row_date = row_date.replace(tzinfo=ZoneInfo("UTC"))
        
        # convert to eastern time
        if interval != "d": # daily will be just 00:00:00, date matches
            row_date = row_date.astimezone(ZoneInfo("America/New_York"))
        
        row_date = row_date.strftime("%Y-%m-%d %H:%M:%S")
            
        chart_data["x"].append(row_date)
        
        chart_data["open"].append(format(row.open, ".3f"))
        chart_data["high"].append(format(row.high, ".3f"))
        chart_data["low"].append(format(row.low, ".3f"))
        chart_data["close"].append(format(row.close, ".3f"))
        chart_data["volume"].append(format(row.volume, ".0f"))
    
    return JSONResponse(content=chart_data)

