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
    
    
# Opt. OHLCV LF Underlying + IV
def fetch_lf_iv(option_ticker, startDate, endDate, interval): 
    client = Historical("db-ccxiVwBnUxpTTPmBRW4C3gSQkj9PX") 
    
    # get the underlying 
    option_ticker, underlying_ticker, expiration_date, strike_price, t = decode_option_ticker(option_ticker)
    
    start_date, end_date, interval = date_and_interval_validation(startDate, endDate, interval)
    
    df_underlying = client.timeseries.get_range(
        dataset="XNAS.ITCH",
        schema=f"ohlcv-1{interval}",
        symbols=underlying_ticker,
        start=start_date,
        end=end_date
    ).to_df()     
    
    df_option = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema=f"ohlcv-1{interval}",
        symbols=option_ticker,
        start=start_date,
        end=end_date,
    ).to_df()

    df_option = df_option.groupby(df_option.index).agg({
        'open': 'mean',
        'high': 'max',
        'low': 'min',
        'close': 'mean',
        'volume': 'sum'
    })

    df_underlying = df_underlying.groupby(df_underlying.index).agg({
        'open': 'mean',
        'high': 'max',
        'low': 'min',
        'close': 'mean',
        'volume': 'sum'
    })
    
    df3 = pd.merge_asof(
        df_option,
        df_underlying,
        on='ts_event',
        direction='nearest'
    )
    
    
    iv_data_full = []
    
    for row in df3.itertuples():
        # get current eastern time date 
        row_date = row.ts_event # in UTC
        row_date = row_date.replace(tzinfo=ZoneInfo("UTC"))
        
        # convert to eastern time
        if interval != "d": # daily will be just 00:00:00, date matches
            row_date = row_date.astimezone(ZoneInfo("America/New_York"))
        
        # hours until expiration
        hours_until_expiration = (expiration_date - row_date).total_seconds() / 3600
        
        years_until_expiration = hours_until_expiration / (24 * 365.25) 
        
        
        open_option = row.open_x
        open_underlying = row.open_y 
        
        high_option = row.high_x
        high_underlying = row.high_y
        
        low_option = row.low_x
        low_underlying = row.low_y
        
        close_option = row.close_x
        close_underlying = row.close_y
        
        volume_option = row.volume_x
        volume_underlying = row.volume_y

        try:
            iv_open = implied_volatility(
                open_option,
                open_underlying,
                strike_price,
                years_until_expiration,
                0.04,
                t.lower())
        except Exception as e:
            iv_open = 0

        try:
            iv_high = implied_volatility(
                high_option,
                high_underlying,
                strike_price,
                years_until_expiration,
                0.04,
                t.lower())
        except Exception as e:
            iv_high = 0

        try:
            iv_low = implied_volatility(
                low_option,
                low_underlying,
                strike_price,
                years_until_expiration,
                0.04,
                t.lower())
        except Exception as e:
            iv_low = 0
        
        try:
            iv_close = implied_volatility(
                close_option,
                close_underlying,
                strike_price,
                years_until_expiration,
                0.04,
                t.lower())
        except Exception as e:
            iv_close = 0
        
        iv_midpoint = (iv_open + iv_high + iv_low + iv_close) / 4
        
        volume_option = row.volume_x
        volume_underlying = row.volume_y
        
        # set precision and stringify
        open_option = format(open_option, ".3f")
        high_option = format(high_option, ".3f")
        low_option = format(low_option, ".3f")
        close_option = format(close_option, ".3f")
        volume_option = format(volume_option, ".0f")
        
        open_underlying = format(open_underlying, ".3f")
        high_underlying = format(high_underlying, ".3f")
        low_underlying = format(low_underlying, ".3f")
        close_underlying = format(close_underlying, ".3f")
        volume_underlying = format(volume_underlying, ".0f")
        
        iv_open = format(iv_open, ".3f")
        iv_high = format(iv_high, ".3f")
        iv_low = format(iv_low, ".3f")
        iv_close = format(iv_close, ".3f")
        iv_midpoint = format(iv_midpoint, ".3f")
        
        row_date = row_date.strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"IV: {iv_open} {iv_high} {iv_low} {iv_close} {iv_midpoint}")
        
        iv_data_full.append({
            "row_date": row_date,
            
            "open_option": open_option,
            "high_option": high_option,
            "low_option": low_option,
            "close_option": close_option,
            "volume_option": volume_option,
            
            "open_underlying": open_underlying,
            "high_underlying": high_underlying,
            "low_underlying": low_underlying,
            "close_underlying": close_underlying,
            "volume_underlying": volume_underlying,
            
            "iv_open": iv_open,
            "iv_high": iv_high,
            "iv_low": iv_low,
            "iv_close": iv_close,
            "iv_midpoint": iv_midpoint
        })
        
    global_data = {
        "expiration_date" : expiration_date.strftime("%Y-%m-%d %H:%M:%S"), 
        "underlying_ticker": underlying_ticker, 
        "option_ticker": option_ticker, 
        "strike_price": format(strike_price, ".2f")
    }
    
    return {
        "global_data": global_data,
        "table_data": iv_data_full
    }

# Opt. NBBO HF Underlying + IV
def fetch_hf_iv(option_ticker, startDate, endDate): 
    client = Historical("db-ccxiVwBnUxpTTPmBRW4C3gSQkj9PX") 
    
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
    client = Historical("db-ccxiVwBnUxpTTPmBRW4C3gSQkj9PX") 
    
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

