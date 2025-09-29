from fastapi import FastAPI, Query, HTTPException
from datetime import datetime
from databento import Historical, SType
import pandas as pd
from fastapi.responses import JSONResponse
from .tables import decode_option_ticker



def get_option_definitions(start_date: str, ticker: str):
    # parse to datetime
    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = start_date + pd.Timedelta(days=1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use DD-MM-YYYY.")
    
    ticker = ticker.upper()
    
    client = Historical(key="db-UVPEpgVxgduVKFnQ9tgEp6cSTL65H",)
    
    try: 
        defs = client.timeseries.get_range(
            dataset="OPRA.PILLAR",
            schema="definition",
            symbols=f"{ticker}.OPT",
            stype_in=SType.PARENT,
            start=start_date,
            end=end_date
        ).to_df()
    except Exception as e:
        return []
    
    data = []
    
    for row in defs.itertuples():
        option_ticker, underlying_ticker, expiration_date, strike_price, t = decode_option_ticker(row.raw_symbol)
        
        data.append({
            "raw_symbol": option_ticker,
            "expiration": expiration_date.strftime('%Y-%m-%d'),
            "strike_price": strike_price,
            "instrument_class": t
        })

    return JSONResponse(content=data)