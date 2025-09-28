from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import HTTPException
from fastapi.responses import JSONResponse
import pandas as pd 
from py_vollib.black_scholes.implied_volatility import implied_volatility
from py_vollib.black_scholes import black_scholes
from scipy.optimize import brentq


def option_solver(r, vol, s, t, type, u, price, solveFor, timeUnits):
    def time_func(T): 
        return black_scholes(
            S=u,
            K=s,
            t=T,
            r=r,
            sigma=vol,
            flag=type[0]
        ) - price
            
    def strike_func(K):
        return black_scholes(
            S=u,
            K=K,
            t=t,
            r=r,
            sigma=vol,
            flag=type[0]
        ) - price
        
    def rate_func(r):
        return black_scholes(
            S=u,
            K=s,
            t=t,
            r=r,
            sigma=vol,
            flag=type[0]
        ) - price
        
    def u_func(u):
        return black_scholes(
            S=u,
            K=s,
            t=t,
            r=r,
            sigma=vol,
            flag=type[0]
        ) - price
        
            
    # Convert time to years if needed
    if timeUnits == "d":
        t = t / 365.0
    elif timeUnits == "h":
        t = t / (365.0 * 24.0)
        
    if solveFor == "vol":
        vol = implied_volatility(
            price=price,
            S=u,
            K=s,
            t=t,
            r=r,
            flag=type[0]
        )
        return {
            "vol": round(vol, 3)
        }
    elif solveFor == "price":
        p = black_scholes(
            S=u,
            K=s,
            t=t,
            r=r,
            sigma=vol,
            flag=type[0]
            )
        
        return {
            "price": round(p, 3)
        }
    elif solveFor == "t":
        try:
            t_min = 1 / (365 * 24 * 60 * 60)  # 1 second in years
            t_max = 10 # 10 years in years
            
            T = brentq(time_func, t_min, t_max)
            
            if timeUnits == "d":
                T = T * 365.0
            elif timeUnits == "h":
                T = T * (365.0 * 24.0)
                
            # format to 3 decimal places
            T = round(T, 3)
            return {"t": T}
        
        except ValueError:
            raise HTTPException(status_code=400, detail="No solution found for time.")
    elif solveFor == "s":
        try:
            K_min = 0.01  # minimum strike price
            K_max = 1_000_000  # maximum strike price
            
            K = brentq(strike_func, K_min, K_max)
            return {"s": round(K, 3)}
        
        except ValueError:
            raise HTTPException(status_code=400, detail="No solution found for strike.")
    elif solveFor == "r":
        try:
            r_min = -1.0  # minimum interest rate
            r_max = 1.0  # maximum interest rate
            
            r = brentq(rate_func, r_min, r_max)
            return {"r": round(r, 3)}
        
        except ValueError:
            raise HTTPException(status_code=400, detail="No solution found for interest rate.")
        
    elif solveFor == "u":
        try:
            u_min = 0.01  # minimum underlying price
            u_max = 1_000_000  # maximum underlying price
            
            u = brentq(u_func, u_min, u_max)
            return {"u": round(u, 3)}
        
        except ValueError:
            raise HTTPException(status_code=400, detail="No solution found for underlying price.")
