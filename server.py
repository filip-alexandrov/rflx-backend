# start via `uvicorn server:app --reload`

from fastapi import FastAPI, Query, HTTPException, Request, status
from pydantic import BaseModel, Field
from fastapi.middleware.gzip import GZipMiddleware
from typing import List, Optional
from datetime import datetime
from fastapi.responses import JSONResponse
import pandas as pd
from databento import Historical, SType
from fastapi.middleware.cors import CORSMiddleware

from datetime import datetime
import pytz
from py_vollib.black_scholes.implied_volatility import implied_volatility
from py_vollib.black_scholes import black_scholes
from scipy.optimize import brentq
from zoneinfo import ZoneInfo
import databento as db
from fastapi import Response


from news_data import ArticleSearch, Shared

from market_data import fetch_multi_iv, equity_lf, fetch_hf_iv, option_solver, get_option_definitions, decode_option_ticker
from kk import KK_data
from starlette.middleware.base import BaseHTTPMiddleware
import psycopg2
import hashlib, secrets
from datetime import timedelta
from notifications import Notify
from observations import ObservationsLibrary
    

app = FastAPI()


origins = ["http://localhost:3000", "https://reflexia.markets", "*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Session middleware
class SessionMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self.auth_prefix = "/auth"
        
        self.db = psycopg2.connect(
            dbname="postgres",
            user="admin",
            password="secret",
            host="localhost",
            port=5432
        )
        self.cursor = self.db.cursor()

    def validate_session(self, token: str):
        # query the sessions table for the token 
        sql = "SELECT id, expires_at FROM sessions WHERE token = %s AND expires_at > NOW()"
        self.cursor.execute(sql, (token,))
        result = self.cursor.fetchone()
        if not result:
            return None

        return {"id": result[0], "expires_at": result[1]}

    async def dispatch(self, request: Request, call_next):
        print(request.headers)
        path = request.url.path
        if path.startswith(self.auth_prefix):
            return await call_next(request)
        
        if request.method == "OPTIONS":
            # Let CORSMiddleware craft headers by passing through
            return await call_next(request)

        # Extract token from Authorization: Bearer <token> or cookie "session"
        auth = request.headers.get("authorization", "")
        token = None
        if auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
        if not token:
            token = request.cookies.get("session")

        if not token:
            return JSONResponse({"detail": "Missing session"}, status_code=401, headers={"WWW-Authenticate": 'Bearer realm="api"'})


        sess = self.validate_session(token)
        if not sess:
            return JSONResponse({"detail": "Invalid session"}, status_code=401, headers={"WWW-Authenticate": 'Bearer error="invalid_token"'})


        # attach to request.state for downstream handlers
        request.state.session_id = sess["id"]

        return await call_next(request)

app.add_middleware(SessionMiddleware)

app.add_middleware(GZipMiddleware, minimum_size=500)

@app.post("/create-session")
def create_session(ttl_minutes: int = 60*24*7):
    token = secrets.token_urlsafe(32)
    hashed = hashlib.sha256(token.encode()).hexdigest()
    expires_at = datetime.utcnow() + timedelta(minutes=ttl_minutes)
    
    db = psycopg2.connect(
        dbname="postgres",
        user="admin",
        password="secret",
        host="localhost",
        port=5432
    )
    cursor = db.cursor()
    sql = "INSERT INTO sessions (token, expires_at) VALUES (%s, %s) RETURNING id"
    cursor.execute(sql, (hashed, expires_at))
    db.commit()
    session_id = cursor.fetchone()
    if not session_id:
        raise HTTPException(status_code=500, detail="Failed to create session")
    session_id = session_id[0]
    cursor.close()
    db.close()

    return {"session_id": session_id, "token": token, "expires_at": expires_at}


@app.get("/")
def test(): 
    return {"test": "connection"}

@app.get("/option-definitions")
def get_option_definitions_handler(start_date: str, ticker: str):
    return get_option_definitions(start_date=start_date, ticker=ticker)


class OptionPriceRequest(BaseModel):
    r: float # interest rate
    vol: float # volatility
    s: float # strike
    t: float # time to expiration
    type: str # call or put
    u: float # underlying
    price: float # option price
    solveFor: str # vol, price, t, s, r, u
    timeUnits: str # d, h (units of t)
    
@app.post("/option-solver")
def option_solver_handler(request: OptionPriceRequest):
    return option_solver(
        r=request.r,
        vol=request.vol,
        s=request.s,
        t=request.t,
        type=request.type,
        u=request.u,
        price=request.price,
        solveFor=request.solveFor,
        timeUnits=request.timeUnits
    )

class EquityChartRequest(BaseModel):
    ticker: str # >4 chars for options contracts
    startDate: str # YYYY-MM-DD HH:MM:SS or YYYY-MM-DD
    endDate: str # YYYY-MM-DD HH:MM:SS or YYYY-MM-DD
    interval: str # D, H, M, S
    
@app.post("/equity-chart")
def equity_chart(request: EquityChartRequest):
    return equity_lf(
        ticker=request.ticker,
        startDate=request.startDate,
        endDate=request.endDate,
        interval=request.interval
    )

@app.post("/opt-nbbo-hf")
def opt_nbbo_hf(request: EquityChartRequest):
    return fetch_hf_iv(request.ticker, request.startDate, request.endDate)

class MultiIVRequest(BaseModel):
    contracts: List[str] # list of option tickers
    startDate: str # YYYY-MM-DD HH:MM:SS or YYYY-MM-DD
    endDate: str # YYYY-MM-DD HH:MM:SS or YYYY-MM-DD

@app.post("/multi-iv")
def multi_iv(request: MultiIVRequest):
    return fetch_multi_iv(raw_opt_tickers=request.contracts, start_date=request.startDate, end_date=request.endDate)


# expressions
class Expression(BaseModel):
    keywords: str
    description: str
    timers: List[int]

@app.get("/expressions")
def get_expressions(read: str, search: str, clean: bool): # all, unread, saved
    article_search = ArticleSearch()
    return article_search.get_expressions(read=read, search=search, clean=clean)

@app.post("/expressions")
def add_expression(expression: Expression):
    search = ArticleSearch()
    search.add_expression(
        keywords=expression.keywords,
        timers=expression.timers,
        description=expression.description
    )
    return JSONResponse(content={"message": "Expression added successfully."})

@app.put("/expressions/{expr_id}")
def update_expression(expr_id: int, expression: Expression):
    search = ArticleSearch()
    search.update_expression(
        id=expr_id,
        new_keywords=expression.keywords,
        new_timers=expression.timers,
        new_description=expression.description
    )
    return JSONResponse(content={"message": "Expression updated successfully."})

@app.delete("/expressions/{expr_id}")
def delete_expression(expr_id: int):
    search = ArticleSearch()
    search.remove_expression(expr_id)
    return JSONResponse(content={"message": "Expression removed successfully."})

class MarkFulfilledDataRequest(BaseModel):
    fulfilled_id: int
    read: int

@app.post("/expression-mark-fulfilled-data")
def mark_fulfilled_data(request: MarkFulfilledDataRequest):
    search = ArticleSearch()
    search.mark_fulfilled_data_read(request.fulfilled_id, request.read)
    return {"success": True, "message": "Fulfilled data marked successfully."}

@app.delete("/clean-fulfilled-data/{expr_id}")
def clean_fulfilled_data(expr_id: int):
    search = ArticleSearch()
    search.clean_fulfilled_data(expr_id)
    return JSONResponse(content={"message": "Fulfilled data cleared successfully."})

class RawSearchRequest(BaseModel):
    query: str = ""
    target: str = "articles"  # can be "articles", "tweets"


class SearchRequest(BaseModel): 
    query: str 
    start_date: Optional[str] # format "YYYY-MM-DD" or "YYYY-MM-DD HH:MM"
    end_date: Optional[str] # format "YYYY-MM-DD" or "YYYY-MM-DD HH:MM"
    source: Optional[str] = None
    page: int = 1
    table: str = "articles"  # can be "articles", "tweets"
    saved_only: bool
    
@app.post("/search")
def search(request: SearchRequest): 
    start_date = None if request.start_date == "" else request.start_date
    end_date = None if request.end_date == "" else request.end_date
    source = request.source if request.source else None
    
    
    search = ArticleSearch()
    if request.table == "articles":
        results = search.search_articles(
            query=request.query,
            start_date=start_date,
            end_date=end_date,
            source=source,
            page=request.page,
            saved_only=request.saved_only   
        )
        
        return JSONResponse(content=results)
    elif request.table == "tweets":
        results = search.search_tweets(
            query=request.query,
            start_date=start_date,
            end_date=end_date,
            source=source,
            page=request.page, 
            saved_only=request.saved_only
        )
        
        return JSONResponse(content=results)
    else:
        raise HTTPException(status_code=400, detail="Invalid table. Use 'articles' or 'tweets'.")
    


class ArticleReadReqeuest(BaseModel):
    url: str
    read: int
    table: str
    
@app.post("/mark")
def mark_article(request: ArticleReadReqeuest):
    shared = Shared()
    shared.mark_article(request.url, request.read, request.table)
    return {"success": True, "message": "Article marked successfully."}
    
@app.get("/unread-articles")
def get_unread_articles(source: Optional[str] = None, page: int = 1):
    shared = Shared()
    return shared.get_unread_articles(source=source, page=page)

@app.get("/group-by-tickers")
def group_by_tickers():
    shared = Shared()
    return shared.group_by_tickers()

@app.get("/underlying-expiration")
def get_underlying_expiration(ticker: str):
    option_ticker, underlying_ticker, expiration_date, strike_price, t = decode_option_ticker(ticker)
    
    # in et timezone
    et = ZoneInfo("America/New_York")
    current_date = datetime.now(et)
    
    # get hours remaining until expiration
    days_remaining = (expiration_date - current_date).total_seconds() / 86400

    return {
            "type": t,
            "strike_price": strike_price,
            "days_remaining": round(days_remaining, 2)
            }
    
@app.get("/kk-comments")
def get_kk_comments(search_query: str, page: int, start_date: str, end_date: str, ascending: bool):
    kk_data = KK_data()
    return kk_data.search_comments(search_query, page, start_date=start_date, end_date=end_date, ascending=ascending)

@app.get("/kk-posts")
def get_kk_posts(search_query: str, page: int, start_date: str, end_date: str):
    kk_data = KK_data()
    return kk_data.search_posts(search_query, page, start_date=start_date, end_date=end_date)


class Observation(BaseModel):
    description: str
    content: str

@app.post("/observation")
def create_observation(observation: Observation):
    obs = ObservationsLibrary()
    return obs.create_observation(description=observation.description, content=observation.content)

@app.get("/observations")
def get_observations(query: str, page: int = 1):
    obs = ObservationsLibrary()
    return obs.get_observations(query=query, page=page)

@app.delete("/observation/{observation_id}")
def delete_observation(observation_id: int):
    obs = ObservationsLibrary()
    success = obs.delete_observation(observation_id)
    if not success:
        raise HTTPException(status_code=404, detail="Observation not found")
    return {"success": True, "message": "Observation deleted successfully."}


@app.put("/observation/{observation_id}")
def update_observation(observation_id: int, observation: Observation):
    obs = ObservationsLibrary()
    success = obs.update_observation(observation_id, description=observation.description, content=observation.content)
    if not success:
        raise HTTPException(status_code=404, detail="Observation not found")
    return {"success": True, "message": "Observation updated successfully."}

@app.get("/calendar-events")
def get_calendar_events():
    obs = ObservationsLibrary()
    return obs.get_calendar_events()

class CalendarEvent(BaseModel):
    title: str
    date: str  # YYYY-MM-DD

@app.post("/calendar-events")
def create_calendar_event(event: CalendarEvent):
    obs = ObservationsLibrary()
    return obs.create_calendar_event(title=event.title, date=event.date)

@app.delete("/calendar-events/{event_id}")
def delete_calendar_event(event_id: int): 
    obs = ObservationsLibrary()
    success = obs.delete_calendar_event(event_id)
    if not success:
        raise HTTPException(status_code=404, detail="Event not found")
    return {"success": True, "message": "Event deleted successfully."}

@app.put("/calendar-events/{event_id}")
def update_calendar_event(event_id: int, event: CalendarEvent):
    obs = ObservationsLibrary()
    success = obs.update_event(event_id, date=event.date)
    if not success:
        raise HTTPException(status_code=404, detail="Event not found")
    return {"success": True, "message": "Event updated successfully."}
