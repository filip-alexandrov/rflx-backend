# export news objects 
from news_data.reuters import ReutersArticleFetcher
from news_data.barrons import BarronsArticleFetcher
from news_data.wsj import WsjArticleFetcher
from news_data.bloomberg import BloombergArticleFetcher
from news_data.twitter import TwitterFetcher
from news_data.sec import SecFetcher
from news_data.search import ArticleSearch
from news_data.shared import Shared
import time
import traceback
from datetime import datetime, tzinfo, timedelta
from zoneinfo import ZoneInfo



def fetch_news(): 
    bw = BarronsArticleFetcher()
    rw = ReutersArticleFetcher()
    wsj = WsjArticleFetcher()
    sec = SecFetcher()
    bb = BloombergArticleFetcher()
    twitter = TwitterFetcher()
    
    WEEKDAY_FETCH_INTERVAL = 2 * 60 # 2 minutes between 9 and 23 CET, weekdays
    WEEKEND_FETCH_INTERVAL = 10 * 60 # 10 minutes between 9 and 23 CET, weekends
    FETCH_INTERVAL_FILLINGS = 30


    next_fetch_sec = 0 # unix seconds
    next_fetch_news = 0
    
    while True:
        current_time = int(time.time())
        
        # GENERAL NEWS FETCH
        if current_time >= next_fetch_news:
            fetch_interval = None
            
            # determine weekday or weekend fetch interval to use
            cet_time = datetime.now(tz=ZoneInfo("CET"))
            

            # weekday between 9 and 23 CET
            if cet_time.weekday() < 5 and 9 <= cet_time.hour < 23:
                fetch_interval = WEEKDAY_FETCH_INTERVAL
            # weekend and weekday outside 9-23 CET
            else:
                fetch_interval = WEEKEND_FETCH_INTERVAL
                
            next_fetch_news = current_time + fetch_interval
        
            funcs = [
                bw.fetch, rw.fetch, wsj.fetch, \
                bb.fetch,
                twitter.fetch
            ]

            for fn in funcs:
                try:
                    fn()
                except Exception as e:
                    print(f"Error in <{fn.__module__}.{fn.__self__.__class__.__name__}.{fn.__func__.__name__}>: {e}")
                    traceback.print_exc()
        
        # SEC FETCH
        if current_time >= next_fetch_sec:
            next_fetch_sec = current_time + FETCH_INTERVAL_FILLINGS
            try:
                sec.fetch()
            except Exception as e:
                print(f"Error fetching SEC fillings: {e}")
                        

        time.sleep(1) # 1 second
    
    