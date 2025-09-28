import requests
from bs4 import BeautifulSoup
from ..shared import Shared
import os
import json
from datetime import datetime, timedelta
import time

def generate_range(start_date_str: str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = start_date
    
    start_date = start_date - timedelta(days=1)
        
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    return start_date_str, end_date_str

accounts = ["FirstSquawk", "TheInsiderPaper", "notreload_ai", "Trade_The_News", "LiveSquawk", "CorleoneDon77", \
     "wallstengine",  "DeItaone", "OracleNYSE", \
        "thehill", "politico", "WSJ" ]

base_url = "https://twitter-api45.p.rapidapi.com/search.php"

headers = {
            "x-rapidapi-key": "e397443822msh6716d33826f9c34p154f6djsn0e6fb053f0f5",
            "x-rapidapi-host": "twitter-api45.p.rapidapi.com"
        }
shared = Shared()
                
patience_count = 20 
min_date = "2024-01-01"

progress = { 
            "account": "WSJ", 
            "end_date": "2024-06-23",
            "start_date": "2024-06-22"
            }

# cut array to the "from" account 
accounts = accounts[accounts.index(progress['account']):]
initialized = False

for account in accounts:
    end_date = "2025-08-17"
    start_date = "2025-08-16"
    
    if not initialized:
        start_date = progress['start_date']
        end_date = progress['end_date']
        initialized = True
    
    while True: 
        if start_date < min_date:
            print(f"Reached minimum date {min_date}, stopping.")
            break
        
        no_new_count = 0 
        
        query = f"from:{account} since:{start_date} until:{end_date}"
        print(query)
        
        querystring = {"query": query, "search_type":"Latest"}
        
        fetched_cursors = 0
        
        try: 
            while True: 
                print("fetching")
                fetched_cursors += 1
                
                response = requests.get(base_url, headers=headers, params=querystring)
                response.raise_for_status()
                
                data = response.json()
                
                    
                if data['status'] == "error": 
                    print(f"Critical error")
                    exit(1)
                    
                with open("test.json", "w") as f:
                    json.dump(data, f, indent=4)
                    
                shared.log_request_response(f"twitter", response.text)
                    
                querystring['cursor'] = data['next_cursor']
                        
                has_new_tweets = False
                
                for tweet in data['timeline']: 
                    source = tweet['user_info']['screen_name']
                    
                    published_at = tweet['created_at'] # format "Fri Aug 15 01:01:01 +0000 2025",
                    published_at = int(datetime.strptime(published_at, "%a %b %d %H:%M:%S %z %Y").timestamp())
                    
                    content = tweet['text']
                    
                    tweet_id = tweet['tweet_id']
                    url = f"https://x.com/{source}/status/{tweet_id}"
                    
                    # check if url is already in db
                    if not shared.tweet_url_fetched(url):
                        has_new_tweets = True
                        
                    shared.save_tweet(url, source, published_at, content)
                    
                if not has_new_tweets:
                    no_new_count += 1
                    break
                else: 
                    no_new_count = 0
                    
                if len(data['timeline']) == 0:
                    print(f"No more tweets for {start_date}.")
                    break
                
        except Exception as e:
            print("Error fetching tweets:", e)
            continue # retry with the same date

        print(f"Fetched {fetched_cursors} for {account}")
        
        start_date, end_date = generate_range(start_date)
        
        if no_new_count >= patience_count:
            print(f"No new tweets for {no_new_count} iterations, stopping for {account}.")
            break
