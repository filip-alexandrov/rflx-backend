from xxlimited import new
import requests
from bs4 import BeautifulSoup
from .shared import Shared
import os
import json
from datetime import datetime
import time

class TwitterFetcher: 
    def __init__(self):
        self.shared = Shared() 
        
        self.accounts = ["FirstSquawk", "TheInsiderPaper", "notreload_ai", \
            "Trade_The_News", "LiveSquawk", "CorleoneDon77", \
            "wallstengine",  "DeItaone", "OracleNYSE", \
            "thehill", "politico", "WSJ" ]
        
        self.search_url = "https://twitter-api45.p.rapidapi.com/timeline.php"
        
        self.headers = {
            "x-rapidapi-key": "e397443822msh6716d33826f9c34p154f6djsn0e6fb053f0f5",
            "x-rapidapi-host": "twitter-api45.p.rapidapi.com"
        }
        
    
                
    def fetch(self): 
        fetched_cursors = 0
        
        for account in self.accounts:
            try: 
                fetched_cursors = 0
                
                querystring = {"screenname":account}
                                        
                while fetched_cursors < 10: 
                    fetched_cursors += 1
                    
                    response = requests.get(self.search_url, headers=self.headers, params=querystring)
                    response.raise_for_status()
                    
                    with open(f"twitter_log.json", "w") as f: 
                        f.write(f"Status Code: {response.status_code}\n")
                        f.write(f"Response Body: {response.text}\n")

                    self.shared.log_request_response(f"twitter", response.text)
                    
                    data = {}
                    try: 
                        data = response.json()
                    except Exception as e: 
                        raise Exception(f"Could not parse JSON response: {response.text}")
                        
                    querystring['cursor'] = data['next_cursor']
                    
                    new_encountered = False
                    
                    if len(data['timeline']) == 0: 
                        raise Exception("Empty timeline retuned")
                        
                    
                    for tweet in data['timeline']: 
                        source = tweet['author']['screen_name']
                        
                        published_at = tweet['created_at'] # format "Fri Aug 15 01:01:01 +0000 2025",
                        published_at = int(datetime.strptime(published_at, "%a %b %d %H:%M:%S %z %Y").timestamp())
                        
                        content = tweet['text']
                        
                        tweet_id = tweet['tweet_id']
                        url = f"https://x.com/{source}/status/{tweet_id}"
                                            
                        # check if url is already in db
                        if self.shared.tweet_url_fetched(url):
                            continue

                        new_encountered = True 
                        self.shared.save_tweet(url, source, published_at, content)

                    if not new_encountered:
                        print(f"FC:{fetched_cursors} for {account}")
                        break
                        
                    time.sleep(1)
            except Exception as e:
                print(f"Error in TwitterFetcher for account {account}: {e} at FC: {fetched_cursors}")
                continue    