import requests
from bs4 import BeautifulSoup
from .shared import Shared
import os
import json
from datetime import datetime
import time

class MarketWatchArticleFetcher: 
    def __init__(self):
        self.shared = Shared() 
        
        self.MW_SITEMAP = "https://www.marketwatch.com/mw_news_sitemap_1.xml"
        self.MW_BASE_URL = "https://mwatch.djmedia.djservices.io/apps/marketwatch/theaters/article"
    
    def parse_dt(self, ts: str) -> int:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return int(parsed.timestamp())
    
    def fetch(self): 
        urls = self.shared.fetch_sitemap(self.MW_SITEMAP)
        urls = self.shared.filter_urls(urls)
        
        for raw in urls: 
            if "/livecoverage/" in raw: 
                continue
            
            print(f"Fetching: {raw}")
            
            # create query url
            id = raw.split("/")[-1]
            
            params = {
                "screen_ids": id
            }
                
            
            response = requests.get(self.MW_BASE_URL, headers=self.shared.get_headers('marketwatch'), params=params)
            response.raise_for_status()
            
            self.shared.log_request_response("marketwatch.article", response.text)
                
            data = response.json()

            data = data['screens'][0]

            frames = data['frames']

            title = data['metadata'].get('title') or data['metadata']['original_headline']
            
            # format 2025-08-13T12:55:00Z
            publication_date_str = data['metadata']['createdAt']
            publication_date = self.parse_dt(publication_date_str)

            content = ""

            for frame in frames: 
                if frame['type'] == 'body':
                    text = frame["body"]['text']
                    
                    # swap stock symbols on the marked places
                    if "additions" in frame["body"]:                        
                        for addition in frame['body']['additions']: 
                            if addition['type'] == 'stockTicker': 
                                value = addition['value'].split("/")[-1]
                                text += f" [{value}]"

                    content += text + "\n"
                    
                elif frame['type'] == 'title':
                    content += frame["title"]['text'] + "\n"
                
            # update in db 
            self.shared.save_article(raw, "marketwatch", publication_date, title, "", content)
            time.sleep(2)





