import requests
from bs4 import BeautifulSoup
from ..news_data.shared import Shared
import os
import json
from datetime import datetime
import time

class BarronsArticleFetcher: 
    def __init__(self):
        self.shared = Shared() 
        
        self.base_url = "https://barrons.djmedia.djservices.io/apps/barrons/theaters/latest-stories"
    
    def parse_dt(self, ts: str) -> int:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return int(parsed.timestamp())
        
    def fetch(self):
        url = "https://barrons.djmedia.djservices.io/apps/barrons/theaters/collections"
        params = {
            "screen_ids": "real-time-analysis"
        }

        response = requests.get(url, headers=self.shared.get_headers("barrons"), params=params)
        response.raise_for_status()

        self.shared.log_request_response("barrons.collections", response.text)

        data = response.json()

        frames = data['screens'][0]['frames']
        
        urls = []

        for frame in frames: 
            if frame['type'] == 'article': 
                article_id = frame['articleId']
                url = f"{self.base_url}?screen_ids={article_id}"
                urls.append(url)

            elif frame['type'] == "real_time_article":
                article_id = frame['articleId']
                url = f"{self.base_url}?screen_ids={article_id}"
                urls.append(url)
                
        urls = self.shared.filter_urls(urls)
        
        for url in urls: 
            print(f"Fetching: {url}")

            response = requests.get(url, headers=self.shared.get_headers("barrons"))
            response.raise_for_status()

            self.shared.log_request_response("barrons.article", response.text)

            data = response.json()
            article_data = data['screens'][0]
            
            tickers = article_data['metadata'].get('referencedSymbols', [])
            
            title = article_data['metadata'].get('title') or article_data['metadata']['original_headline']
            publication_date_str = article_data['metadata']['createdAt']
            publication_date = self.parse_dt(publication_date_str)


            frames = article_data['frames']

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
                    
            # save article to database
            self.shared.save_article(url, "barrons", publication_date, title, tickers)
            time.sleep(2)
        
            