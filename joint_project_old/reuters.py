import requests
from bs4 import BeautifulSoup
from .shared import Shared
import os
import json
from datetime import datetime
import time
import re 

class ReutersArticleFetcher: 
    def __init__(self):
        self.shared = Shared() 
        
        self.REUTERS_SITEMAP = "https://www.reuters.com/arc/outboundfeeds/sitemap/?outputType=xml"
        self.REUTERS_BASE_URL = "https://www.reuters.com/mobile/v1/"
        
    def parse_iso8601_z(self, ts: str) -> int:
        # Handles ...Z and ...±HH:MM, with or without microseconds
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        # (dt is aware). timestamp() now reflects absolute UTC seconds
        return int(dt.timestamp())

                
    def fetch(self): 
        urls = self.shared.fetch_sitemap(self.REUTERS_SITEMAP)
        urls = self.shared.filter_urls(urls)
        
        for raw in urls:
            if "/sports/" in raw or "/wider-image/" in raw: 
                continue
            
            print(f"Fetching: {raw}")
            
            url = raw.replace("https://www.reuters.com/", self.REUTERS_BASE_URL)
            
            params = {
                "outputType": "json"
            }


            response = requests.get(url, headers=self.shared.get_headers("reuters"), params=params)
            response.raise_for_status()

            tickers = []
            matches = re.findall(r"https://www\.reuters\.com/markets/companies/([a-zA-Z0-9\.\:\-\_]+)", response.text)
            
            for match in matches: 
                if match not in tickers:
                    tickers.append(match)
            
            

            self.shared.log_request_response("reuters.article", response.text)

            # ignore pure html  
            if response.text.startswith("<!DOCTYPE html"): 
                print("Ignoring pure HTML response")
                continue
                            
            data = response.json()
            article = data[1]['data']['article']

            title = article['title']
            content_elements = article['content_elements']
            published_time = article['published_time']
            
            published_at = self.parse_iso8601_z(published_time)

            content = ""

            for element in content_elements: 
                if element['type'] == "paragraph" or element['type'] == "header": 
                    content += element["content"] + "\n\n"
                    
            soup = BeautifulSoup(content, "html.parser")

            for a in soup.find_all("a"):
                a.replace_with(a.get_text())

            content = str(soup) 
            
            # update in db 
            self.shared.save_article(raw, "reuters", published_at, title, tickers)
            time.sleep(2)





