import requests
from bs4 import BeautifulSoup
from ..news_data.shared import Shared
import os
import json
from datetime import datetime
import time

class WsjArticleFetcher: 
    def __init__(self):
        self.shared = Shared() 
        
        self.WSJ_SITEMAP_1 = "https://www.wsj.com/wsjsitemaps/wsj_google_news.xml"
        self.WSJ_SITEMAP_2 = "https://www.wsj.com/live_news_sitemap.xml"
        self.WSJ_BASE_URL = "https://shared-data.dowjones.io/gateway/graphql"
    
    
    def parse_dt(self, ts: str) -> int:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return int(parsed.timestamp())
    
    def fetch(self): 
        urls_1 = self.shared.fetch_sitemap(self.WSJ_SITEMAP_1)
        urls_2 = self.shared.fetch_sitemap(self.WSJ_SITEMAP_2)
        
        # ignore duplicate urls
        urls = list(set(urls_1 + urls_2))
        
        urls = self.shared.filter_urls(urls)
        
        for raw in urls:
            if "/livecoverage/" in raw or "/buyside/" in raw or '/arts-culture/' in raw:
                continue
            
            print(f"Fetching: {raw}")
            
            # remove trailing / to prevent catching it next
            if raw.endswith("/"):
                raw = raw[:-1]
                
            public_id = raw.split("/")[-1]

            vars = {"id": public_id, "idType":"seoid"}

            # resolve the internal article id
            params = {
                "extensions": '{"persistedQuery":{"sha256Hash":"64224db1551e3d7fbedd338be336b4846288b7df9a05baab9d6fd40dc5fef877","version":1}}',
                "operationName": "ArticleMetadata",
                "variables": json.dumps(vars)
            }
            
            response = requests.get(self.WSJ_BASE_URL, headers=self.shared.get_headers("wsj"), params=params)
            response.raise_for_status()
            
            self.shared.log_request_response("wsj.article_metadata", response.text)
            
            article_metadata = response.json()
            origin_id = article_metadata['data']['articleContent']['originId']
            
            vars = {"filterByScope": "MOBILE", "id": origin_id, "idType": "originid"}
            
            params = { 
                "extensions": '{"persistedQuery":{"sha256Hash":"97019e06b2ca7a3e8384a9fbc6efb2b02eb6c80ae9c6df140c0bfe209a1ff898","version":1}}',
                "operationName": "ArticleContent",
                "variables": json.dumps(vars)
            }

            response = requests.get(self.WSJ_BASE_URL, headers=self.shared.get_headers("wsj"), params=params)
            response.raise_for_status()
            
            self.shared.log_request_response("wsj.article", response.text)
            
            content_data = response.json()
            
            title = content_data['data']['articleContent']['articleHeadline']['flattened']['text']
            
            published_at = content_data['data']['articleContent']['publishedDateTimeUtc'] # string format: "2025-08-13T13:17:00Z"
            published_at = self.parse_dt(published_at)
            
            body = content_data['data']['articleContent']['articleBody']
            
            content = []
            tickers = []
            
            for elem in body: 
                if elem['__typename'] == "ParagraphArticleBody" and elem["textAndDecorations"] != None: 
                    flattened = elem["textAndDecorations"]['flattened']
                    text = flattened["text"]
                    content.append(text)
                    
                    decorations = flattened.get("decorations")
                    
                    if not decorations:
                        continue
                        
                    for decoration in decorations:
                        # ensure ticker is present
                        try: 
                            if decoration['decorationType'] == "COMPANY" and decoration['decorationMetadata']['instrumentResult']['exchangeData']['countryCode'] == "US": 
                                ticker = decoration["decorationMetadata"]["instrumentResult"]["ticker"]
                                tickers.append(ticker)
                        except: 
                            pass # programmed with my ass
                    
            content = "\n".join(content)
            
            # update in db 
            self.shared.save_article(raw, "wsj", published_at, title, tickers)
            time.sleep(2)





