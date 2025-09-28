import requests
from bs4 import BeautifulSoup
from ..news_data.shared import Shared
import os
import json
from datetime import datetime
import time

class BloombergArticleFetcher: 
    def __init__(self):
        self.shared = Shared() 
        
        self.BLOOMBERG_SITEMAP = "https://www.bloomberg.com/sitemaps/news/latest.xml"
        
    def extract_text(self, component) -> tuple:
        text = "" 
        tickers = []
        
        if 'text' in component: 
            text = component['text']
            
        if "parts" in component: 
            for part in component['parts']:
                new_text, new_tickers = self.extract_text(part)
                
                text += new_text
                tickers.extend(new_tickers)
                
        if "html" in component:
            soup = BeautifulSoup(component['html'], "html.parser")
            text = soup.get_text().strip().replace("\n", " ")
        
        if "security" in component and "ticker" in component['security']:
            tickers.append(component['security']['ticker'])

        return text, tickers

    def clean_article(self, article: dict):
        title = article['title']
        
        published = article["published"] # already as unix seconds
        
        summary = article.get("summary", None)
        
        # try abstract if there is no summary
        if summary is None:
            abstract = article.get("abstract", [])
            
            if len(abstract) == 0: 
                abstract = ""
            elif isinstance(abstract, list):
                abstract = "\n".join(abstract)
                
            summary = abstract
                    
        components = article.get("components", [])
        body = ""
        tickers = []
        for component in components:
            new_body, new_tickers = self.extract_text(component)
            
            body += new_body
            tickers.extend(new_tickers)

        url = article.get("longURL")
        if not url:
            url = article.get("shortURL")

        return url, published, title, summary, body, tickers

    def get_internal_id(self, article_url):
        url = "https://cdn-mobapi.bloomberg.com/wssmobile/v1/urllookup/find"
        params = {
            "variant": "iphone",
            "newsEdition": "AMER",
            "liveRegion": "US",
            "url": f"{article_url}?utm_medium=deeplink"
        }
        headers = self.shared.get_headers("bloomberg")


        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        
        article_resolver = response.json()

        return article_resolver.get('internalID')

        
    def fetch_articles(self): 
        urls = self.shared.fetch_sitemap(self.BLOOMBERG_SITEMAP)
        
        urls = self.shared.filter_urls(urls)
        
        for raw in urls: 
            if "/sessions/" in raw: 
                continue
            print(f"Fetching: {raw}")
            
            
            internal_id = self.get_internal_id(raw)
            
            if not internal_id:
                print(f"Could not resolve internal ID for {raw}")
                continue
            
            
            mobile_url = f"https://cdn-mobapi.bloomberg.com/wssmobile/v1/stories/{internal_id}"
            
            params = {
                "contentCliff": "false"
            }
            
            response = requests.get(mobile_url, params=params, headers=self.shared.get_headers("bloomberg"))
            response.raise_for_status()
            
            self.shared.log_request_response("bloomberg.article", response.text)
                
            article_data = response.json()

            url, published_at, title, summary, content, tickers = self.clean_article(article_data)

            self.shared.save_article(url, "bloomberg", published_at, title, tickers)

            time.sleep(1)