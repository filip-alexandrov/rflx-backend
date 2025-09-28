import traceback
import requests
from bs4 import BeautifulSoup
from .shared import Shared
import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import time

class BloombergArticleFetcher: 
    def __init__(self):
        self.shared = Shared() 
        
        self.BLOOMBERG_SITEMAP = "https://www.bloomberg.com/sitemaps/news/latest.xml"
        self.BLOOMBERG_MOBILE = "https://cdn-mobapi.bloomberg.com/wssmobile/v1/pages/business/phx-latest"

    def fetch_mobile_latest(self): 
        response = requests.get(self.BLOOMBERG_MOBILE, headers=self.shared.get_headers("bloomberg"))
        response.raise_for_status()
        
        clean = []
        
        data = response.json()

        modules = data['modules']

        for module in modules: 
            if module['id'] != "filter_latest": 
                continue
            
            stories = module['stories']
            
            for story in stories: 
                title = story['title']
                link = story['longURL']
                published = story['published']
                
                clean.append((title, link, published))
                
        return clean
    
    def fetch_sitemap(self):
        response = requests.get(self.BLOOMBERG_SITEMAP, headers=self.shared.get_headers("bloomberg"))
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'xml')

        xml_urls = soup.findAll("url")

        clean = []
        
        for xml_url in xml_urls:
            url = xml_url.find("loc").text
            publication_raw = xml_url.find("news:news").find("news:publication_date") # 2025-08-18T17:15:35.955Z
            
            # parse to unix seconds (utc)
            publication = datetime.strptime(publication_raw.text, "%Y-%m-%dT%H:%M:%S.%fZ")
            publication = publication.replace(tzinfo=ZoneInfo("UTC"))
            
            title = xml_url.find("news:news").find("news:title").text

            clean.append((title, url, int(publication.timestamp())))

        return clean
    

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

        return text + " ", tickers

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

    
    def fetch(self): 
        clean = self.fetch_sitemap() + self.fetch_mobile_latest()
        
        for title, url, published in clean:
            if self.shared.article_url_fetched(url):
                continue
            
            if "/sessions/" in url: 
                continue
            
            try:
                internal_id = self.get_internal_id(url)
                
                if not internal_id:
                    print(f"Could not resolve internal ID for {url}")
                    continue
                
                print("Fetching: ", url)
                
                mobile_url = f"https://cdn-mobapi.bloomberg.com/wssmobile/v1/stories/{internal_id}"
                
                params = {
                    "contentCliff": "false"
                }

                response = requests.get(mobile_url, params=params, headers=self.shared.get_headers("bloomberg"))
                response.raise_for_status()
                
                self.shared.log_request_response("bloomberg.article", response.text)
                
                article_data = response.json()
                
                url, published, title, summary, content, tickers = self.clean_article(article_data)
                
            except Exception as e: 
                tickers = []
                content = ""
                print(f"Error fetching {url}: {e}")
                
                print(f"Error in <{self.__module__}.{self.__class__.__name__}>: {e}")
                traceback.print_exc()
                
            # try to get the article text 
            self.shared.save_article(url, "bloomberg", published, title, tickers, content)
            
            time.sleep(1)