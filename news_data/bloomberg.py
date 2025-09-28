import requests
from bs4 import BeautifulSoup
from .shared import Shared
import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import time
from langdetect import detect, detect_langs


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

            
            
    def fetch(self): 
        clean = self.fetch_sitemap() + self.fetch_mobile_latest()
        
        for title, url, published in clean:
            if detect(title) != 'en':
                continue
            
            if self.shared.article_url_fetched(url):
                continue
            
            self.shared.save_article(url, "bloomberg", published, title, [])