import requests
from bs4 import BeautifulSoup
from .shared import Shared
import os
import json
from datetime import datetime
import time
import re 
from zoneinfo import ZoneInfo
from langdetect import detect, detect_langs



class ReutersArticleFetcher: 
    def __init__(self):
        self.shared = Shared() 
        
        self.REUTERS_SITEMAP_1 = "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml"
        self.REUTERS_SITEMAP_2 = "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=100"
        self.REUTERS_SITEMAP_3 = "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=200"

         
    def fetch_sitemap(self, url):
        response = requests.get(url, headers=self.shared.get_headers("reuters"))
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'xml')

        xml_urls = soup.findAll("url")

        clean = []
        
        for xml_url in xml_urls:
            url = xml_url.find("loc").text
            publication_raw = xml_url.find("news:news").find("news:publication_date") # 2025-08-19T09:58:48.93Z
            
            if "/sports/" in url: 
                continue
            
            # inspect the url for language
            lang = re.search(r'/([a-z]{2})/', url)
            if lang:
                lang = lang.group(1)
            else:
                lang = "en"
            
            if lang != "en":
                continue
            
            try: 
                # parse to unix seconds (utc)
                publication = datetime.strptime(publication_raw.text, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                # format 2025-08-19T13:16:08Z
                publication = datetime.strptime(publication_raw.text, "%Y-%m-%dT%H:%M:%SZ")
            
            publication = publication.replace(tzinfo=ZoneInfo("UTC"))
            
            title = xml_url.find("news:news").find("news:title").text

            clean.append((title, url, int(publication.timestamp())))

        return clean

    def fetch(self):
        clean = self.fetch_sitemap(self.REUTERS_SITEMAP_1)
        clean += self.fetch_sitemap(self.REUTERS_SITEMAP_2)
        clean += self.fetch_sitemap(self.REUTERS_SITEMAP_3)
        
        for c in clean: 
            title, url, published = c
            
            if detect(title) != 'en':
                continue
            
            if self.shared.article_url_fetched(url):
                continue
            
            self.shared.save_article(url, "reuters", published, title, [])
            
    


