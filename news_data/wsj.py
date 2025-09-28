import requests
from bs4 import BeautifulSoup
from .shared import Shared
import os
import json
from datetime import datetime
import time
from zoneinfo import ZoneInfo
from langdetect import detect, detect_langs


class WsjArticleFetcher: 
    def __init__(self):
        self.shared = Shared() 
        
        self.WSJ_SITEMAP_1 = "https://www.wsj.com/wsjsitemaps/wsj_google_news.xml"
        self.WSJ_SITEMAP_2 = "https://www.wsj.com/live_news_sitemap.xml" 
        
    def fetch_sitemap(self, url):
        response = requests.get(url, headers=self.shared.get_headers("wsj"))
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'xml')

        xml_urls = soup.findAll("url")

        clean = []
        
        for xml_url in xml_urls:
            url = xml_url.find("loc").text
            publication_raw = xml_url.find("news:news").find("news:publication_date") # 2025-08-18T21:39:00Z
            
            # parse to unix seconds (utc)
            publication = datetime.strptime(publication_raw.text, "%Y-%m-%dT%H:%M:%SZ")
            publication = publication.replace(tzinfo=ZoneInfo("UTC"))
            
            title = xml_url.find("news:news").find("news:title").text

            clean.append((title, url, int(publication.timestamp())))

        return clean
       
    def fetch(self):
        clean = self.fetch_sitemap(self.WSJ_SITEMAP_1) + self.fetch_sitemap(self.WSJ_SITEMAP_2)

        for title, url, published in clean:
            if self.shared.article_url_fetched(url):
                continue
            
            if detect(title) != 'en':
                continue

            self.shared.save_article(url, "wsj", published, title, [])
