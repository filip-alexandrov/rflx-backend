import requests
from bs4 import BeautifulSoup
import psycopg2
import time
import json
import os
from datetime import datetime
from .search import ArticleSearch
from collections import defaultdict

class Shared:
    def __init__(self):
        self.db = psycopg2.connect(
            dbname="postgres",
            user="admin",
            password="secret",
            host="localhost",
            port=5432
        )
        self.cursor = self.db.cursor()
        self.search = ArticleSearch()

    # destructor to close the database connection
    def __del__(self):
        self.cursor.close()
        self.db.close()
        print("Database connection closed.")
        
    # sitemap fetcher
    def fetch_sitemap(self, sitemap_url: str): 
        headers = {
            'User-Agent': 'Googlebot-News/1.0 (+http://www.google.com/bot.html)'
        }

        response = requests.get(sitemap_url, headers=headers)

        soup = BeautifulSoup(response.content, 'xml')

        xml_urls = soup.findAll("url")

        urls = []
        
        for xml_url in xml_urls:
            url = xml_url.find("loc").text
            urls.append(url)
            
        return urls

    # return only urls not in sql database 
    def filter_urls(self, urls): 
        if not urls:
            return []
        
        query = "SELECT url FROM articles WHERE url = ANY(%s)"
        self.cursor.execute(query, (urls,))
        
        existing_urls = set(row[0] for row in self.cursor.fetchall())
        
        return [url for url in urls if url not in existing_urls]
    
    def article_url_fetched(self, url):
        query = "SELECT content FROM articles WHERE url = %s"
        self.cursor.execute(query, (url,))
        return self.cursor.fetchone() is not None
    
    def tweet_url_fetched(self, url):
        query = "SELECT url FROM tweets WHERE url = %s"
        self.cursor.execute(query, (url,))
        return self.cursor.fetchone() is not None
    
    def filling_url_fetched(self, url):
        query = "SELECT url FROM fillings WHERE url = %s"
        self.cursor.execute(query, (url,))
        return self.cursor.fetchone() is not None
    
    
    def save_article(self, url, source, published_at, title, tickers):
        tickers = ",".join(tickers) if tickers else ""
        
        query = """
        INSERT INTO articles (url, source, published_at, title, tickers)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING
        """
        self.cursor.execute(query, (url, source, published_at, title, tickers))
        self.db.commit()
        
        self.search.match_candidate(url, "articles")

    def save_tweet(self, url, source, published_at, content):
        query = """
        INSERT INTO tweets (url, source, published_at, content)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING
        """
        self.cursor.execute(query, (url, source, published_at, content))
        self.db.commit()

        # alert if match found
        self.search.match_candidate(url, "tweets")

    def save_filling(self, url, title, published_at, category): 
        query = """
        INSERT INTO fillings (url, title, published_at, category)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING
        """
        self.cursor.execute(query, (url, title, published_at, category))
        self.db.commit()
        
        self.search.match_candidate(url, "fillings")
    
    
    def get_headers(self, source):
        with open(os.path.join(os.path.dirname(__file__), "headers.json"), "r") as f:
            tokens = json.load(f)
        
        # exclude encoding header 
        headers = tokens[source]
        headers.pop("Accept-Encoding", None)
        
        return headers

    def log_request_response(self, file, text):
        # append to the respective jsonl log file
        log_file = os.path.join(os.path.dirname(__file__), "logs", f"{file}.jsonl")
        with open(log_file, "a") as f:
            f.write(json.dumps(text) + "\n")
            
    def row_to_article(self, row):
        url, source, published_at, title, tickers, read, _ = row
        
        return {
            "url": url,
            "source": source,
            "published_at": published_at,
            "title": title,
            "tickers": tickers, 
            "read": read
        }
    
    def mark_article(self, url, mark, table):
        query = f"""
            UPDATE {table}
            SET read = %s
            WHERE url = %s;
        """
        self.cursor.execute(query, (mark, url))
        self.db.commit()
        
    def group_by_tickers(self): 
        # articles from the last 72 hours 
        query = """
            SELECT *
            FROM articles
            WHERE published_at >= EXTRACT(EPOCH FROM NOW()) - (72 * 60 * 60)
            ORDER BY published_at DESC;
        """
        self.cursor.execute(query)
        articles = self.cursor.fetchall()
        
        ticker_to_articles = defaultdict(list)
        
        for row in articles:
            article = self.row_to_article(row)
            
            clean_tickers = []
            
            raw_tickers = article['tickers'].split(",")
            
            # decode tickers based on source 
            #OWL:US,BX:US
            if article['source'] == "bloomberg":
                for rt in raw_tickers:
                    if rt.endswith(":US"):
                        clean_tickers.append(rt[:-3])
            # CAVA,CMG,SG,EAT
            elif article['source'] == "wsj": 
                for rt in raw_tickers:
                    clean_tickers.append(rt)
            # STOCK/US/XNYS/HD,STOCK/US/XNYS/LOW,STOCK/US/XNYS/TGT,STOCK/US/XNYS/WMT,INDEX/US/S&P US/SPX
            elif article['source'] == "barrons":
                for rt in raw_tickers: 
                    if "/US/" in rt: 
                        clean_tickers.append(rt.split("/")[-1])
                        
            # PHM.N,LEN.N,DHI.N
            elif article['source'] == "reuters": 
                for rt in raw_tickers: 
                    if rt.endswith(".N"):
                        clean_tickers.append(rt[:-2])
                        
            for ct in clean_tickers:
                ticker_to_articles[ct].append(article)
                
        del ticker_to_articles[""]  # remove empty tickers
        
        # sort by number of articles
        ticker_to_articles = dict(sorted(ticker_to_articles.items(), key=lambda item: len(item[1]), reverse=True))
                
        return ticker_to_articles
    
    def get_unread_articles(self, source=None, page=1):
        limit = 100
        offset = (page - 1) * limit
        
        where_clauses = ["read = 0"]
        
        if source:
            where_clauses.append(f"source = '{source}'")

        where_sql = " AND ".join(where_clauses)

        query = f"""
            SELECT *
            FROM articles
            WHERE {where_sql}
            LIMIT {limit} OFFSET {offset};
        """
        self.cursor.execute(query)
        
        data = []
        result = self.cursor.fetchall()
        
        for row in result: 
            article = self.row_to_article(row)
            data.append(article)
        
        return data