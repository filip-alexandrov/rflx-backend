import requests
from bs4 import BeautifulSoup
import psycopg2
import time
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
from news_data import ArticleSearch

class KK_data: 
    def __init__(self) -> None:
        self.db = psycopg2.connect(
            dbname="postgres",
            user="admin",
            password="secret",
            host="localhost",
            port=5432
            )
        self.cursor = self.db.cursor()
        
        self.article_search = ArticleSearch()
        
        
    def search_comments(self, query: str, page: int, start_date: str, end_date: str, ascending: bool):
        # Parse dates (as naive datetimes). Adjust end_date to be exclusive: < next day
        # YYYY-MM-DD HH:MM or YYYY-MM-DD
        try: 
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M") if start_date else None
            
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M") if end_date else None
            

        items_per_page = 100
        offset = (page - 1) * items_per_page

        where_clauses = []
        params = []

        if start_dt:
            where_clauses.append("createdAt >= %s")
            params.append(start_dt)

        if end_dt:
            # exclusive upper bound is more robust for timestamps
            where_clauses.append("createdAt < %s")
            params.append(end_dt)

        if query:
            # GROUP the OR terms so date filters apply to both
            # use plainto_tsquery to safely parse human text
            where_clauses.append("(search_tsv @@ plainto_tsquery('english', %s) OR body ILIKE %s)")
            params.append(query)
            params.append(f"%{query}%")

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        order_by = "ASC" if ascending else "DESC"

        sql = f"""
            SELECT *
            FROM kk_comments
            {where_sql}
            ORDER BY createdAt {order_by}
            LIMIT %s OFFSET %s
        """

        params.extend([items_per_page, offset])

        self.cursor.execute(sql, tuple(params))
        rows = self.cursor.fetchall()

        data = []
        for row in rows:
            data.append({
                "id": row[0],
                "subredditNamePrefixed": row[1],
                "body": self.article_search.mark_search_keywords(self.article_search.linkify(row[2]), query),
                "score": row[3],
                "createdAt": row[5].strftime("%Y-%m-%d %H:%M:%S"),
                "permalink": row[6],
            })
        return data

    def search_posts(self, query: str, page: int, start_date: str, end_date: str): 
        limit = 100 
        offset = (page - 1) * limit

        start_date_parsed = datetime.strptime(start_date, "%Y-%m-%d") if start_date != "" else None
        end_date_parsed = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)) if end_date != "" else None
        
        where_clauses = []
        params = []
        
        if start_date_parsed:
            where_clauses.append(f"createdAt >= %s")
            params.append(start_date_parsed)
        if end_date_parsed:
            where_clauses.append(f"createdAt <= %s")
            params.append(end_date_parsed)
        if query:
            where_clauses.append(f"(search_tsv @@ to_tsquery('english', %s) OR title ILIKE %s OR selftext ILIKE %s)")
            params.extend([query, query, query])

        where_clause = " AND ".join(where_clauses)
        
        if where_clause:
            where_clause = "WHERE " + where_clause
        else:
            where_clause = ""


        sql_query = f"""
            SELECT * FROM kk_posts
            {where_clause}
            ORDER BY createdAt DESC
            LIMIT %s OFFSET %s
        """
        self.cursor.execute(sql_query, tuple(params + [limit, offset]))

        raw = self.cursor.fetchall()

        data = []

        for row in raw:
            data.append({
                "id": row[0],
                "title": row[1],
                "selfText": self.article_search.mark_search_keywords(self.article_search.linkify(row[2]), query),
                "upvoteRatio": row[3],
                "createdAt": row[5].strftime("%Y-%m-%d %H:%M:%S"),
                "permalink": row[6]
            })

        return data