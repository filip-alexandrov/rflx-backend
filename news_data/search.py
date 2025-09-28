import psycopg2
from psycopg2 import sql
import json 
from datetime import datetime, timezone, timedelta
import re
from typing import List
from zoneinfo import ZoneInfo
from notifications import Notify


class ArticleSearch:
    def __init__(self) -> None:        
        db = psycopg2.connect(
        dbname="postgres",
        user="admin",
        password="secret",
        host="localhost",
        port=5432
        )
        self.cursor = db.cursor()
        
        self.notification_service = Notify()
        
    def search_fillings(self, query: str): 
        sql_query = """
            SELECT * FROM fillings
            WHERE title ILIKE %s
            ORDER BY published_at DESC
        """
        self.cursor.execute(sql_query, (f"%{query}%",))
        
        return self.cursor.fetchall()
    
    
    def mark_search_keywords(self, text, search_term): 
        sql = """
        SELECT ts_headline(
        'english',
        %(text)s,
        websearch_to_tsquery('english', %(q)s),
        'StartSel=<mark>,StopSel=</mark>,HighlightAll=TRUE'
        )
        """
        self.cursor.execute(sql, {"text": text, "q": search_term})
        marked = self.cursor.fetchone()
        if marked:
            return marked[0]
        
    def linkify(self, text: str) -> str:
        url_pattern = re.compile(r'(https?://[^\s]+)')
        return url_pattern.sub(r'<a target="_blank" href="\1">\1</a>', text)



    def match_candidate(self, url: str, table: str):
        # get all expressions from the database 
        self.cursor.execute("SELECT id, keywords, description FROM expression_store")
        expressions = self.cursor.fetchall()

        for expr in expressions:
            expr_id, keywords_raw, description = expr

            # Get timers for this expression
            self.cursor.execute(
                "SELECT timer_val FROM expression_timers WHERE expression_store_id = %s",
                (expr_id,)
            )
            timers = [row[0] for row in self.cursor.fetchall()]
            
            candidate = None
            source = ""

            if table == "articles":
                where_sql = self.build_search_sql(keywords_raw, start_date=None, end_date=None, source=None, column="title", saved_only=False)
                sql_query = f"SELECT title, source FROM {table} {where_sql} AND url = %s"
                self.cursor.execute(sql_query, (url,))
                result = self.cursor.fetchone()
                if result:
                    candidate_raw, src = result
                    candidate = self.mark_search_keywords(candidate_raw, keywords_raw)
                    source = f"[articles - {src}]"
                    
                    self.notification_service.send_notification(
                        title=keywords_raw,
                        body=f"{source} {candidate_raw}",
                    )
            elif table == "tweets":
                where_sql = self.build_search_sql(keywords_raw, start_date=None, end_date=None, source=None, column="content", saved_only=False)
                sql_query = f"SELECT content, source FROM {table} {where_sql} AND url = %s"
                self.cursor.execute(sql_query, (url,))
                result = self.cursor.fetchone()
                if result:
                    candidate_raw, src = result
                    candidate = self.mark_search_keywords(self.linkify(candidate_raw), keywords_raw)
                    source = f"[tweets - {src}]"
                    
                    self.notification_service.send_notification(
                        title=keywords_raw,
                        body=f"{source} {candidate_raw}",
                    )
                    
            elif table == "fillings":
                cik_match = re.search(r'\b\d{10}\b', keywords_raw)
                if cik_match:
                    sql_query = """
                        SELECT title FROM fillings
                        WHERE title ILIKE %s
                        AND url = %s
                    """
                    self.cursor.execute(sql_query, (f"%{cik_match.group(0)}%", url))
                    result = self.cursor.fetchone()
                    if result:
                        candidate = result[0]
                        source = "[fillings]"

                        self.notification_service.send_notification(
                            title=keywords_raw,
                            body=f"{source} {candidate}",
                        )

            # matched
            if candidate:

                # print(f"Matched expression {expr_id}: {candidate}")

                # Insert fulfilled data into expression_fulfilled_data
                self.cursor.execute(
                    """
                    INSERT INTO expression_fulfilled_data
                    (type, content, timestamp, url, expression_store_id, read)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        "match_on_candidate",
                        f"{source} {candidate}",
                        int(datetime.now(timezone.utc).timestamp()),
                        url,
                        expr_id,
                        0
                    )
                )
                self.cursor.connection.commit()

            # timer reached (unix, utc)
            current_time = int(datetime.now().timestamp())
            for timer in timers:
                if timer <= current_time:
                    # Insert timer reached event
                    self.cursor.execute(
                        """
                        INSERT INTO expression_fulfilled_data
                        (type, content, timestamp, url, expression_store_id, read)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            "timer_reached",
                            str(timer),
                            current_time,
                            "",
                            expr_id,
                            0
                        )
                    )
                    # Remove the timer from expression_timers
                    self.cursor.execute(
                        "DELETE FROM expression_timers WHERE timer_val = %s AND expression_store_id = %s",
                        (timer, expr_id)
                    )
                    self.cursor.connection.commit()
                    # print(f"Timer reached for expression {expr_id}: {description}")
                

    def add_expression(self, keywords, timers, description):
        timers = json.dumps(timers)
        
        # insert into the database
        # Insert the expression and get its id
        self.cursor.execute(
            "INSERT INTO expression_store (keywords, description) VALUES (%s, %s) RETURNING id",
            (keywords, description)
        )
        
        expression_id = self.cursor.fetchone()
        expression_id = expression_id[0] if expression_id else None

        # Insert timers into expression_timers table
        for timer in json.loads(timers):
            self.cursor.execute(
            "INSERT INTO expression_timers (timer_val, expression_store_id) VALUES (%s, %s)",
            (timer, expression_id)
            )

        self.cursor.connection.commit()
        
        
    def remove_expression(self, id):
        # Remove related fulfilled data first
        self.cursor.execute("DELETE FROM expression_fulfilled_data WHERE expression_store_id = %s", (id,))
        # Remove related timers
        self.cursor.execute("DELETE FROM expression_timers WHERE expression_store_id = %s", (id,))
        # Remove the expression itself
        self.cursor.execute("DELETE FROM expression_store WHERE id = %s", (id,))
        self.cursor.connection.commit()
        
    def update_expression(self, id, new_keywords, new_timers, new_description):
        # Update keywords and description in expression_store
        self.cursor.execute(
            "UPDATE expression_store SET keywords = %s, description = %s WHERE id = %s",
            (new_keywords, new_description, id)
        )

        # Remove old timers for this expression
        self.cursor.execute(
            "DELETE FROM expression_timers WHERE expression_store_id = %s",
            (id,)
        )

        # Insert new timers
        for timer in new_timers:
            self.cursor.execute(
                "INSERT INTO expression_timers (timer_val, expression_store_id) VALUES (%s, %s)",
                (timer, id)
            )

        self.cursor.connection.commit()
        
    # read = all, unread = 1, saved = 2
    def get_expressions(self, read="all", search="", clean=False):
        # Get all expressions
        if search != "":
            self.cursor.execute(
            "SELECT id, keywords, description FROM expression_store WHERE keywords ILIKE %s ORDER BY id DESC",
            (f"%{search}%",)
            )
        else:
            self.cursor.execute("SELECT id, keywords, description FROM expression_store ORDER BY id DESC")
            
        expressions = self.cursor.fetchall()

        results = []
        for expr in expressions:
            expr_id, keywords, description = expr

            # Get timers for this expression
            self.cursor.execute(
                "SELECT id, timer_val FROM expression_timers WHERE expression_store_id = %s ORDER BY timer_val ASC",
                (expr_id,)
            )
            timers = [row[1] for row in self.cursor.fetchall()]

            # Build fulfilled_data query based on 'read' argument
            fulfilled_query = "SELECT id, type, content, timestamp, url, read FROM expression_fulfilled_data WHERE expression_store_id = %s"
            params = [expr_id]
            if read == "unread":
                fulfilled_query += " AND read = 1"
            elif read == "saved":
                fulfilled_query += " AND read = 2"
            # else "all": no filter
            

            fulfilled_query += " ORDER BY timestamp DESC"
            self.cursor.execute(fulfilled_query, params)
            fulfilled_data = [
                {
                    "id": row[0],
                    "type": row[1],
                    "content": row[2],
                    "timestamp": row[3],
                    "url": row[4],
                    "read": row[5]
                }
                for row in self.cursor.fetchall()
            ]
            
            # if no fulfilled and clean is True, skip
            if not clean and not fulfilled_data:
                continue

            results.append({
                "id": expr_id,
                "keywords": keywords,
                "timers": timers,
                "description": description,
                "fulfilled_data": fulfilled_data,
            })

        return results
    
    # read fulfilled data
    def mark_fulfilled_data_read(self, fulfilled_id, read):
        self.cursor.execute(
            "UPDATE expression_fulfilled_data SET read = %s WHERE id = %s",
            (read, fulfilled_id)
        )
        self.cursor.connection.commit()
        
    def clean_fulfilled_data(self, expr_id):
        self.cursor.execute(
            "DELETE FROM expression_fulfilled_data WHERE expression_store_id = %s",
            (expr_id,)
        )
        self.cursor.connection.commit()
    
    
    def build_search_sql(self, query: str, start_date, end_date, \
                        source, column, saved_only):
        # create utc unix seconds timestamp 
        if start_date:
            try: 
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M")
                
            # convert from ny time to utc unix seconds
            ny_time = start_date.replace(tzinfo=ZoneInfo("America/New_York"))
            start_date = int(ny_time.timestamp())

        if end_date:
            try:
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M")

            # convert from ny time to utc unix seconds
            ny_time = end_date.replace(tzinfo=ZoneInfo("America/New_York"))
            end_date = int(ny_time.timestamp())

        # build the WHERE clause
        where_clauses = []

        # keywords
        if query:
            where_clauses.append(f"{column}_tsv @@ to_tsquery('{query}')")

        # time range
        if start_date:
            where_clauses.append(f"published_at >= {start_date}")
        if end_date:
            where_clauses.append(f"published_at <= {end_date}")
            
        # source
        if source:
            where_clauses.append(f"source = '{source}'")
            
        # saved -> read = 2
        if saved_only:
            where_clauses.append(f"read = 2")

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)
            
        return where_sql

    def search_articles(self, query: str, start_date, end_date, \
                        source, page, saved_only):
        
        column="title"

        where_sql = self.build_search_sql(query, start_date, end_date, source, column, saved_only)

        limit = 100
        offset = (page - 1) * limit

        sql_query = f"SELECT * FROM articles {where_sql} ORDER BY published_at DESC LIMIT {limit} OFFSET {offset}"
        
        self.cursor.execute(sql_query)
        rows = self.cursor.fetchall()
        
        results = []
        for row in rows:
            article = {
                "url": row[0],
                "source": row[1],
                "published_at": row[2],
                "title": self.mark_search_keywords(row[3], query),
                "read": row[5]
            }

            results.append(article)
            
        return results
            
        
    def search_tweets(self, query: str, start_date=None, end_date=None, \
                        source=None, page = 1, saved_only=False):
        column="content"
        
        where_sql = self.build_search_sql(query, start_date, end_date, source, column, saved_only)

        limit = 100
        offset = (page - 1) * limit
        
        sql_query = f"SELECT * FROM tweets {where_sql} ORDER BY published_at DESC LIMIT {limit} OFFSET {offset}"
        self.cursor.execute(sql_query)
        
        # fetch all rows
        rows = self.cursor.fetchall()
        
        results = []
        for row in rows:
            tweet = {
                "url": row[0],
                "source": row[1],
                "published_at": row[2],
                "content": self.mark_search_keywords(self.linkify(row[3]), query),
                "read": row[5]
            }
            
            results.append(tweet)
            
        return results     