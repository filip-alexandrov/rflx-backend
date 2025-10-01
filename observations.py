import psycopg2


class ObservationsLibrary:
    def __init__(self, **data):
        # db connection 
        self.db = psycopg2.connect(
            dbname="postgres",
            user="admin",
            password="secret",
            host="localhost",
            port=5432
        )
        self.cursor = self.db.cursor()

    def __del__(self):
        self.cursor.close()
        self.db.close()

    def create_observation(self, description, content):
        sql = "INSERT INTO observations (description, content) VALUES (%s, %s) RETURNING id"
        self.cursor.execute(sql, (description, content))
        self.db.commit()
        result = self.cursor.fetchone()
        if result is None:
            return None  # or raise Exception("Failed to create calendar event")
        return result[0]

    def get_observations(self, query, page=1, page_size=100):
        offset = (page - 1) * page_size
        sql = "SELECT id, description, content FROM observations"
        params = []

        if query != "":
            sql += " WHERE content ILIKE %s"
            params.append(f"%{query}%")

        sql += " ORDER BY id DESC LIMIT %s OFFSET %s"
        params.extend([page_size, offset])

        self.cursor.execute(sql, tuple(params))
        data = [{"id": row[0], "description": row[1], "content": row[2]} for row in self.cursor.fetchall()]
        return data

    def delete_observation(self, observation_id):
        sql = "DELETE FROM observations WHERE id = %s"
        self.cursor.execute(sql, (observation_id,))
        self.db.commit()
        return self.cursor.rowcount > 0
    
    def update_observation(self, observation_id, description, content):
        sql = "UPDATE observations SET description = %s, content = %s WHERE id = %s"
        self.cursor.execute(sql, (description, content, observation_id))
        self.db.commit()
        return self.cursor.rowcount > 0
    
    def get_calendar_events(self):
        sql = "SELECT id, title, date FROM calendar_events ORDER BY id DESC"
        self.cursor.execute(sql)
        data = [{"id": row[0], "title": row[1], "date": row[2]} for row in self.cursor.fetchall()]
        return data
    
    def create_calendar_event(self, title, date):
        sql = "INSERT INTO calendar_events (title, date) VALUES (%s, %s) RETURNING id"
        self.cursor.execute(sql, (title, date))
        self.db.commit()
        return self.cursor.fetchone()[0]
    
    def delete_calendar_event(self, event_id): 
        sql = "DELETE FROM calendar_events WHERE id = %s"
        self.cursor.execute(sql, (event_id,))
        self.db.commit()
        return self.cursor.rowcount > 0

    def update_event(self, event_id, date):
        sql = "UPDATE calendar_events SET date = %s WHERE id = %s"
        self.cursor.execute(sql, (date, event_id))
        self.db.commit()
        return self.cursor.rowcount > 0
    

