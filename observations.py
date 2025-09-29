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

    def create_observation(self, type, content):
        sql = "INSERT INTO observations (type, content) VALUES (%s, %s) RETURNING id"
        self.cursor.execute(sql, (type, content))
        self.db.commit()
        return self.cursor.fetchone()[0]

    def get_observations(self, type, query):
        sql = "SELECT id, type, content FROM observations where type = %s"

        if query:
            sql += " AND content ILIKE %s"
            self.cursor.execute(sql, (type, f"%{query}%"))
        else: 
            self.cursor.execute(sql, (type,))
            
        data = [{"id": row[0], "type": row[1], "content": row[2]} for row in self.cursor.fetchall()]
        return data

    def delete_observation(self, observation_id):
        sql = "DELETE FROM observations WHERE id = %s"
        self.cursor.execute(sql, (observation_id,))
        self.db.commit()
        return self.cursor.rowcount > 0
        