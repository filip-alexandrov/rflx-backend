import psycopg2
from pywebpush import webpush, WebPushException
import json
from fastapi import HTTPException
import requests



class Notify:
    def __init__(self):
        self.vapid_private_key = "uorVfvwax8Rh7lA8igvBRyNLKJSkf7guUorLHjfu1Cc",
        self.vapid_public_key = "BLQYvbumjA05-TybyLoL5kzKQbqHb-UmPjKCo5txUg3BBC2Fx2PXzt7JoLJ4G8ssHB8-DhyjnJDdiiaVwpeZeo8"
        
        # db connection 
        self.db = psycopg2.connect(
            dbname="postgres",
            user="admin",
            password="secret",
            host="localhost",
            port=5432
        )
        self.cursor = self.db.cursor()
        
        self.USER_KEY = 'u68oqk5688d4rbpus83ez9e4ek1tsf'
        self.API_TOKEN = 'arsmjqav873gxbprt17zden5sq35s8'

        
    def __del__(self):
        self.cursor.close()
        self.db.close()


    def subscribe(self, endpoint, p256dh, auth):  
        data = {
            "title": "Subscription Test",
            "body": "Successful subscription!",
        }

        try:
            webpush(
                subscription_info={
                    "endpoint": endpoint,
                    "keys": {
                        "p256dh": p256dh,
                        "auth": auth
                    }},
                data=json.dumps(data),
                vapid_private_key="uorVfvwax8Rh7lA8igvBRyNLKJSkf7guUorLHjfu1Cc",
                vapid_claims={
                        "sub": "mailto:YourNameHere@example.org",
                    }
            )
            
            # save endpoint to database
            insert_query = """INSERT INTO subscriptions (endpoint, p256dh, auth) VALUES (%s, %s, %s)
                              ON CONFLICT (endpoint) DO NOTHING;"""
                              
            self.cursor.execute(insert_query, (endpoint, p256dh, auth))
            self.db.commit()

            return {"success": True, "message": "Subscription successful."}
        except WebPushException as ex:
            print("Subscription send error: {}", repr(ex))
            # Mozilla returns additional information in the body of the response.
            if ex.response is not None and ex.response.json():
                extra = ex.response.json()
                print("Remote service replied with a {}:{}, {}",
                    extra.code,
                    extra.errno,
                    extra.message
                    )
                
            raise HTTPException(status_code=500, detail="Subscription failed.")    
        
        
    def send_notification(self, title: str, body: str): 
        payload = {
            'token': self.API_TOKEN,
            'user': self.USER_KEY,
            'message': body,
            'title': title,
            'priority': 0,
        }
        requests.post('https://api.pushover.net/1/messages.json', data=payload)

        # # limits
        # if len(body) > 500:
        #     body = body[:500] + "..."
        
        # # get all subscriptions from database
        # self.cursor.execute("SELECT endpoint, p256dh, auth FROM subscriptions;")
        # subscriptions = self.cursor.fetchall()
        
        # data = { 
        #     "title": title,
        #     "body": body,
        #     }
        
        # # send to each subscription
        # for sub in subscriptions:
        #     endpoint, p256dh, auth = sub
        #     try:
        #         webpush(
        #             subscription_info={
        #                 "endpoint": endpoint,
        #                 "keys": {
        #                     "p256dh": p256dh,
        #                     "auth": auth
        #                 }},
        #             data=json.dumps(data),
        #             vapid_private_key="uorVfvwax8Rh7lA8igvBRyNLKJSkf7guUorLHjfu1Cc",
        #             vapid_claims={
        #                     "sub": "mailto:YourNameHere@example.org",
        #             }
        #         )
        #     except WebPushException as ex:
        #         print("Subscription send error: {}", repr(ex))
        #         # Mozilla returns additional information in the body of the response.
        #         if ex.response is not None and ex.response.json():
        #             extra = ex.response.json()
        #             print("Remote service replied with a {}:{}, {}",
        #                 extra.code,
        #                 extra.errno,
        #                 extra.message
        #             )
        #         # Optionally, remove invalid subscriptions from the database
        #         if ex.response is not None and ex.response.status_code == 410:
        #             self.cursor.execute("DELETE FROM subscriptions WHERE endpoint = %s;", (endpoint,))
        #             self.db.commit()  
                    