import requests
from bs4 import BeautifulSoup
from .shared import Shared
import os
import json
from datetime import datetime
import time

class SecFetcher: 
    def __init__(self):
        self.shared = Shared() 
        
        self.SEC_SITEMAP = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=&company=&dateb=&owner=include&start=0&count=400&output=atom"
        
    def parse_dt(self, ts: str) -> int:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return int(parsed.timestamp())
    
    def parse_sec_atom(self, xml_text: str):
        soup = BeautifulSoup(xml_text, "xml")
        
        new_fillings = 0 

        for entry in soup.find_all("entry"):
            title = entry.find("title").get_text(" ", strip=True) if entry.find("title") else None
            href = entry.find("link").get("href") if entry.find("link") else None

            # Per-entry updated
            ent_updated = None
            ent_upd = entry.find("updated")
            if ent_upd:
                ent_updated = self.parse_dt(ent_upd.get_text(strip=True))

            # Category (form type)
            cat = entry.find("category")
            category_term = cat.get("term") if cat else ""
            category_label = cat.get("label") if cat else ""
            
            category = f"{category_label} ({category_term})" 
                    
            # Accession urn/id
            entry_id = entry.find("id").get_text(strip=True) if entry.find("id") else None

            # save to db
            if href and not self.shared.filling_url_fetched(href):
                self.shared.save_filling(href, title, ent_updated, category)
                new_fillings += 1
                
        print(f"Fetched {new_fillings} new fillings from SEC")

    def fetch(self):
        response = requests.get(self.SEC_SITEMAP, headers=self.shared.get_headers("bloomberg"))
        response.raise_for_status()
        
        self.shared.log_request_response("sec.atom", response.text)
        
        self.parse_sec_atom(response.text)




