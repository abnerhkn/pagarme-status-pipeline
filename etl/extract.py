import feedparser
import json
from datetime import datetime
import os


URL_FEED = "https://status.pagar.me/history.atom"


BRONZE_DIR = "/opt/airflow/data/bronze"
os.makedirs(BRONZE_DIR, exist_ok=True)
def extract():
    
    feed = feedparser.parse(URL_FEED)

    
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    filename = os.path.join(BRONZE_DIR, f"incidents_{timestamp}.json")

    
    incidents = []
    for entry in feed.entries:
        incidents.append({
            "id": entry.get("id"),
            "title": entry.get("title"),
            "published": entry.get("published"),
            "updated": entry.get("updated"),
            "summary": entry.get("summary"),
            
        })

    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(incidents, f, indent=4, ensure_ascii=False)

    print(f"[Bronze] Arquivo salvo em: {filename}")

if __name__ == "__main__":
    extract()
