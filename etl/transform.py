import os
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

BRONZE_DIR = "/opt/airflow/data/bronze"
SILVER_DIR = "/opt/airflow/data/silver"


def transform():
    bronze_files = [f for f in os.listdir(BRONZE_DIR) if f.endswith(".json")]

    if not bronze_files:
        print("[Silver] Nenhum arquivo disponível na Bronze.")
        return

    latest_file = max(
        bronze_files, key=lambda f: os.path.getmtime(os.path.join(BRONZE_DIR, f))
    )
    file_path = os.path.join(BRONZE_DIR, latest_file)

    df = pd.read_json(file_path)


    
    df = df.rename(columns={"id": "incident_id"})

    
    for col in ["published", "updated"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")


    
    def clean_summary(html):
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        strong_tags = soup.find_all("strong")
        if strong_tags:
            return strong_tags[0].get_text(strip=True) + " - " + strong_tags[0].next_sibling.strip()
        return soup.get_text(" ", strip=True)

    df["summary"] = df["summary"].apply(clean_summary)

    
    silver_file = os.path.join(
        SILVER_DIR, f"incidentes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    )
    df.to_parquet(silver_file, index=False)

    print(f"[Silver] Dados transformados e salvos em {silver_file}")


if __name__ == "__main__":
    transform()
