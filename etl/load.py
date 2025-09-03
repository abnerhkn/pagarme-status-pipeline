import os
import pandas as pd
from sqlalchemy import create_engine, text

SILVER_DIR = "/opt/airflow/data/silver"
USER = "airflow"
PASSWORD = "airflow"
HOST = "pg-pagarme"
PORT = "5432"
DB = "airflow"
DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"
engine = create_engine(DATABASE_URL)

def load():
    silver_files = [f for f in os.listdir(SILVER_DIR) if f.endswith(".parquet")]
    if not silver_files:
        print("[Gold] Nenhum arquivo disponível na Silver.")
        return

    latest_file = max(
        silver_files, key=lambda f: os.path.getmtime(os.path.join(SILVER_DIR, f))
    )
    file_path = os.path.join(SILVER_DIR, latest_file)

    df = pd.read_parquet(file_path)

    
    if "status" not in df.columns:
        def extract_status(summary):
            if isinstance(summary, str) and " - " in summary:
                first_part = summary.split(" - ")[0]  
                return first_part.split()[0]          
            return "UNKNOWN"

        df["status"] = df["summary"].apply(extract_status)

    with engine.begin() as conn:
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS incidentes (
                incident_id VARCHAR(50) PRIMARY KEY,
                title TEXT NOT NULL,
                summary TEXT,
                published TIMESTAMP,
                updated TIMESTAMP,
                status TEXT
            );
        """))

        
        conn.execute(text("TRUNCATE TABLE incidentes RESTART IDENTITY CASCADE"))

        
        df.to_sql("incidentes", conn, if_exists="append", index=False)

    print(f"[Gold] Registros inseridos: {len(df)}")

if __name__ == "__main__":
    load()
