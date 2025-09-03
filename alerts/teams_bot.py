import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text


USER = "airflow"
PASSWORD = "airflow"
HOST = "pg-pagarme"
PORT = "5432"
DB = "airflow"
DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"
engine = create_engine(DATABASE_URL)


TEAMS_WEBHOOK = os.getenv("TEAMS_WEBHOOK")

def notify():
    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)

    with engine.connect() as conn:
        
        new_incidents = pd.read_sql(
            text("SELECT incident_id, title, summary, updated FROM incidentes WHERE updated >= :time"),
            conn,
            params={"time": one_hour_ago}
        )

        
        if new_incidents.empty:
            last_update = pd.read_sql(
                text("SELECT incident_id, title, summary, updated FROM incidentes ORDER BY updated DESC LIMIT 1"),
                conn
            )
            if not last_update.empty:
                row = last_update.iloc[0]
                message = (
                    f"Tudo normal!  \n\n"
                    f"Último incidente registrado:  \n  \n"
                    f"**{row['title']}**  \n"
                    f"Status: {row['summary']}  \n  \n"
                    f"Última atualização: {row['updated']}"
                )
            else:
                message = "Tudo normal! Nenhum incidente registrado até agora."
        else:
            rows = []
            for _, row in new_incidents.iterrows():
                rows.append(
                    f"**{row['title']}**  \n"
                    f"{row['summary']}  \n\n"
                    f"Atualizado em: {row['updated']}"
                )
            message = "Novos incidentes na última hora:\n\n" + "\n\n---\n\n".join(rows)

    
    payload = {"text": message}
    response = requests.post(TEAMS_WEBHOOK, json=payload)

    if response.status_code == 200:
        print("Mensagem enviada com sucesso para o Teams!")
    else:
        print(f"Erro ao enviar para o Teams: {response.status_code}, {response.text}")

if __name__ == "__main__":
    notify()
