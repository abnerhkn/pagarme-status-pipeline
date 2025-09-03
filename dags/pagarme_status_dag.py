from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pagarme_status_pipeline",
    default_args=default_args,
    description="Pipeline para monitorar status.pagar.me e salvar no Postgres",
    schedule_interval="0 */2 * * *",  
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["pagarme", "status", "etl"],
) as dag:

    
    extract = BashOperator(
        task_id="extract",
        bash_command="python3 /opt/airflow/etl/extract.py",
    )

    
    transform = BashOperator(
        task_id="transform",
        bash_command="python3 /opt/airflow/etl/transform.py",
    )

    
    load = BashOperator(
        task_id="load",
        bash_command="python3 /opt/airflow/etl/load.py",
    )
    
    notify = BashOperator(
    task_id="notify",
    bash_command="python3 /opt/airflow/alerts/teams_bot.py",
    dag=dag,
    )


    
    extract >> transform >> load >> notify
