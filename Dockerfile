FROM apache/airflow:2.9.0-python3.10

USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir feedparser beautifulsoup4 requests


COPY ./dags /opt/airflow/dags
COPY ./etl /opt/airflow/etl
COPY ./alerts /opt/airflow/alerts
COPY ./db /opt/airflow/db
COPY ./data /opt/airflow/data


ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor


EXPOSE 8080


CMD ["airflow", "standalone"]