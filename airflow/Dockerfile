FROM apache/airflow:2.9.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    "dbt-core==1.10.0" \
    "dbt-postgres==1.10.0" \
    "psycopg2-binary==2.9.*"
