from __future__ import annotations

from datetime import datetime, timezone, timedelta
import time
import requests
import psycopg2
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"


# ---------------------------
# INGEST FUNCTION
# ---------------------------

def ingest():
    as_of_ts = datetime.now(timezone.utc)

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }

    last_err = None
    for attempt in range(1, 6):
        try:
            r = requests.get(COINGECKO_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            break
        except Exception as e:
            last_err = e
            time.sleep(2 * attempt)
    else:
        raise RuntimeError(f"Failed to fetch data: {last_err}")

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "mp_postgres"),
        dbname=os.getenv("POSTGRES_DB", "marketpulse"),
        user=os.getenv("POSTGRES_USER", "marketpulse"),
        password=os.getenv("POSTGRES_PASSWORD", "CHANGE_ME_STRONG_PASSWORD"),
    )

    try:
        with conn.cursor() as cur:

            cur.execute(
                """
                CREATE SCHEMA IF NOT EXISTS raw;
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw.coin_markets (
                    coin_id TEXT NOT NULL,
                    symbol TEXT,
                    name TEXT,
                    current_price NUMERIC,
                    market_cap NUMERIC,
                    total_volume NUMERIC,
                    market_cap_rank INTEGER,
                    last_updated TIMESTAMPTZ,
                    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    source TEXT NOT NULL,
                    vs_currency TEXT NOT NULL,
                    page INTEGER NOT NULL,
                    per_page INTEGER NOT NULL,
                    PRIMARY KEY (coin_id, vs_currency, page, per_page)
                );
                """
            )

            insert_sql = """
            INSERT INTO raw.coin_markets (
                coin_id, symbol, name, current_price,
                market_cap, total_volume, market_cap_rank,
                last_updated, source,
                vs_currency, page, per_page
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (coin_id, vs_currency, page, per_page)
            DO UPDATE SET
                symbol = EXCLUDED.symbol,
                name = EXCLUDED.name,
                current_price = EXCLUDED.current_price,
                market_cap = EXCLUDED.market_cap,
                total_volume = EXCLUDED.total_volume,
                market_cap_rank = EXCLUDED.market_cap_rank,
                last_updated = EXCLUDED.last_updated,
                source = EXCLUDED.source,
                ingested_at = now();
            """

            for row in data:
                cur.execute(
                    insert_sql,
                    (
                        row.get("id"),
                        row.get("symbol"),
                        row.get("name"),
                        row.get("current_price"),
                        row.get("market_cap"),
                        row.get("total_volume"),
                        row.get("market_cap_rank"),
                        row.get("last_updated"),
                        "coingecko",
                        "usd",
                        1,
                        50,
                    ),
                )

        conn.commit()

    finally:
        conn.close()

    print(f"Inserted/updated {len(data)} rows.")


# ---------------------------
# DAG CONFIG
# ---------------------------

default_args = {
    "owner": "marketpulse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="mp_ingest_coin_markets",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["marketpulse", "ingestion", "dbt"],
    default_args=default_args,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_coin_markets",
        python_callable=ingest,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="docker exec mp_dbt dbt run --target dev",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="docker exec mp_dbt dbt test --target dev",
    )

    ingest_task >> dbt_run >> dbt_test