from __future__ import annotations

from datetime import datetime, timedelta
import requests
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def ingest_coin_markets():
    # Airflow Variables (UI > Admin > Variables)
    # If you don't set them, defaults below will be used.
    api_url = Variable.get(
        "COINGECKO_MARKETS_URL",
        default_var="https://api.coingecko.com/api/v3/coins/markets",
    )
    vs_currency = Variable.get("COINGECKO_VS_CURRENCY", default_var="usd")
    per_page = int(Variable.get("COINGECKO_PER_PAGE", default_var="50"))
    page = int(Variable.get("COINGECKO_PAGE", default_var="1"))

    pg_host = Variable.get("POSTGRES_HOST", default_var="mp_postgres")
    pg_port = int(Variable.get("POSTGRES_PORT", default_var="5432"))
    pg_db = Variable.get("POSTGRES_DB", default_var="marketpulse")
    pg_user = Variable.get("POSTGRES_USER", default_var="marketpulse")
    pg_pass = Variable.get("POSTGRES_PASSWORD", default_var="marketpulse")

    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }

    r = requests.get(api_url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    now_ts = datetime.utcnow()

    rows = []
    for item in data:
        rows.append(
            (
                now_ts,
                item.get("id"),
                item.get("symbol"),
                item.get("name"),
                item.get("current_price"),
                item.get("market_cap"),
                item.get("market_cap_rank"),
                item.get("total_volume"),
                item.get("price_change_percentage_24h"),
                "coingecko_api",
            )
        )

    if not rows:
        return

    conn = psycopg2.connect(
        host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_pass
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        # Ensure schema/table exists (idempotent)
        cur.execute("create schema if not exists raw;")
        cur.execute(
            """
            create table if not exists raw.coin_markets (
              as_of_ts timestamptz not null,
              coin_id text not null,
              symbol text,
              name text,
              current_price numeric,
              market_cap numeric,
              market_cap_rank int,
              total_volume numeric,
              price_change_percentage_24h numeric,
              source text,
              primary key (as_of_ts, coin_id)
            );
            """
        )

        cur.executemany(
            """
            insert into raw.coin_markets
            (as_of_ts, coin_id, symbol, name, current_price, market_cap, market_cap_rank, total_volume, price_change_percentage_24h, source)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            on conflict do nothing;
            """,
            rows,
        )

    conn.close()


default_args = {
    "owner": "marketpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="coin_markets_ingest",
    default_args=default_args,
    start_date=datetime(2026, 2, 1),
    schedule="*/15 * * * *",  # every 15 minutes
    catchup=False,
    tags=["marketpulse", "ingestion"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_coin_markets",
        python_callable=ingest_coin_markets,
    )