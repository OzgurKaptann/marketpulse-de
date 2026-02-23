from datetime import datetime, timezone
import time
import requests
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"


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
        host="postgres",
        port=5432,
        dbname="marketpulse",
        user="de_user",
        password="de_pass",
    )

    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;

                CREATE TABLE IF NOT EXISTS raw.coin_markets (
                  as_of_ts TIMESTAMPTZ NOT NULL,
                  coin_id TEXT NOT NULL,
                  symbol TEXT,
                  name TEXT,
                  current_price NUMERIC,
                  market_cap NUMERIC,
                  total_volume NUMERIC,
                  market_cap_rank INTEGER,
                  price_change_percentage_24h NUMERIC,
                  source TEXT DEFAULT 'coingecko',
                  PRIMARY KEY (as_of_ts, coin_id)
                );
            """)

            sql = """
            INSERT INTO raw.coin_markets (
              as_of_ts, coin_id, symbol, name,
              current_price, market_cap, total_volume, market_cap_rank,
              price_change_percentage_24h, source
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (as_of_ts, coin_id) DO UPDATE SET
              symbol = EXCLUDED.symbol,
              name = EXCLUDED.name,
              current_price = EXCLUDED.current_price,
              market_cap = EXCLUDED.market_cap,
              total_volume = EXCLUDED.total_volume,
              market_cap_rank = EXCLUDED.market_cap_rank,
              price_change_percentage_24h = EXCLUDED.price_change_percentage_24h,
              source = EXCLUDED.source;
            """

            for row in data:
                cur.execute(sql, (
                    as_of_ts,
                    row.get("id"),
                    row.get("symbol"),
                    row.get("name"),
                    row.get("current_price"),
                    row.get("market_cap"),
                    row.get("total_volume"),
                    row.get("market_cap_rank"),
                    row.get("price_change_percentage_24h"),
                    "coingecko",
                ))

        conn.commit()
    finally:
        conn.close()

    print(f"Inserted/updated {len(data)} rows at {as_of_ts.isoformat()}")


with DAG(
    dag_id="mp_ingest_coin_markets",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["marketpulse", "ingestion"],
) as dag:
    PythonOperator(
        task_id="ingest_coin_markets",
        python_callable=ingest,
    )
