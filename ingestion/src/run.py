import os
import time
from datetime import datetime, timezone

import requests
import psycopg2

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"


def get_env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


def fetch_markets(vs_currency: str = "usd", per_page: int = 50, page: int = 1):
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }
    r = requests.get(COINGECKO_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
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
            """
        )
    conn.commit()


def upsert_rows(conn, as_of_ts, rows):
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
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                sql,
                (
                    as_of_ts,
                    r.get("id"),
                    r.get("symbol"),
                    r.get("name"),
                    r.get("current_price"),
                    r.get("market_cap"),
                    r.get("total_volume"),
                    r.get("market_cap_rank"),
                    r.get("price_change_percentage_24h"),
                    "coingecko",
                ),
            )
    conn.commit()


def main():
    pg_host = get_env("PG_HOST", "postgres")
    pg_port = int(get_env("PG_PORT", "5432"))
    pg_db = get_env("PG_DB")
    pg_user = get_env("PG_USER")
    pg_pass = get_env("PG_PASSWORD")

    vs_currency = os.getenv("VS_CURRENCY", "usd")
    per_page = int(os.getenv("PER_PAGE", "50"))
    page = int(os.getenv("PAGE", "1"))

    as_of_ts = datetime.now(timezone.utc)

    last_err = None
    for attempt in range(1, 6):
        try:
            data = fetch_markets(vs_currency, per_page, page)
            break
        except Exception as e:
            last_err = e
            time.sleep(2 * attempt)
    else:
        raise RuntimeError(f"Failed to fetch data after retries: {last_err}")

    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_pass,
    )

    try:
        ensure_table(conn)
        upsert_rows(conn, as_of_ts, data)
        print(f"Inserted/updated {len(data)} rows at {as_of_ts.isoformat()}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
