CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.etl_run_log (
  run_id TEXT PRIMARY KEY,
  dag_id TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  status TEXT,
  row_count INTEGER,
  notes TEXT
);

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