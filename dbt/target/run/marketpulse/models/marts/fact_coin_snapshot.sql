create or replace view "marketpulse"."analytics_marts"."fact_coin_snapshot__dbt_tmp" as
  select
  as_of_ts,
  coin_id,
  current_price,
  market_cap,
  total_volume,
  market_cap_rank,
  price_change_percentage_24h
from "marketpulse"."analytics_staging"."stg_coin_markets"