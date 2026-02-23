select distinct
  coin_id,
  symbol,
  name
from "marketpulse"."analytics_staging"."stg_coin_markets"
where coin_id is not null