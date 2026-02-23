select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select coin_id
from "marketpulse"."analytics_marts"."fact_coin_snapshot"
where coin_id is null



      
    ) dbt_internal_test