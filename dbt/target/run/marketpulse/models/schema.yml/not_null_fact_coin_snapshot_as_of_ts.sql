select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select as_of_ts
from "marketpulse"."analytics_marts"."fact_coin_snapshot"
where as_of_ts is null



      
    ) dbt_internal_test