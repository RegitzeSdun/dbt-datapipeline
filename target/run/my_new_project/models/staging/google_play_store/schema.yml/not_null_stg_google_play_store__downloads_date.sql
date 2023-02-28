select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date
from `wawa-83630`.`dbt_rsdun`.`stg_google_play_store__downloads`
where date is null



      
    ) dbt_internal_test