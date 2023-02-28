select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select original_timestamp
from `wawa-83630`.`dbt_rsdun`.`articles_evidence`
where original_timestamp is null



      
    ) dbt_internal_test