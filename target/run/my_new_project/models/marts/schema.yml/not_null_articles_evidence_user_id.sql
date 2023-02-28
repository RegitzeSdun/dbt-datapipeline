select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select user_id
from `wawa-83630`.`dbt_rsdun`.`articles_evidence`
where user_id is null



      
    ) dbt_internal_test