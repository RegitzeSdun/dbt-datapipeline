select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select download_date
from `wawa-83630`.`dbt_rsdun`.`downloads`
where download_date is null



      
    ) dbt_internal_test