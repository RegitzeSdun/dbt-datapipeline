select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_downloads
from `wawa-83630`.`dbt_rsdun`.`downloads`
where total_downloads is null



      
    ) dbt_internal_test