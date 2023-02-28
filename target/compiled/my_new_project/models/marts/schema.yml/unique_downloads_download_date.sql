
    
    

with dbt_test__target as (

  select download_date as unique_field
  from `wawa-83630`.`dbt_rsdun`.`downloads`
  where download_date is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


