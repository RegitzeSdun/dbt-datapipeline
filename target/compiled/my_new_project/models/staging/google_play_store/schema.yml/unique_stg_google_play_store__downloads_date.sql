
    
    

with dbt_test__target as (

  select date as unique_field
  from `wawa-83630`.`dbt_rsdun`.`stg_google_play_store__downloads`
  where date is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


