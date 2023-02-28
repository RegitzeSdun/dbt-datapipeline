
        
    

    

    merge into `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__screens_article` as DBT_INTERNAL_DEST
        using (
          

SELECT a.category, a.name, a.original_timestamp, u.id as user_id, a.referrer, a.article_id, a.slug  
FROM `wawa-83630`.`raw_segment_ios__production_events`.`screens` as a
LEFT JOIN `wawa-83630`.`raw_airbyte__wawa_production_database`.`wawa_user` as u ON a.user_id=u.firebase_id
WHERE a.name='Article'

-- this filter will only be applied on an incremental run
AND (a.original_timestamp > (select max(a.original_timestamp) from `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__screens_article`))

        ) as DBT_INTERNAL_SOURCE
        on FALSE

    

    when not matched then insert
        (`category`, `name`, `original_timestamp`, `user_id`, `referrer`, `article_id`, `slug`)
    values
        (`category`, `name`, `original_timestamp`, `user_id`, `referrer`, `article_id`, `slug`)


    