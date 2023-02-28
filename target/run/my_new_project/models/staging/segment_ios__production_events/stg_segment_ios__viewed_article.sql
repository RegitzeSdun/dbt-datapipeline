
        
    

    

    merge into `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__viewed_article` as DBT_INTERNAL_DEST
        using (
          

-- only 14 events
SELECT a.slug, a.timestamp as original_timestamp, a.category, u.id as user_id, a.referrer
FROM `wawa-83630`.`raw_segment_ios__production_events`.`viewed_article` as a
LEFT JOIN `wawa-83630`.`raw_airbyte__wawa_production_database`.`wawa_user` as u ON a.user_id=u.firebase_id

-- this filter will only be applied on an incremental run
WHERE (a.original_timestamp > (select max(a.original_timestamp) from `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__viewed_article`))

        ) as DBT_INTERNAL_SOURCE
        on FALSE

    

    when not matched then insert
        (`slug`, `original_timestamp`, `category`, `user_id`, `referrer`)
    values
        (`slug`, `original_timestamp`, `category`, `user_id`, `referrer`)


    