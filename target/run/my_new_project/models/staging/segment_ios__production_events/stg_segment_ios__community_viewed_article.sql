
        
    

    

    merge into `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__community_viewed_article` as DBT_INTERNAL_DEST
        using (
          

SELECT a.slug, max(a.original_timestamp) as original_timestamp, a.reading_session, a.category, a.clinic_id, u.id as user_id, a.referrer 
FROM `wawa-83630`.`raw_segment_ios__production_events`.`community_viewed_article` as a
LEFT JOIN `wawa-83630`.`raw_airbyte__wawa_production_database`.`wawa_user` as u ON a.user_id=u.firebase_id
WHERE a.slug is not null

-- this filter will only be applied on an incremental run
AND (a.original_timestamp > (select max(a.original_timestamp) from `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__community_viewed_article`))

GROUP BY a.reading_session, a.slug, a.category, a.clinic_id, user_id, a.referrer
        ) as DBT_INTERNAL_SOURCE
        on FALSE

    

    when not matched then insert
        (`slug`, `original_timestamp`, `reading_session`, `category`, `clinic_id`, `user_id`, `referrer`)
    values
        (`slug`, `original_timestamp`, `reading_session`, `category`, `clinic_id`, `user_id`, `referrer`)


    