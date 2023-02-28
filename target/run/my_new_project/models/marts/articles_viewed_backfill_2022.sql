
        
    

    

    merge into `wawa-83630`.`dbt_rsdun`.`articles_viewed_backfill_2022` as DBT_INTERNAL_DEST
        using (
          

/*
'Community | Viewed article' events from 2022-10-07 07:58:22.030000 UTC to 2022-12-08 12:57:18.498000 UTC
Very few for android
*/


SELECT 
    event.slug, 
    event.original_timestamp, 
    event.category, 
    event.user_id, 
    CASE
        WHEN event.referrer = 'Personal assistant' THEN 'home'
        WHEN event.referrer = 'Community' THEN 'community'
        ELSE event.referrer
    END as referrer,
    event.clinic_id, 
    metadata.id as article_id
FROM (
    SELECT slug, original_timestamp, category, user_id, referrer, clinic_id
    FROM `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__community_viewed_article`

    UNION ALL

    SELECT slug, original_timestamp, category, user_id, referrer, NULL
    FROM `wawa-83630`.`dbt_rsdun`.`stg_segment_android__community_viewed_article`
) as event
LEFT JOIN `wawa-83630.raw_airbyte__wawa_production_database.community_article` as metadata on metadata.slug=event.slug




  -- this filter will only be applied on an incremental run
  where original_timestamp > (select max(original_timestamp) from `wawa-83630`.`dbt_rsdun`.`articles_viewed_backfill_2022`)


        ) as DBT_INTERNAL_SOURCE
        on FALSE

    

    when not matched then insert
        (`slug`, `original_timestamp`, `category`, `user_id`, `referrer`, `clinic_id`, `article_id`)
    values
        (`slug`, `original_timestamp`, `category`, `user_id`, `referrer`, `clinic_id`, `article_id`)


    