

SELECT event.slug, event.original_timestamp, event.category, event.user_id, event.referrer, event.clinic_id, metadata.id as article_id
FROM (
    SELECT slug, original_timestamp, category, user_id, referrer, clinic_id
    FROM `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__community_viewed_article`

    UNION ALL

    SELECT slug, original_timestamp, category, user_id, referrer, NULL
    FROM `wawa-83630`.`dbt_rsdun`.`stg_segment_android__community_viewed_article`
) as event
LEFT JOIN `wawa-83630.raw_airbyte__wawa_production_database.community_article` as metadata on metadata.slug=event.slug