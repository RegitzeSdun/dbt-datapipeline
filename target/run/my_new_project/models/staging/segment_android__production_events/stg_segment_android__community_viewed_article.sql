

  create or replace view `wawa-83630`.`dbt_rsdun`.`stg_segment_android__community_viewed_article`
  OPTIONS()
  as SELECT a.slug, a.original_timestamp, a.category, u.id as user_id, a.referrer 
FROM `wawa-83630`.`raw_segment_android__production_events`.`community_viewed_article` as a
LEFT JOIN `wawa-83630`.`raw_airbyte__wawa_production_database`.`wawa_user` as u ON a.user_id=u.firebase_id;

