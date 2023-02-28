{{ config(
    materialized='incremental') 
}}


WITH extracted_events AS (
    SELECT
    DISTINCT
      CASE
      WHEN JSON_VALUE(properties,'$.Referrer') = 'Community' THEN 'community'
      WHEN JSON_VALUE(properties,'$.Referrer') = 'Personal assistant' THEN 'home'
      WHEN JSON_VALUE(properties,'$.Referrer') = 'Content' THEN 'content'
      WHEN JSON_VALUE(properties,'$.Referrer') = 'Health' THEN 'health'
      WHEN JSON_VALUE(properties,'$.Referrer') = 'Treatment' THEN 'treatment'
      WHEN JSON_VALUE(properties,'$.Referrer') = 'Journal' THEN 'journal'    
      ELSE JSON_VALUE(properties,'$.Referrer')
    END as referrer,
    JSON_VALUE(properties,'$.Slug') as slug,
    JSON_VALUE(properties,'$."$insert_id"') as event_id,
    JSON_VALUE(properties,'$.distinct_id') as user_id,
    JSON_VALUE(properties,'$.Category') as category,
    JSON_VALUE(properties,'$.os') as os,
    JSON_VALUE(properties,'$.Reading session') as reading_session,
    TIMESTAMP_SECONDS(CAST(JSON_VALUE(properties,'$.time') AS INT64)) as original_timestamp,
  FROM `wawa-83630.raw_mixpanel__events.raw_mixpanel__events`
  where (event = 'Viewed article')
)

SELECT e.referrer, e.slug, e.user_id, e.category, e.os, e.original_timestamp, e.reading_session
FROM extracted_events as e
WHERE REGEXP_CONTAINS(e.user_id, r'-')

UNION ALL

SELECT e.referrer, e.slug, u.id as user_id, e.category, e.os, e.original_timestamp, e.reading_session
FROM extracted_events as e
LEFT JOIN `wawa-83630.raw_airbyte__wawa_production_database.wawa_user` as u ON e.user_id=u.firebase_id
WHERE NOT REGEXP_CONTAINS(e.user_id, r'-')