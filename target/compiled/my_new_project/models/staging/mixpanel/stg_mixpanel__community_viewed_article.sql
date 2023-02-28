

SELECT
  CASE
    WHEN JSON_VALUE(properties,'$.referrer') = 'Community' THEN 'community'
    WHEN JSON_VALUE(properties,'$.referrer') = 'Personal assistant' THEN 'home'
    ELSE JSON_VALUE(properties,'$.referrer')
  END as referrer,
  JSON_VALUE(properties,'$.slug') as slug,
  JSON_VALUE(properties,'$.distinct_id') as user_id,
  JSON_VALUE(properties,'$.category') as category,
  JSON_VALUE(properties,'$.os') as os,
  max(TIMESTAMP_SECONDS(CAST(JSON_VALUE(properties,'$.time') AS INT64))) as original_timestamp,
FROM `wawa-83630.raw_mixpanel__events.raw_mixpanel__events`
WHERE (event = 'Community | Viewed article')
GROUP BY JSON_VALUE(properties,'$.reading_session'), referrer, slug, user_id, category, os