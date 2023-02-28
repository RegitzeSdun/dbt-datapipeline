

SELECT
  DISTINCT
  referrer, slug, user_id, category, os, max(original_timestamp) as original_timestamp
FROM `wawa-83630`.`dbt_rsdun`.`stg_mixpanel__viewed_article`

-- this filter will only be applied on an incremental run
WHERE original_timestamp > (select max(original_timestamp) from `wawa-83630`.`dbt_rsdun`.`stg_mixpanel__agg_viewed_article`)

GROUP BY reading_session, referrer, slug, user_id, category, os