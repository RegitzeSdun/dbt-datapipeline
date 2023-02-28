{{ config(
    materialized='incremental') 
}}

SELECT
  DISTINCT
  referrer, slug, user_id, category, os, max(original_timestamp) as original_timestamp
FROM {{ref('stg_mixpanel__viewed_article')}}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE original_timestamp > (select max(original_timestamp) from {{ this }})
{% endif %}
GROUP BY reading_session, referrer, slug, user_id, category, os
