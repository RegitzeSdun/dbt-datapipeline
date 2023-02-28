{{ config(
    materialized='incremental',
    enabled=true) 
}}

-- only 14 events
SELECT a.slug, a.timestamp as original_timestamp, a.category, u.id as user_id, a.referrer
FROM {{source('raw_segment_ios__production_events','viewed_article')}} as a
LEFT JOIN {{source('raw_airbyte__wawa_production_database','wawa_user')}} as u ON a.user_id=u.firebase_id
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE (a.original_timestamp > (select max(a.original_timestamp) from {{ this }}))
{% endif %}