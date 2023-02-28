{{ config(
    materialized='incremental',
    enabled=true) 
}}

SELECT a.category, a.name, a.original_timestamp, u.id as user_id, a.referrer, a.article_id, a.slug  
FROM {{source('raw_segment_ios__production_events','screens')}} as a
LEFT JOIN {{source('raw_airbyte__wawa_production_database','wawa_user')}} as u ON a.user_id=u.firebase_id
WHERE a.name='Article'
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
AND (a.original_timestamp > (select max(a.original_timestamp) from {{ this }}))
{% endif %}