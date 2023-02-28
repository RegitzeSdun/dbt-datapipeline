{{ config(
    materialized='incremental') 
}}

SELECT a.slug, max(a.original_timestamp) as original_timestamp, a.reading_session, a.category, a.clinic_id, u.id as user_id, a.referrer 
FROM {{source('raw_segment_ios__production_events','community_viewed_article')}} as a
LEFT JOIN {{source('raw_airbyte__wawa_production_database','wawa_user')}} as u ON a.user_id=u.firebase_id
WHERE a.slug is not null
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
AND (a.original_timestamp > (select max(a.original_timestamp) from {{ this }}))
{% endif %}
GROUP BY a.reading_session, a.slug, a.category, a.clinic_id, user_id, a.referrer

