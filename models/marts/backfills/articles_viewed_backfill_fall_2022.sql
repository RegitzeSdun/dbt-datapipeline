{{ config(
    materialized='incremental',
    enabled=false) 
}}

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
    metadata.id as article_id
FROM (
    SELECT slug, original_timestamp, category, user_id, referrer
    FROM {{ref('stg_segment_ios__community_viewed_article')}}

    UNION ALL

    SELECT slug, original_timestamp, category, user_id, referrer, NULL
    FROM {{ref('stg_segment_android__community_viewed_article')}}
) as event
LEFT JOIN {{source('raw_airbyte__wawa_production_database','community_article')}} as metadata on metadata.slug=event.slug


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where original_timestamp > (select max(original_timestamp) from {{ this }})

{% endif %}


