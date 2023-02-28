SELECT a.slug, a.original_timestamp, a.category, u.id as user_id, a.referrer 
FROM {{source('raw_segment_android__production_events','community_viewed_article')}} as a
LEFT JOIN {{source('raw_airbyte__wawa_production_database','wawa_user')}} as u ON a.user_id=u.firebase_id