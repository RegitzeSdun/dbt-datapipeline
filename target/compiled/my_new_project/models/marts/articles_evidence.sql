

with StartDate AS (
 SELECT min(segment.original_timestamp) as start_date
 FROM `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__community_viewed_article` AS segment
),

-- merge all historical events from segment and mixpanel
historical_events AS (
    SELECT slug, original_timestamp, category, user_id, referrer 
    FROM `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__community_viewed_article` AS segment
    UNION ALL 
    SELECT slug, original_timestamp, category, user_id, referrer 
    FROM `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__viewed_article` AS segment
    UNION ALL
    SELECT slug, original_timestamp, category, user_id, referrer 
    FROM `wawa-83630`.`dbt_rsdun`.`stg_mixpanel__agg_viewed_article` AS mixpanel
    WHERE (mixpanel.original_timestamp < (select start_date From StartDate))
),

-- add article_id to the historical events
historical_events_with_article_id AS (
    SELECT he.slug, original_timestamp, he.category, user_id, referrer, metadata.sanity_id AS article_id
    FROM historical_events AS he
    LEFT JOIN `wawa-83630`.`raw_airbyte__wawa_production_database`.`community_article` AS metadata ON he.slug = metadata.slug
),

-- merge all current events from segment
current_events_with_article_id AS (
    SELECT slug, original_timestamp, category, user_id, referrer, article_id
    FROM `wawa-83630`.`dbt_rsdun`.`stg_segment_ios__screens_article` AS segment
    UNION ALL
    SELECT slug, original_timestamp, category, user_id, referrer, article_id 
    FROM `wawa-83630`.`dbt_rsdun`.`stg_segment_android__screens_article` AS segment
),

-- merge all events
all_events_with_article_id AS (
    SELECT slug, original_timestamp, category, user_id, referrer, article_id
    FROM historical_events_with_article_id
    UNION ALL 
    SELECT slug, original_timestamp, category, user_id, referrer, article_id
    FROM current_events_with_article_id
)

SELECT slug, original_timestamp, category, user_id, referrer, article_id,
CASE
WHEN STARTS_WITH(article_id, 'i18') THEN SPLIT(article_id,'.')[offset(1)]
ELSE article_id
END as base_article_id
FROM all_events_with_article_id 
WHERE article_id IS NOT NULL