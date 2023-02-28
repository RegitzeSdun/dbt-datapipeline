{{ config(materialized='table') }}

-- Reduce articles_evidence such that an article can only appear for a user once based on base_article_id

SELECT base_article_id, user_id, original_timestamp
FROM (
    SELECT ROW_NUMBER() OVER (PARTITION BY base_article_id, user_id ORDER BY original_timestamp DESC) row_num, base_article_id, original_timestamp, user_id
    FROM {{ref('articles_evidence')}}
)
WHERE row_num=1
