
  
    

    create or replace table `wawa-83630`.`dbt_rsdun`.`reduced_base_articles_evidence`
    
    
    OPTIONS()
    as (
      

-- Reduce articles_evidence such that an article can only appear for a user once based on base_article_id

SELECT base_article_id, user_id, original_timestamp
FROM (
    SELECT ROW_NUMBER() OVER (PARTITION BY base_article_id ORDER BY original_timestamp, user_id DESC) row_num, base_article_id, original_timestamp, user_id
    FROM `wawa-83630`.`dbt_rsdun`.`articles_evidence`
)
WHERE row_num=1
    );
  