
  
    

    create or replace table `wawa-83630`.`dbt_rsdun`.`reduced_articles_evidence`
    
    
    OPTIONS()
    as (
      

-- Reduce articles_evidence such that an article can only appear for a user once

SELECT base_article_id, user_id, original_timestamp, article_id
FROM (
    SELECT ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY original_timestamp, user_id DESC) row_num, base_article_id, original_timestamp, user_id, article_id
    FROM `wawa-83630`.`dbt_rsdun`.`articles_evidence`
)
WHERE row_num=1
    );
  