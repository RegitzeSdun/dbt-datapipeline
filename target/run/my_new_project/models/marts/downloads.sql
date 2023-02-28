
  
    

    create or replace table `wawa-83630`.`dbt_rsdun`.`downloads`
    
    
    OPTIONS()
    as (
      

SELECT 
  CASE WHEN g.date IS NULL THEN a.date ELSE g.date END AS download_date, 
  a.downloads as apple_downloads, 
  g.downloads as google_downloads, 
  IFNULL(a.downloads,0)+IFNULL(g.downloads,0) as total_downloads
FROM `wawa-83630`.`dbt_rsdun`.`stg_google_play_store__downloads` as g
      FULL OUTER JOIN `wawa-83630`.`dbt_rsdun`.`stg_apple_app_store__downloads` AS a ON a.date=g.date
ORDER BY download_date DESC
    );
  