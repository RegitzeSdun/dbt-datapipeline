

  create or replace view `wawa-83630`.`dbt_rsdun`.`stg_apple_app_store__downloads`
  OPTIONS()
  as SELECT Begin_Date as date, SUM(Units) as downloads
FROM `wawa-83630`.`raw_apple_app_store_connect__reports`.`sales_and_trends_reports`
WHERE Product_Type_Identifier="1"
GROUP BY Begin_Date
ORDER BY Begin_Date DESC;

