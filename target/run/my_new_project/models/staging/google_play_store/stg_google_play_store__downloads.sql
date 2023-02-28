

  create or replace view `wawa-83630`.`dbt_rsdun`.`stg_google_play_store__downloads`
  OPTIONS()
  as SELECT Date as date, SUM(Store_listing_acquisitions) as downloads
FROM `wawa-83630`.`raw_google_play__reports`.`p_Store_Performance_traffic_source_raw`
WHERE Date >= "2022-08-29"
GROUP BY Date
ORDER BY Date DESC;

