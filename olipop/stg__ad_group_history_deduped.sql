CREATE OR REPLACE TABLE `looker-studio-pro-452620.repo_google_ads.stg__ad_group_history_deduped` AS
    SELECT *
    FROM (
      SELECT *, 
             ROW_NUMBER() OVER (
               PARTITION BY id 
               ORDER BY updated_at DESC, _fivetran_synced DESC
             ) AS dedupe
      FROM `giant-spoon-299605.google_ads_olipop.ad_group_history`
    )
    WHERE dedupe = 1