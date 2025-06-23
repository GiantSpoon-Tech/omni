-- @file: mart__stg__mft.sql
-- @layer: marts
-- @description: Creates a unified delivery reporting table for MASS campaigns from DCM and Basis sources.
--               Aligns schemas across platforms using UNION ALL, mapping platform-specific fields and 
--               renaming for consistency. Filters to campaigns with >10 impressions and flight dates from 2025-01-01 onward.
-- @source: `looker-studio-pro-452620.repo_stg.dcm_plus_utms`
-- @source: `looker-studio-pro-452620.repo_stg.basis_plus_utms`
-- @target: 'looker-studio-pro-452620.repo_mart.mft_view'

 create or replace view looker-studio-pro-452620.repo_mart.mft_view as

SELECT
  `date`,
  `campaign`,
  `package_roadblock`,
  `placement_name`,
  `utm_source`,
  `utm_medium`,
  `utm_campaign`,
  `utm_content`,
  `utm_term`,
  `daily_recalculated_cost` as cost,
  `impressions`,
  `clicks`,
  creative
  
FROM
  `looker-studio-pro-452620.repo_stg.dcm_plus_utms`
  WHERE date >= DATE '2025-01-01' 
  and package_roadblock like '%MASS%'
  and impressions >10
  
UNION ALL
SELECT
   `date`,
  `campaign`,
  `package_roadblock`,
  --`placement` as placement_name,
   concat(placement, " -- ", creative_name) as placement_name,
  `utm_source`,
  `utm_medium`,
  `utm_campaign`,
  `utm_content`,
  `utm_term`,
  `media_cost` as cost,
  `impressions`,
  `clicks`,
  cleaned_creative_name as creative
  
FROM
  --`looker-studio-pro-452620.repo_stg.basis_plus_utms` update 6/17
  `looker-studio-pro-452620.repo_stg.basis_plus_utms_v2`
  WHERE date >= DATE '2025-01-01' 
  and package_roadblock like '%MASS%'
  and impressions >10;


create or replace view looker-studio-pro-452620.repo_mart.mft_clean_view as (
SELECT
  date,
  campaign,
  placement_name,
  utm_source,
  utm_medium,
  utm_campaign,
  utm_content,
  utm_term,
  SUM(cost) as Cost,
  SUM(impressions) as Impressions,
  SUM(clicks) as Clicks
FROM
  `looker-studio-pro-452620.repo_mart.mft_view`
GROUP BY 1,2,3,4,5,6,7,8  
 )