-- @file: mart__stg__mft.sql
-- @layer: marts
-- @description: Creates a unified delivery reporting table for MASS campaigns from DCM and Basis sources.
--               Aligns schemas across platforms using UNION ALL, mapping platform-specific fields and 
--               renaming for consistency. Filters to campaigns with >10 impressions and flight dates from 2025-01-01 onward.
-- @source: looker-studio-pro-452620.repo_tables.dcm
-- @source: looker-studio-pro-452620.repo_tables.basis
-- @target: looker-studio-pro-452620.repo_tables.delivery_mft

 create or replace table looker-studio-pro-452620.repo_tables.delivery_mft as

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
  `looker-studio-pro-452620.repo_tables.dcm`
  WHERE date >= DATE '2025-01-01' 
  and package_roadblock like '%MASS%'
  and impressions >10
  
UNION ALL
SELECT
   `date`,
  `campaign`,
  `package_roadblock`,
  `placement` as placement_name,
  -- concat(placement, " - ", creative_name) as placement_name,
  `utm_source`,
  `utm_medium`,
  `utm_campaign`,
  `utm_content`,
  `utm_term`,
  `media_cost` as cost,
  `impressions`,
  `clicks`,
  creative_name as creative
  
FROM
  `looker-studio-pro-452620.repo_tables.basis`
  WHERE date >= DATE '2025-01-01' 
  and package_roadblock like '%MASS%'
  and impressions >10