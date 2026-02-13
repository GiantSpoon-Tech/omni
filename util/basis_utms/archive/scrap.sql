--- Placement

SELECT 
  --placement as dim,

  SUM(impressions) as total_impressions,
  -- Count of unique placements where utm_medium is null
  COUNT(DISTINCT CASE WHEN utm_medium IS NULL THEN creative_name END) as unique_creative_utm_medium_null,
  -- Count of unique placements (total)
  COUNT(DISTINCT creative_name) as unique_placements_total,
  COUNT(DISTINCT CASE WHEN utm_medium IS NULL THEN del_key END) as unique_del_keys,

  -- Count of unique del_ids
  COUNT(DISTINCT del_key) as unique_del_keys,
  -- Sum of impressions where utm_medium is null for unique del_ids
  SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END) as impressions_utm_medium_null,
  -- Sum of impressions where utm_medium is null and the percent of total
  SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END) as impressions_utm_medium_null_sum,
  ROUND(safe_divide(SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END),SUM(impressions)) * 100, 
    2) as percent_impressions_utm_medium_null

FROM looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table
WHERE 
  date >= DATE '2025-01-01' and
  placement LIKE '%003%' and
  utm_source is null
group by 1;

--- Placement

SELECT 

  creative_name,
  
  SUM(impressions) as total_impressions,
  -- Count of unique placements where utm_medium is null
  COUNT(DISTINCT CASE WHEN utm_medium IS NULL THEN placement END) as unique_placements_utm_medium_null,
  -- Count of unique placements (total)
  COUNT(DISTINCT placement) as unique_placements_total,
  -- Count of unique del_ids
  COUNT(DISTINCT del_key) as unique_del_ids,
  -- Sum of impressions where utm_medium is null for unique del_ids
  SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END) as impressions_utm_medium_null,
  -- Sum of impressions where utm_medium is null and the percent of total
  SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END) as impressions_utm_medium_null_sum,
  ROUND(safe_divide(SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END),SUM(impressions)) * 100, 
    2) as percent_impressions_utm_medium_null

FROM looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table
WHERE 
  date >= DATE '2025-01-01' and
  placement LIKE '%003%' and
  utm_source is null
group by 1,2,3,4;

--- Placement

SELECT 
  placement as dim,
  creative_name,
  cleaned_creative_name,
  del_key,
  MAX(date),
  SUM(impressions) as total_impressions,
  -- Count of unique placements where utm_medium is null
  COUNT(DISTINCT CASE WHEN utm_medium IS NULL THEN placement END) as unique_placements_utm_medium_null,
  -- Count of unique placements (total)
  COUNT(DISTINCT placement) as unique_placements_total,
  -- Count of unique del_ids
  COUNT(DISTINCT del_key) as unique_del_ids,
  -- Sum of impressions where utm_medium is null for unique del_ids
  SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END) as impressions_utm_medium_null,
  -- Sum of impressions where utm_medium is null and the percent of total
  SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END) as impressions_utm_medium_null_sum,
  ROUND(safe_divide(SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END),SUM(impressions)) * 100, 
    2) as percent_impressions_utm_medium_null

FROM looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table
WHERE 
  date >= DATE '2025-01-01' and
  placement LIKE '%003%' and
  utm_source is null
group by 1,2,3,4;
--- Placement

SELECT 
  placement as dim,
  creative_name,
  cleaned_creative_name,
  del_key,
  MAX(date),
  SUM(impressions) as total_impressions,
  -- Count of unique placements where utm_medium is null
  COUNT(DISTINCT CASE WHEN utm_medium IS NULL THEN placement END) as unique_placements_utm_medium_null,
  -- Count of unique placements (total)
  COUNT(DISTINCT placement) as unique_placements_total,
  -- Count of unique del_ids
  COUNT(DISTINCT del_key) as unique_del_ids,
  -- Sum of impressions where utm_medium is null for unique del_ids
  SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END) as impressions_utm_medium_null,
  -- Sum of impressions where utm_medium is null and the percent of total
  SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END) as impressions_utm_medium_null_sum,
  ROUND(safe_divide(SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END),SUM(impressions)) * 100, 
    2) as percent_impressions_utm_medium_null

FROM looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table
WHERE 
  date >= DATE '2025-01-01' and
  placement LIKE '%003%' and
  utm_source is null
group by 1,2,3,4;
------------------------------------------------

SELECT 

  
  placement_name,
  
  cleaned_creative_name,
  del_key,
  MAX(date),
  SUM(impressions) as total_impressions,
  
  -- Count of unique placements where utm_medium is null
  COUNT(DISTINCT CASE WHEN utm_medium IS NULL THEN placement_name END) as unique_placements_utm_medium_null,
  
  -- Count of unique placements (total)
  COUNT(DISTINCT placement_name) as unique_placements_total,
  
--   -- Count of unique del_ids
--   COUNT(DISTINCT del_key) as unique_del_ids,
  
  -- Sum of impressions where utm_medium is null for unique del_ids
  SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END) as impressions_utm_medium_null,
  
  -- Sum of impressions where utm_medium is null and the percent of total
  SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END) as impressions_utm_medium_null_sum,
  ROUND(
    (SUM(CASE WHEN utm_medium IS NULL THEN impressions ELSE 0 END) / SUM(impressions)) * 100, 
    2
  ) as percent_impressions_utm_medium_null

FROM looker-studio-pro-452620.repo_mart.mft_clean_view
WHERE 
  date >= DATE '2025-01-01' and
  placement_name LIKE '%003%' and
  utm_source is null
group by 1,2,3,4;



  select * FROM  `looker-studio-pro-452620.repo_stg.basis_utms_stg_view_2507` 

  SELECT 
    date, 
    campaign, 
    placement, 
    impressions, 
    clicks, 
    utm_source, 
    utm_medium, 
    utm_campaign
  FROM looker-studio-pro-452620.repo_stg.basis_plus_utms_table
  LIMIT 100;  -- Adjust the limit as needed
  