WITH a AS (
  SELECT 
    creative_name,
    placement,
    cleaned_creative_name,
    del_key,
    min(date) as min_date,
    MAX(date) as max_date,
    SUM(impressions) as total_impressions
  FROM looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table
  WHERE 
    date >= DATE '2025-01-01'
    AND placement LIKE '%003%'
    AND utm_source IS NULL
  GROUP BY 1,2,3,4
)

SELECT distinct creative_name
-- FROM `looker-studio-pro-452620.repo_stg.basis_utms_stg_view_2507` 
from  looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table

select