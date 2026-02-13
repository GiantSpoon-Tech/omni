-- @title: Bootstrap CM360 landing->staging (create empty partitioned table)
-- @owner: Gene
-- @last_updated: 2025-11-05

DECLARE project_id         STRING DEFAULT "looker-studio-pro-452620";
DECLARE src_dataset        STRING DEFAULT "ALL_DCM_adswerve";           -- where CM360 drops tables
DECLARE table_prefix       STRING DEFAULT "DCMtoBigquery_Last14D_";     -- your report's prefix
DECLARE target_dataset     STRING DEFAULT "repo_stg";
DECLARE target_table       STRING DEFAULT "dcm_v2_raw_combined";

-- 1) Find the newest source table to infer columns.
DECLARE sample_table STRING;
SET sample_table = (
  SELECT table_name
  FROM  `${project_id}.${src_dataset}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name LIKE CONCAT(table_prefix, '%')
  ORDER BY creation_time DESC
  LIMIT 1
);

-- 2) Create the partitioned target table with your enforced schema.
--    EDIT the SELECT list below to match your CM360 report’s column names!
EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS `%s.%s.%s`
PARTITION BY date
CLUSTER BY placement_id, campaign
AS
SELECT
  SAFE_CAST(Date                   AS DATE)     AS date,
  SAFE_CAST(Advertiser             AS STRING)   AS advertiser,
  SAFE_CAST(Campaign               AS STRING)   AS campaign,
  SAFE_CAST(Package_ID             AS STRING)   AS package_id,
  SAFE_CAST(Placement_ID           AS STRING)   AS placement_id,
  SAFE_CAST(Placement              AS STRING)   AS placement,
  SAFE_CAST(Creative               AS STRING)   AS creative,
  SAFE_CAST(Impressions            AS INT64)    AS impressions,
  SAFE_CAST(Clicks                 AS INT64)    AS clicks,
  SAFE_CAST(Media_Cost             AS FLOAT64)  AS media_cost,
  
  -- add any other fields you use downstream, each with SAFE_CAST(...)
  TIMESTAMP '1970-01-01 00:00:00 UTC'           AS load_ts,
  ''                                           AS source_table
FROM `%s.%s`
WHERE 1=0
""", project_id, target_dataset, target_table, project_id, src_dataset || "." || sample_table);