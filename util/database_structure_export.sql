-- Multi-Project BigQuery Database Structure Export Script
-- This script queries INFORMATION_SCHEMA across multiple projects and unions the results
-- Run this periodically to keep your database documentation up to date

-- Configuration for multi-project setup:
-- Project 1: looker-studio-pro-452620 (include all tables)
-- Project 2: giant-spoon-299605 (only starred tables)

-- Get all tables from looker-studio-pro-452620
create or replace view looker-studio-pro-452620.repo_util.db_structure_export as
WITH looker_studio_tables AS (
  SELECT 
    table_catalog as project_id,
    table_schema as dataset_id,
    table_name,
    table_type,
    creation_time,
    --last_modified_time,
    row_count,
    size_bytes,
    description as table_description,
    CONCAT(table_catalog, '.', table_schema, '.', table_name) as full_table_name,
    'all_tables' as inclusion_reason
  FROM `looker-studio-pro-452620.region-us.INFORMATION_SCHEMA.TABLES`
  WHERE 
    table_catalog = 'looker-studio-pro-452620'
    AND NOT STARTS_WITH(table_schema, '_')  -- Exclude system datasets
    AND table_type IN ('BASE TABLE', 'VIEW')
),

-- Get only starred tables from giant-spoon-299605
giant_spoon_starred_tables AS (
  SELECT 
    table_catalog as project_id,
    table_schema as dataset_id,
    table_name,
    table_type,
    creation_time,
    last_modified_time,
    row_count,
    size_bytes,
    description as table_description,
    CONCAT(table_catalog, '.', table_schema, '.', table_name) as full_table_name,
    'starred_only' as inclusion_reason
  FROM `giant-spoon-299605.region-us.INFORMATION_SCHEMA.TABLES` t
  WHERE 
    table_catalog = 'giant-spoon-299605'
    AND NOT STARTS_WITH(table_schema, '_')  -- Exclude system datasets
    AND table_type IN ('BASE TABLE', 'VIEW')
    -- Filter for starred tables only (tables with labels containing 'starred')
    AND EXISTS (
      SELECT 1 
      FROM `giant-spoon-299605.region-us.INFORMATION_SCHEMA.TABLE_OPTIONS` o
      WHERE o.table_catalog = t.table_catalog
        AND o.table_schema = t.table_schema
        AND o.table_name = t.table_name
        AND o.option_name = 'labels'
        AND LOWER(o.option_value) LIKE '%starred%'
    )
),

-- Union all tables from both projects
all_tables AS (
  SELECT * FROM looker_studio_tables
  UNION ALL
  SELECT * FROM giant_spoon_starred_tables
),

-- Get column information for all selected tables
looker_studio_columns AS (
  SELECT 
    table_catalog as project_id,
    table_schema as dataset_id,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    is_nullable,
    column_default,
    is_generated,
    generation_expression,
    is_stored,
    is_partitioning_column,
    clustering_ordinal_position,
    description as column_description
  FROM `looker-studio-pro-452620.region-us.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_catalog = 'looker-studio-pro-452620'
),

giant_spoon_columns AS (
  SELECT 
    table_catalog as project_id,
    table_schema as dataset_id,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    is_nullable,
    column_default,
    is_generated,
    generation_expression,
    is_stored,
    is_partitioning_column,
    clustering_ordinal_position,
    description as column_description
  FROM `giant-spoon-299605.region-us.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_catalog = 'giant-spoon-299605'
),

-- Union all column information
all_columns AS (
  SELECT * FROM looker_studio_columns
  UNION ALL
  SELECT * FROM giant_spoon_columns
),

-- Combine table and column information
complete_structure AS (
  SELECT 
    t.project_id,
    t.dataset_id,
    t.table_name,
    t.full_table_name,
    t.table_type,
    t.creation_time,
    t.last_modified_time,
    t.row_count,
    t.size_bytes,
    t.table_description,
    t.inclusion_reason,
    c.column_name,
    c.ordinal_position,
    c.data_type,
    c.is_nullable,
    c.column_default,
    c.is_generated,
    c.generation_expression,
    c.is_stored,
    c.is_partitioning_column,
    c.clustering_ordinal_position,
    c.column_description,
    -- Add flags for tables containing specific keywords
    --CASE WHEN LOWER(t.table_name) LIKE '%campaign%' THEN TRUE ELSE FALSE END as has_campaign,
    CASE WHEN LOWER(t.table_name) LIKE '%history%' THEN TRUE ELSE FALSE END as has_history,
    --CASE WHEN LOWER(t.table_name) LIKE '%campaign%' AND LOWER(t.table_name) LIKE '%history%' THEN TRUE ELSE FALSE END as is_campaign_history,
    CASE WHEN LOWER(t.table_name) LIKE '%staging%' OR LOWER(t.table_name) LIKE '%stg%' THEN TRUE ELSE FALSE END as is_staging,
    CASE WHEN LOWER(t.table_name) LIKE '%mart%' OR LOWER(t.table_name) LIKE '%dim%' OR LOWER(t.table_name) LIKE '%fact%' THEN TRUE ELSE FALSE END as is_mart
  FROM all_tables t
  LEFT JOIN all_columns c
    ON t.project_id = c.project_id
    AND t.dataset_id = c.dataset_id
    AND t.table_name = c.table_name
)

-- Final output with all database structure information from both projects
SELECT 
  project_id,
  dataset_id,
  table_name,
  full_table_name,
  table_type,
  inclusion_reason,
  creation_time,
  last_modified_time,
  row_count,
  size_bytes,
  table_description,
  column_name,
  ordinal_position,
  data_type,
  is_nullable,
  column_default,
  is_generated,
  generation_expression,
  is_stored,
  is_partitioning_column,
  clustering_ordinal_position,
  column_description,
  has_campaign,
  has_history,
  is_campaign_history,
  is_staging,
  is_mart,
  -- Add current export timestamp
  CURRENT_TIMESTAMP() as export_timestamp
FROM complete_structure
ORDER BY 
  project_id,
  dataset_id,
  table_name,
  ordinal_position;
