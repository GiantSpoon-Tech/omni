-- v1
  /*──────────────────────────────────────────────────────────────────────────────
  📜 Script Name: Project Table & Column Metadata Export
  📅 Last Updated: 2025-08-15
  👤 Author: [Your Name]

  📌 Description:
    This script retrieves metadata for specific BigQuery projects, combining:
      1. Tables from `looker-studio-pro-452620` (all base tables & views)
      2. Starred tables from `giant-spoon-299605`
      3. Column definitions for all selected tables

    It outputs:
      - Table-level details (project, dataset, type, creation time, inclusion reason)
      - Column-level details (name, position, type, nullability, partition/clustering)
      - Flags for history, staging, and mart tables

  💡 Notes:
    - `last_modified_time` is not available in INFORMATION_SCHEMA.TABLES; set to NULL.
    - Starred table detection is based on TABLE_OPTIONS labels containing 'starred'.
    - Useful for auditing schemas, building lineage tools, and tracking schema changes.

  ──────────────────────────────────────────────────────────────────────────────*/

  WITH
  --──────────────────────────────────────────────────────────────────────────────
  -- 1️⃣ Retrieve all base tables & views from looker-studio-pro-452620
  --──────────────────────────────────────────────────────────────────────────────
  looker_studio_tables AS (
    SELECT
      table_catalog AS project_id,
      table_schema AS dataset_id,
      table_name,
      table_type,
      creation_time,
      CAST(NULL AS TIMESTAMP) AS last_modified_time, -- Not available in INFORMATION_SCHEMA
      CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_table_name,
      'all_tables' AS inclusion_reason,
      FALSE AS starred_flag
    FROM
      `looker-studio-pro-452620.region-us.INFORMATION_SCHEMA.TABLES`
    WHERE
      table_catalog = 'looker-studio-pro-452620'
      AND NOT STARTS_WITH(table_schema, '_') -- Exclude system datasets
      AND table_type IN ('BASE TABLE', 'VIEW')
  ),

  --──────────────────────────────────────────────────────────────────────────────
  -- 2️⃣ Retrieve only starred tables from giant-spoon-299605
  --    (Tables where TABLE_OPTIONS.labels contains 'starred')
  --──────────────────────────────────────────────────────────────────────────────
  giant_spoon_starred_tables AS (
    SELECT
      table_catalog AS project_id,
      table_schema AS dataset_id,
      table_name,
      table_type,
      creation_time,
      CAST(NULL AS TIMESTAMP) AS last_modified_time,
      CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_table_name,
      'starred_only' AS inclusion_reason,
      TRUE AS starred_flag
    FROM
      `giant-spoon-299605.region-us.INFORMATION_SCHEMA.TABLES` AS t
    WHERE
      table_catalog = 'giant-spoon-299605'
      AND NOT STARTS_WITH(table_schema, '_')
      AND table_type IN ('BASE TABLE', 'VIEW')
      AND EXISTS (
        SELECT 1
        FROM `giant-spoon-299605.region-us.INFORMATION_SCHEMA.TABLE_OPTIONS` AS o
        WHERE o.table_catalog = t.table_catalog
          AND o.table_schema = t.table_schema
          AND o.table_name = t.table_name
          AND o.option_name = 'labels'
          AND LOWER(o.option_value) LIKE '%starred%'
      )
  ),

  --──────────────────────────────────────────────────────────────────────────────
  -- 3️⃣ Combine table lists from both projects
  --──────────────────────────────────────────────────────────────────────────────
  all_tables AS (
    SELECT * FROM looker_studio_tables
    UNION ALL
    SELECT * FROM giant_spoon_starred_tables
  ),

  --──────────────────────────────────────────────────────────────────────────────
  -- 4️⃣ Get column metadata for looker-studio-pro-452620
  --──────────────────────────────────────────────────────────────────────────────
  looker_studio_columns AS (
    SELECT
      table_catalog AS project_id,
      table_schema AS dataset_id,
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
      clustering_ordinal_position
    FROM
      `looker-studio-pro-452620.region-us.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      table_catalog = 'looker-studio-pro-452620'
  ),

  --──────────────────────────────────────────────────────────────────────────────
  -- 5️⃣ Get column metadata for giant-spoon-299605
  --──────────────────────────────────────────────────────────────────────────────
  giant_spoon_columns AS (
    SELECT
      table_catalog AS project_id,
      table_schema AS dataset_id,
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
      clustering_ordinal_position
    FROM
      `giant-spoon-299605.region-us.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      table_catalog = 'giant-spoon-299605'
  ),

  --──────────────────────────────────────────────────────────────────────────────
  -- 6️⃣ Combine column lists from both projects
  --──────────────────────────────────────────────────────────────────────────────
  all_columns AS (
    SELECT * FROM looker_studio_columns
    UNION ALL
    SELECT * FROM giant_spoon_columns
  ),

  --──────────────────────────────────────────────────────────────────────────────
  -- 7️⃣ Join tables & columns, and apply classification flags
  --──────────────────────────────────────────────────────────────────────────────
  complete_structure AS (
    SELECT
      t.project_id,
      t.dataset_id,
      t.table_name,
      t.full_table_name,
      t.table_type,
      t.creation_time,
      t.last_modified_time,
      t.inclusion_reason,
      t.starred_flag,
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
      CASE WHEN LOWER(t.table_name) LIKE '%history%' THEN TRUE ELSE FALSE END AS has_history,
      CASE WHEN LOWER(t.table_name) LIKE '%staging%' OR LOWER(t.table_name) LIKE '%stg%' THEN TRUE ELSE FALSE END AS is_staging,
      CASE WHEN LOWER(t.table_name) LIKE '%mart%' OR LOWER(t.table_name) LIKE '%dim%' OR LOWER(t.table_name) LIKE '%fact%' THEN TRUE ELSE FALSE END AS is_mart
    FROM
      all_tables AS t
    LEFT JOIN
      all_columns AS c
    ON
      t.project_id = c.project_id
      AND t.dataset_id = c.dataset_id
      AND t.table_name = c.table_name
  )

  --──────────────────────────────────────────────────────────────────────────────
  -- 8️⃣ Final output
  --──────────────────────────────────────────────────────────────────────────────
  SELECT
    project_id,
    dataset_id,
    table_name,
    full_table_name,
    table_type,
    inclusion_reason,
    starred_flag,
    creation_time,
    last_modified_time,
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
    has_history,
    is_staging,
    is_mart,
    CURRENT_TIMESTAMP() AS export_timestamp
  FROM
    complete_structure
  ORDER BY
    project_id,
    dataset_id,
    table_name,
    ordinal_position;
--
-- v2
-- =======================
-- CONFIG
-- =======================
DECLARE src_project STRING DEFAULT 'looker-studio-pro-452620';
DECLARE src_dataset STRING DEFAULT 'repo_google_ads';
DECLARE gcs_uri     STRING DEFAULT 'gs://YOUR_BUCKET/exports/colAndTableNames_nonnull.csv';
DECLARE include_views BOOL  DEFAULT TRUE;  -- set FALSE to skip views

-- =======================
-- Working vars
-- =======================
DECLARE table_name STRING;
DECLARE select_counts_list STRING;   -- e.g., "COUNTIF(`a` IS NOT NULL) AS `a`, COUNTIF(`b` IS NOT NULL) AS `b`"
DECLARE unpivot_list STRING;         -- e.g., "`a`, `b`"

-- Final result (temp)
CREATE TEMP TABLE _nonnull_structure (
  column_name STRING,
  full_table_name STRING,
  ordinal_position INT64
);

-- Build table list (temp) via dynamic CTAS
EXECUTE IMMEDIATE FORMAT("""
  CREATE TEMP TABLE _tables AS
  SELECT table_name
  FROM `%s.%s.INFORMATION_SCHEMA.TABLES`
  WHERE table_type IN ('BASE TABLE'%s)
  ORDER BY table_name
""",
  src_project,
  src_dataset,
  IF(include_views, ", 'VIEW'", "")
);

-- Loop tables (one scan per table)
FOR t IN (SELECT table_name FROM _tables ORDER BY table_name) DO
  SET table_name = t.table_name;

  -- Build dynamic COUNTIF list and UNPIVOT list for this table
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE _cols AS
    SELECT column_name, ordinal_position
    FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @tbl
    ORDER BY ordinal_position
  """, src_project, src_dataset)
  USING table_name AS tbl;

  -- Build: COUNTIF expressions
  SELECT STRING_AGG(FORMAT("COUNTIF(`%s` IS NOT NULL) AS `%s`", column_name, column_name), ', ')
  INTO select_counts_list
  FROM _cols;

  -- Build: UNPIVOT list
  SELECT STRING_AGG(FORMAT("`%s`", column_name), ', ')
  INTO unpivot_list
  FROM _cols;

  -- Skip empty schema (defensive)
  IF select_counts_list IS NULL OR unpivot_list IS NULL THEN
    CONTINUE;
  END IF;

  -- One-pass scan of the table, then UNPIVOT and keep only columns with nn > 0
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE _nonnull_for_tbl AS
    WITH counts AS (
      SELECT %s
      FROM `%s.%s.%s`
    )
    SELECT u.column_name,
           @full_name AS full_table_name,
           c.ordinal_position
    FROM counts
    UNPIVOT (nn FOR column_name IN (%s)) AS u
    JOIN (
      SELECT column_name, ordinal_position
      FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = @tbl
    ) AS c
    USING (column_name)
    WHERE nn > 0
  """,
    select_counts_list,
    src_project, src_dataset, table_name,
    unpivot_list,
    src_project, src_dataset
  )
  USING FORMAT('%s.%s.%s', src_project, src_dataset, table_name) AS full_name,
        table_name AS tbl;

  INSERT INTO _nonnull_structure
  SELECT column_name, full_table_name, ordinal_position
  FROM _nonnull_for_tbl;
END FOR;

-- (Optional) Export CSV
-- EXPORT DATA OPTIONS (
--   uri       = gcs_uri,
--   format    = 'CSV',
--   overwrite = TRUE,
--   header    = TRUE
-- ) AS
-- SELECT column_name, full_table_name, ordinal_position
-- FROM _nonnull_structure
-- ORDER BY full_table_name, ordinal_position;