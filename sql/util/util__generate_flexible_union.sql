-- dynamically generates a UNION ALL SQL query that includes all columns from both tables, filling in missing columns with NULL.

-- @file: util__generate_flexible_union.sql
-- @layer: utilities
-- @description: Dynamically generates a UNION ALL query between two tables with partially overlapping schemas.
--               Inserts NULLs for missing fields and preserves full column structure. Useful for aligning
--               cross-channel delivery tables (e.g., DCM vs Basis).
-- @inputs:
--   - dataset_a, table_a: First table
--   - dataset_b, table_b: Second table
-- @output: Returns SQL string for the UNION query. Optionally executes it.

BEGIN

-- Step 1: Declare table inputs
DECLARE dataset_a STRING DEFAULT 'looker-studio-pro-452620.repo_tables';
DECLARE table_a STRING DEFAULT 'dcm';

DECLARE dataset_b STRING DEFAULT 'looker-studio-pro-452620.repo_tables';
DECLARE table_b STRING DEFAULT 'basis';

-- Step 2: Declare result variables
DECLARE select_a STRING;
DECLARE select_b STRING;
DECLARE full_union_sql STRING;

-- Step 3: Build temp tables of column names from each source
EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE TEMP TABLE temp_columns_a AS
  SELECT column_name
  FROM `%s.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '%s'
""", dataset_a, table_a);

EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE TEMP TABLE temp_columns_b AS
  SELECT column_name
  FROM `%s.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '%s'
""", dataset_b, table_b);

-- Step 4: Compare columns
EXECUTE IMMEDIATE """
  CREATE OR REPLACE TEMP TABLE column_comparison AS
  WITH
    all_columns AS (
      SELECT column_name FROM temp_columns_a
      UNION ALL
      SELECT column_name FROM temp_columns_b
    ),
    distinct_columns AS (
      SELECT DISTINCT column_name FROM all_columns
    )
  SELECT
    dc.column_name,
    CASE
      WHEN ca.column_name IS NOT NULL AND cb.column_name IS NOT NULL THEN 'common'
      WHEN ca.column_name IS NOT NULL THEN 'only_in_a'
      WHEN cb.column_name IS NOT NULL THEN 'only_in_b'
    END AS column_location
  FROM distinct_columns dc
  LEFT JOIN temp_columns_a ca ON dc.column_name = ca.column_name
  LEFT JOIN temp_columns_b cb ON dc.column_name = cb.column_name
""";

-- Step 5: Create SELECT clause for each table
SET select_a = (
  SELECT STRING_AGG(
    CASE
      WHEN column_location = 'only_in_b' THEN 'NULL AS `' || column_name || '`'
      ELSE '`' || column_name || '`'
    END
  )
  FROM column_comparison
);

SET select_b = (
  SELECT STRING_AGG(
    CASE
      WHEN column_location = 'only_in_a' THEN 'NULL AS `' || column_name || '`'
      ELSE '`' || column_name || '`'
    END
  )
  FROM column_comparison
);

-- Step 6: Create full UNION SQL
SET full_union_sql = FORMAT("""
  SELECT %s FROM `%s.%s`
  UNION ALL
  SELECT %s FROM `%s.%s`
""", select_a, dataset_a, table_a, select_b, dataset_b, table_b);

-- Step 7: Output the final query
SELECT full_union_sql AS generated_sql;

-- Optional: actually run it
-- EXECUTE IMMEDIATE full_union_sql;

END;