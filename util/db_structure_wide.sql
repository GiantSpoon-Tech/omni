-- Wide view of fields per table, built from the tall structure table
-- Source: looker-studio-pro-452620.repo_util.db_structure

-- Wide view of fields per table from the tall structure table
-- Source: looker-studio-pro-452620.repo_util.db_structure
create or replace view `looker-studio-pro-452620.repo_util.db_structure_wide` as

  WITH headers AS (
    -- Use dataset.table as headers
    SELECT DISTINCT
      REGEXP_EXTRACT(full_table_name, r'^[^.]+\\.([^.]+\\.[^.]+)$') AS header_name
    FROM `looker-studio-pro-452620.repo_util.db_structure`
  ),
  sql AS (
    SELECT FORMAT("""
      WITH base AS (
        SELECT
          ordinal_position AS row_idx,
          column_name,
          REGEXP_EXTRACT(full_table_name, r'^[^.]+\\.([^.]+\\.[^.]+)$') AS header_name
        FROM `looker-studio-pro-452620.repo_util.db_structure`
      )
      SELECT *
      FROM (
        SELECT row_idx, header_name, column_name
        FROM base
      )
      PIVOT (MAX(column_name) FOR header_name IN (%s))
      ORDER BY row_idx
    """,
      (SELECT STRING_AGG(FORMAT("`%s`", header_name) ORDER BY header_name) FROM headers)
    ) AS stmt
  )
  SELECT stmt FROM sql
;