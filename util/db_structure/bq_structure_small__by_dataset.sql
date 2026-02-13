/*──────────────────────────────────────────────────────────────────────────────
📜 Script Name: Multi-Dataset Column Non-Null Extractor
📅 Last Updated: 2025-08-15
👤 Author: [gene]

📌 Description:
  Scans multiple BigQuery datasets to identify only those columns that contain 
  at least one non-NULL value. The output is stored in a tall table format:
    - column_name
    - full_table_name
    - ordinal_position

  This helps reduce noise from all-NULL fields and makes downstream schema 
  processing faster and smaller.

⚙️ How It Works:
  1. Loops over datasets listed in `src_datasets`.
  2. For each table, builds a `COUNTIF(IS NOT NULL)` list for all columns.
  3. Scans each table **once** (per table) to find non-NULL columns.
  4. Inserts results into a staging temp table `_nonnull_structure`.
  5. Writes results into `tgt_project.tgt_dataset.tgt_table` with append or 
     overwrite mode controlled by `overwrite` flag.

📝 Parameters:
  - src_project:   Project containing the datasets to scan.
  - src_datasets:  Array of dataset IDs to process.
  - include_views: Whether to include views in the scan (TRUE/FALSE).
  - tgt_project:   Project for storing results.
  - tgt_dataset:   Dataset for storing results.
  - tgt_table:     Name of the tall structure table to write to.
  - overwrite:     FALSE (default) appends, TRUE overwrites the table.

💡 Notes:
  - Efficient: Uses one scan per table, not per column.
  - `full_table_name` is stored in the form project.dataset.table.
  - Works across multiple datasets in the same project.
  - Ensure target table schema matches (STRING, STRING, INT64).

──────────────────────────────────────────────────────────────────────────────
*/
-- =======================
-- CONFIG
-- =======================
DECLARE src_project   STRING DEFAULT 'looker-studio-pro-452620';
DECLARE src_datasets  ARRAY<STRING> DEFAULT ['repo_google_ads','repo_tiktok','repo_linkedin'];  -- 👈 add more here
DECLARE include_views BOOL   DEFAULT TRUE;     -- set FALSE to skip views
DECLARE tgt_project   STRING DEFAULT 'looker-studio-pro-452620';
DECLARE tgt_dataset   STRING DEFAULT 'repo_util';
DECLARE tgt_table     STRING DEFAULT 'db_structure_small';
DECLARE overwrite     BOOL   DEFAULT FALSE;    -- 🔄 FALSE = append (default), TRUE = overwrite

-- =======================
-- Working vars
-- =======================
DECLARE dataset_id STRING;
DECLARE table_name STRING;
DECLARE select_counts_list STRING;   -- "COUNTIF(`a` IS NOT NULL) AS `a`, ..."
DECLARE unpivot_list STRING;         -- "`a`, `b`, ..."

-- Ensure target table exists in append mode
IF NOT overwrite THEN
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS `%s.%s.%s` (
      column_name STRING,
      full_table_name STRING,
      ordinal_position INT64
    )
  """, tgt_project, tgt_dataset, tgt_table);
END IF;

-- Temp accumulator for this run
CREATE OR REPLACE TEMP TABLE _nonnull_structure (
  column_name STRING,
  full_table_name STRING,
  ordinal_position INT64
);

-- =======================
-- Loop datasets
-- =======================
FOR ds IN (SELECT dataset AS dataset_id FROM UNNEST(src_datasets) AS dataset) DO
  SET dataset_id = ds.dataset_id;

  -- Build table list for this dataset
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE _tables AS
    SELECT table_name
    FROM `%s.%s.INFORMATION_SCHEMA.TABLES`
    WHERE table_type IN ('BASE TABLE'%s)
    ORDER BY table_name
  """,
    src_project,
    dataset_id,
    IF(include_views, ", 'VIEW'", "")
  );

  -- Loop tables in the dataset (one scan per table)
  FOR t IN (SELECT table_name FROM _tables ORDER BY table_name) DO
    SET table_name = t.table_name;

    -- Build column list for this table
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TEMP TABLE _cols AS
      SELECT column_name, ordinal_position
      FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = @tbl
      ORDER BY ordinal_position
    """, src_project, dataset_id)
    USING table_name AS tbl;

    -- COUNTIF list (all columns at once)
    SET select_counts_list = (
      SELECT STRING_AGG(FORMAT("COUNTIF(`%s` IS NOT NULL) AS `%s`", column_name, column_name), ', ')
      FROM _cols
    );

    -- UNPIVOT list
    SET unpivot_list = (
      SELECT STRING_AGG(FORMAT("`%s`", column_name), ', ')
      FROM _cols
    );

    -- Skip if no columns (defensive)
    IF select_counts_list IS NULL OR unpivot_list IS NULL THEN
      CONTINUE;
    END IF;

    -- One-pass scan + UNPIVOT for this table
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
      src_project, dataset_id, table_name,
      unpivot_list,
      src_project, dataset_id
    )
    USING FORMAT('%s.%s.%s', src_project, dataset_id, table_name) AS full_name,
          table_name AS tbl;

    INSERT INTO _nonnull_structure
    SELECT column_name, full_table_name, ordinal_position
    FROM _nonnull_for_tbl;
  END FOR; -- tables
END FOR;   -- datasets

-- =======================
-- Write to target table
-- =======================
IF overwrite THEN
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `%s.%s.%s` AS
    SELECT column_name, full_table_name, ordinal_position
    FROM _nonnull_structure
  """, tgt_project, tgt_dataset, tgt_table);
ELSE
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `%s.%s.%s` (column_name, full_table_name, ordinal_position)
    SELECT column_name, full_table_name, ordinal_position
    FROM _nonnull_structure
  """, tgt_project, tgt_dataset, tgt_table);
END IF;

-- INSERT INTO `looker-studio-pro-452620.repo_util.db_structure_small`
-- (column_name, full_table_name, ordinal_position)
-- SELECT column_name, full_table_name, ordinal_position
-- FROM looker-studio-pro-452620._scriptcf54169f08a26e07f0cebf1d0eca8191f35f46b4._nonnull_structure;

--