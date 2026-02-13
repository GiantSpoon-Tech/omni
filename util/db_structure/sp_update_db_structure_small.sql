/*──────────────────────────────────────────────────────────────────────────────
📜 Proc 1: sp_update_db_structure_small
📅 Last Updated: 2025-08-15
👤 Author: [Your Name]

📌 What it does
  For each provided table, computes which columns have at least one non-NULL
  value using a single full scan per table, then writes results to the tall
  table (repo_util.db_structure_small). Existing rows for each table are replaced.

✅ Avoids prior errors:
  - Dynamic identifiers via FORMAT()+EXECUTE IMMEDIATE (no var interpolation in `)
  - Regex dots escaped as `\\.` (prevents "Illegal escape sequence: \.")
  - No SELECT ... INTO (uses SET = (SELECT ...), or EXECUTE ... INTO)
  - CREATE OR REPLACE TEMP TABLE (no DROP needed)
──────────────────────────────────────────────────────────────────────────────*/
CREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_util.sp_update_db_structure_small`(
  src_project STRING,                -- default project for shorthand names (dataset.table)
  table_names ARRAY<STRING>,         -- list of tables to refresh
  tgt_project STRING,                -- may be NULL → defaulted in body
  tgt_dataset STRING,                -- may be NULL → defaulted in body
  tgt_table   STRING                 -- may be NULL → defaulted in body
)
BEGIN
  -- Apply defaults inside the proc (BigQuery procs don't support DEFAULT in signature)
  DECLARE _tgt_project STRING DEFAULT COALESCE(tgt_project, 'looker-studio-pro-452620');
  DECLARE _tgt_dataset STRING DEFAULT COALESCE(tgt_dataset, 'repo_util');
  DECLARE _tgt_table   STRING DEFAULT COALESCE(tgt_table,   'db_structure_small');

  DECLARE tbl_in STRING;              
  DECLARE p STRING;                   
  DECLARE d STRING;                   
  DECLARE t STRING;                   
  DECLARE full_name STRING;           
  DECLARE select_counts_list STRING;
  DECLARE unpivot_list STRING;

  -- Ensure target table exists
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS `%s.%s.%s` (
      column_name STRING,
      full_table_name STRING,
      ordinal_position INT64
    )
  """, _tgt_project, _tgt_dataset, _tgt_table);

  -- Temp for a single table’s results
  CREATE OR REPLACE TEMP TABLE _nonnull_for_tbl (
    column_name STRING,
    full_table_name STRING,
    ordinal_position INT64
  );

  -- Process each requested table
  FOR rec IN (SELECT tbl AS tbl_in FROM UNNEST(table_names) AS tbl) DO
    SET tbl_in = rec.tbl_in;

    -- Parse input name: full (p.d.t) or shorthand (d.t)
    IF REGEXP_CONTAINS(tbl_in, r'^([^.]+)\\.([^.]+)\\.([^.]+)$') THEN
      SET p = REGEXP_EXTRACT(tbl_in, r'^([^.]+)\\.');
      SET d = REGEXP_EXTRACT(tbl_in, r'^[^.]+\\.([^.]+)\\.');
      SET t = REGEXP_EXTRACT(tbl_in, r'^[^.]+\\.[^.]+\\.([^.]+)$');
    ELSeIF REGEXP_CONTAINS(tbl_in, r'^([^.]+)\\.([^.]+)$') THEN
      SET p = src_project;
      SET d = REGEXP_EXTRACT(tbl_in, r'^([^.]+)\\.');
      SET t = REGEXP_EXTRACT(tbl_in, r'^[^.]+\\.([^.]+)$');
    ELSE
      CONTINUE;  -- invalid format
    END IF;

    SET full_name = FORMAT('%s.%s.%s', p, d, t);

    -- Columns (ordered)
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TEMP TABLE _cols AS
      SELECT column_name, ordinal_position
      FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = @tbl
      ORDER BY ordinal_position
    """, p, d)
    USING t AS tbl;

    IF (SELECT COUNT(*) FROM _cols) = 0 THEN
      CONTINUE;
    END IF;

    -- Build COUNTIF and UNPIVOT lists
    SET select_counts_list = (
      SELECT STRING_AGG(FORMAT("COUNTIF(`%s` IS NOT NULL) AS `%s`", column_name, column_name), ', ')
      FROM _cols
    );
    SET unpivot_list = (
      SELECT STRING_AGG(FORMAT("`%s`", column_name), ', ')
      FROM _cols
    );

    IF select_counts_list IS NULL OR unpivot_list IS NULL THEN
      CONTINUE;
    END IF;

    -- Compute non-null columns with one scan
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
      p, d, t,
      unpivot_list,
      p, d
    )
    USING full_name AS full_name,
          t AS tbl;

    -- Replace prior rows for this table, then insert fresh rows
    EXECUTE IMMEDIATE FORMAT("""
      DELETE FROM `%s.%s.%s` WHERE full_table_name = @full_name
    """, _tgt_project, _tgt_dataset, _tgt_table)
    USING full_name AS full_name;

    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `%s.%s.%s` (column_name, full_table_name, ordinal_position)
      SELECT column_name, full_table_name, ordinal_position
      FROM _nonnull_for_tbl
    """, _tgt_project, _tgt_dataset, _tgt_table);
  END FOR;
END;