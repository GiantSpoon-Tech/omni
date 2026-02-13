-- v5 
CREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_util.dedupe_table_by_primary_id_v3` (
  IN source_project STRING,
  IN source_schema  STRING,
  IN source_table   STRING,
  IN target_project STRING,
  IN target_schema  STRING,
  IN primary_key_override STRING  -- optional: e.g., 'ad_id'
)
BEGIN
  -- =========================
  -- Declarations (top only)
  -- =========================
  DECLARE source_full STRING DEFAULT NULL;
  DECLARE target_full STRING DEFAULT NULL;

  DECLARE id_field STRING DEFAULT NULL;
  DECLARE tmp_id STRING DEFAULT NULL;

  DECLARE candidates ARRAY<STRING>;
  DECLARE candidate_query STRING DEFAULT NULL;

  DECLARE override_exists BOOL DEFAULT FALSE;
  DECLARE exists_col BOOL DEFAULT FALSE;
  DECLARE final_exists BOOL DEFAULT FALSE;

  DECLARE row_cnt INT64 DEFAULT 0;
  DECLARE approx_distinct INT64 DEFAULT NULL;

  DECLARE update_cols ARRAY<STRING>;
  DECLARE sync_cols ARRAY<STRING>;
  DECLARE update_cols_query STRING DEFAULT NULL;
  DECLARE sync_cols_query STRING DEFAULT NULL;

  DECLARE has_any_update BOOL DEFAULT FALSE;
  DECLARE has_any_sync   BOOL DEFAULT FALSE;

  DECLARE order_by_clause STRING DEFAULT '';

  DECLARE audit_dup_table STRING DEFAULT NULL;
  DECLARE run_log_table STRING DEFAULT NULL;

  DECLARE rows_out INT64 DEFAULT 0;

  -- =========================
  -- Setup
  -- =========================
  SET source_full = FORMAT("%s.%s.%s", source_project, source_schema, source_table);
  SET target_full = FORMAT("%s.%s.stg__%s_deduped", target_project, target_schema, source_table);
  SET audit_dup_table = FORMAT("%s.%s.audit__dup_counts", target_project, target_schema);
  SET run_log_table   = FORMAT("%s.%s.audit__run_log",   target_project, target_schema);

  -- =========================
  -- Manual override (if provided and present)
  -- =========================
  IF primary_key_override IS NOT NULL THEN
    EXECUTE IMMEDIATE FORMAT("""
      SELECT COUNT(1) > 0
      FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '%s' AND column_name = '%s'
    """, source_project, source_schema, source_table, primary_key_override)
    INTO override_exists;

    IF override_exists THEN
      SET id_field = primary_key_override;
    END IF;
  END IF;

  -- =========================
  -- Auto-pick MOST GRANULAR id (max distinct), if no valid override
  -- =========================
  IF id_field IS NULL THEN
    SET candidate_query = FORMAT("""
      SELECT ARRAY_AGG(column_name ORDER BY column_name)
      FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '%s'
        AND (REGEXP_CONTAINS(column_name, r'_id$') OR column_name = 'id')
    """, source_project, source_schema, source_table);
    EXECUTE IMMEDIATE candidate_query INTO candidates;

    EXECUTE IMMEDIATE FORMAT("SELECT COUNT(*) FROM `%s`", source_full) INTO row_cnt;

    CREATE TEMP TABLE _id_metrics (
      col STRING,
      approx_distinct INT64
    );

    IF candidates IS NOT NULL THEN
      FOR rec IN (SELECT col FROM UNNEST(candidates) AS col) DO
        EXECUTE IMMEDIATE FORMAT("SELECT APPROX_COUNT_DISTINCT(`%s`) FROM `%s`", rec.col, source_full)
        INTO approx_distinct;

        INSERT INTO _id_metrics(col, approx_distinct)
        VALUES (rec.col, approx_distinct);
      END FOR;

      -- Choose most granular: highest approx_distinct; avoid plain 'id' on ties
      SET id_field = (
        SELECT col
        FROM _id_metrics
        WHERE approx_distinct IS NOT NULL
        ORDER BY approx_distinct DESC,
                 CASE WHEN col = 'id' THEN 1 ELSE 0 END,
                 col
        LIMIT 1
      );
    END IF;
  END IF;

  -- =========================
  -- Heuristic fallback if still NULL
  -- =========================
  IF id_field IS NULL THEN
    -- 1) first part + _id (campaign_history -> campaign_id)
    SET tmp_id = FORMAT('%s_id', SPLIT(source_table, '_')[OFFSET(0)]);
    EXECUTE IMMEDIATE FORMAT("""
      SELECT COUNT(1) > 0
      FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '%s' AND column_name = '%s'
    """, source_project, source_schema, source_table, tmp_id) INTO exists_col;

    IF exists_col THEN
      SET id_field = tmp_id;
    ELSE
      -- 2) all but last + _id (ad_group_history -> ad_group_id)
      SET tmp_id = CONCAT(
        ARRAY_TO_STRING(
          ARRAY_SLICE(SPLIT(source_table, '_'), 0, ARRAY_LENGTH(SPLIT(source_table, '_')) - 1),
          '_'
        ), '_id'
      );
      EXECUTE IMMEDIATE FORMAT("""
        SELECT COUNT(1) > 0
        FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '%s' AND column_name = '%s'
      """, source_project, source_schema, source_table, tmp_id) INTO exists_col;

      IF exists_col THEN
        SET id_field = tmp_id;
      ELSE
        -- 3) strip _history + _id (adgroup_history -> adgroup_id)
        SET tmp_id = CONCAT(REGEXP_REPLACE(source_table, r'_history$', ''), '_id');
        EXECUTE IMMEDIATE FORMAT("""
          SELECT COUNT(1) > 0
          FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
          WHERE table_name = '%s' AND column_name = '%s'
        """, source_project, source_schema, source_table, tmp_id) INTO exists_col;

        IF exists_col THEN
          SET id_field = tmp_id;
        ELSE
          -- 4) fallback 'id'
          SET id_field = 'id';
        END IF;
      END IF;
    END IF;
  END IF;

  -- =========================
  -- Final validation: chosen id exists
  -- =========================
  EXECUTE IMMEDIATE FORMAT("""
    SELECT COUNT(1) > 0
    FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '%s' AND column_name = '%s'
  """, source_project, source_schema, source_table, id_field)
  INTO final_exists;

  IF NOT final_exists THEN
    RAISE USING MESSAGE = FORMAT(
      "No usable primary key found for %s.%s.%s. Chosen '%s' not present.",
      source_project, source_schema, source_table, id_field
    );
  END IF;

  -- =========================
  -- Build ORDER BY:
  --   1) any columns whose name starts with 'update' (case-insensitive)
  --   2) else a sync column (prefer _fivetran_synced, else any col containing 'sync')
  -- =========================
  SET update_cols_query = FORMAT("""
    SELECT ARRAY_AGG(column_name)
    FROM (
      SELECT column_name
      FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '%s'
        AND REGEXP_CONTAINS(LOWER(column_name), r'^update')  -- starts with 'update'
      ORDER BY
        -- sensible priority for common names
        CASE LOWER(column_name)
          WHEN 'updated_time' THEN 1
          WHEN 'updated_at'   THEN 2
          WHEN 'update_time'  THEN 3
          WHEN 'update_datetime' THEN 4
          WHEN 'update_ts'    THEN 5
          WHEN 'update_date'  THEN 6
          WHEN 'updated_date' THEN 7
          ELSE 99
        END,
        column_name
    )
  """, source_project, source_schema, source_table);

  EXECUTE IMMEDIATE update_cols_query INTO update_cols;
  SET has_any_update = (update_cols IS NOT NULL AND ARRAY_LENGTH(update_cols) > 0);

  IF NOT has_any_update THEN
    -- collect sync-like columns
    SET sync_cols_query = FORMAT("""
      SELECT ARRAY_AGG(column_name ORDER BY
        CASE
          WHEN LOWER(column_name) = '_fivetran_synced' THEN 1
          ELSE 2
        END, column_name)
      FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '%s'
        AND (LOWER(column_name) = '_fivetran_synced'
             OR REGEXP_CONTAINS(LOWER(column_name), r'sync'))
    """, source_project, source_schema, source_table);
    EXECUTE IMMEDIATE sync_cols_query INTO sync_cols;
    SET has_any_sync = (sync_cols IS NOT NULL AND ARRAY_LENGTH(sync_cols) > 0);

    IF NOT has_any_sync THEN
      RAISE USING MESSAGE = FORMAT(
        "No 'update*' columns or sync columns found in %s.%s.%s. Cannot deduplicate reliably.",
        source_project, source_schema, source_table
      );
    END IF;
  END IF;

  -- Build order_by_clause: use all update* columns (in priority order); else single best sync col
  SET order_by_clause = '';
  IF has_any_update THEN
    -- add every update* col in order
    FOR u IN (SELECT col FROM UNNEST(update_cols) AS col) DO
      SET order_by_clause = IF(
        order_by_clause = '',
        FORMAT("TIMESTAMP(SAFE_CAST(`%s` AS DATETIME)) DESC", u.col),
        order_by_clause || ', ' || FORMAT("TIMESTAMP(SAFE_CAST(`%s` AS DATETIME)) DESC", u.col)
      );
    END FOR;
  ELSE
    -- default to the top-priority sync column only
    SET order_by_clause = FORMAT("TIMESTAMP(SAFE_CAST(`%s` AS DATETIME)) DESC", sync_cols[OFFSET(0)]);
  END IF;

  -- =========================
  -- Pre-run audit: duplicate groups in SOURCE (on chosen id)
  -- =========================
  -- EXECUTE IMMEDIATE FORMAT("""
  --   --CREATE OR REPLACE TABLE `%s` AS
  --   SELECT `%s` AS entity_id, COUNT(*) AS n
  --   FROM `%s`
  --   GROUP BY `%s`
  --   HAVING COUNT(*) > 1
  --   ORDER BY n DESC
  --   LIMIT 200
  -- """, audit_dup_table, id_field, source_full, id_field);

  -- =========================
  -- Deduplicate into TARGET
  -- =========================
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `%s` AS
    SELECT *
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY `%s`
          ORDER BY %s
        ) AS dedupe
      FROM `%s`
    )
    WHERE dedupe = 1
  """, target_full, id_field, order_by_clause, source_full);

  -- =========================
  -- Run log
  -- =========================
  EXECUTE IMMEDIATE FORMAT("SELECT COUNT(*) FROM `%s`", target_full) INTO rows_out;
  EXECUTE IMMEDIATE FORMAT("SELECT COUNT(*) FROM `%s`", source_full) INTO row_cnt;

  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS `%s` (
      run_ts TIMESTAMP,
      source_full STRING,
      target_full STRING,
      id_field STRING,
      order_by STRING,
      rows_in INT64,
      rows_out INT64
    )
  """, run_log_table);

  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `%s`
    (run_ts, source_full, target_full, id_field, order_by, rows_in, rows_out)
    VALUES (CURRENT_TIMESTAMP(), @src, @tgt, @idf, @ord, @rin, @rout)
  """, run_log_table)
  USING
    source_full AS src,
    target_full AS tgt,
    id_field AS idf,
    order_by_clause AS ord,
    row_cnt AS rin,
    rows_out AS rout;

END;
--
-- v4 | works if id is definied
    --   CREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_util.dedupe_table_by_primary_id_v3_2` (
    --     IN source_project STRING,
    --     IN source_schema  STRING,
    --     IN source_table   STRING,
    --     IN target_project STRING,
    --     IN target_schema  STRING,
    --     IN primary_key_override STRING  -- optional: e.g., 'ad_id'
    --   )
    --   BEGIN
    --     -- =========================
    --     -- Declarations (top only)
    --     -- =========================
    --     DECLARE source_full STRING DEFAULT NULL;
    --     DECLARE target_full STRING DEFAULT NULL;

    --     DECLARE id_field STRING DEFAULT NULL;
    --     DECLARE tmp_id STRING DEFAULT NULL;

    --     DECLARE override_exists BOOL DEFAULT FALSE;
    --     DECLARE exists_col BOOL DEFAULT FALSE;
    --     DECLARE final_exists BOOL DEFAULT FALSE;

    --     DECLARE has_updated_time BOOL DEFAULT FALSE;
    --     DECLARE has_updated_at BOOL DEFAULT FALSE;
    --     DECLARE has_fivetran_synced BOOL DEFAULT FALSE;

    --     DECLARE order_by_clause STRING DEFAULT '';

    --     DECLARE candidate_query STRING DEFAULT NULL;
    --     DECLARE query STRING DEFAULT NULL;

    --     DECLARE candidates ARRAY<STRING>;
    --     DECLARE row_cnt INT64 DEFAULT 0;
    --     DECLARE approx_distinct INT64 DEFAULT NULL;
    --     DECLARE ratio FLOAT64 DEFAULT NULL;

    --     DECLARE approx_selected INT64 DEFAULT NULL;
    --     DECLARE dup_ratio_selected FLOAT64 DEFAULT NULL;

    --     DECLARE rows_out INT64 DEFAULT 0;

    --     -- =========================
    --     -- Setup
    --     -- =========================
    --     SET source_full = FORMAT("%s.%s.%s", source_project, source_schema, source_table);
    --     SET target_full = FORMAT("%s.%s.stg__%s_deduped", target_project, target_schema, source_table);

    --     -- =========================
    --     -- Manual override (if provided and present)
    --     -- =========================
    --     IF primary_key_override IS NOT NULL THEN
    --       EXECUTE IMMEDIATE FORMAT("""
    --         SELECT COUNT(1) > 0
    --         FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    --         WHERE table_name = '%s' AND column_name = '%s'
    --       """, source_project, source_schema, source_table, primary_key_override)
    --       INTO override_exists;

    --       IF override_exists THEN
    --         SET id_field = primary_key_override;
    --       END IF;
    --     END IF;

    --     -- =========================
    --     -- Auto-scan *_id (and 'id') if no valid override
    --     -- =========================
    --     IF id_field IS NULL THEN
    --       SET candidate_query = FORMAT("""
    --         SELECT ARRAY_AGG(column_name ORDER BY column_name)
    --         FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    --         WHERE table_name = '%s'
    --           AND (REGEXP_CONTAINS(column_name, r'_id$') OR column_name = 'id')
    --       """, source_project, source_schema, source_table);

    --       EXECUTE IMMEDIATE candidate_query INTO candidates;
    --       EXECUTE IMMEDIATE FORMAT("SELECT COUNT(*) FROM `%s`", source_full) INTO row_cnt;

    --       CREATE TEMP TABLE _cand_metrics (
    --         col STRING,
    --         approx_distinct INT64,
    --         row_cnt INT64,
    --         dup_ratio FLOAT64
    --       );

    --       IF candidates IS NOT NULL THEN
    --         FOR rec IN (SELECT col FROM UNNEST(candidates) AS col) DO
    --           SET query = FORMAT("SELECT APPROX_COUNT_DISTINCT(`%s`) FROM `%s`", rec.col, source_full);
    --           EXECUTE IMMEDIATE query INTO approx_distinct;

    --           SET ratio = SAFE_DIVIDE(row_cnt, NULLIF(approx_distinct, 0));
    --           INSERT INTO _cand_metrics(col, approx_distinct, row_cnt, dup_ratio)
    --           VALUES (rec.col, approx_distinct, row_cnt, ratio);
    --         END FOR;

    --         -- Choose best by highest dup_ratio; prefer NOT 'id' on tie
    --         SET id_field = (
    --           SELECT col
    --           FROM _cand_metrics
    --           WHERE approx_distinct BETWEEN 1 AND row_cnt
    --           ORDER BY dup_ratio DESC,
    --                   CASE WHEN col = 'id' THEN 1 ELSE 0 END,
    --                   col
    --           LIMIT 1
    --         );

    --         SET approx_selected = (SELECT approx_distinct FROM _cand_metrics WHERE col = id_field LIMIT 1);
    --         SET dup_ratio_selected = (SELECT dup_ratio FROM _cand_metrics WHERE col = id_field LIMIT 1);
    --       END IF;
    --     END IF;

    --     -- =========================
    --     -- Heuristic fallback if still NULL
    --     -- =========================
    --     IF id_field IS NULL THEN
    --       -- 1) first part + _id (campaign_history -> campaign_id)
    --       SET tmp_id = FORMAT('%s_id', SPLIT(source_table, '_')[OFFSET(0)]);
    --       EXECUTE IMMEDIATE FORMAT("""
    --         SELECT COUNT(1) > 0
    --         FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    --         WHERE table_name = '%s' AND column_name = '%s'
    --       """, source_project, source_schema, source_table, tmp_id) INTO exists_col;

    --       IF exists_col THEN
    --         SET id_field = tmp_id;
    --       ELSE
    --         -- 2) all but last + _id (ad_group_history -> ad_group_id)
    --         SET tmp_id = CONCAT(
    --           ARRAY_TO_STRING(
    --             ARRAY_SLICE(SPLIT(source_table, '_'), 0, ARRAY_LENGTH(SPLIT(source_table, '_')) - 1),
    --             '_'
    --           ), '_id'
    --         );
    --         EXECUTE IMMEDIATE FORMAT("""
    --           SELECT COUNT(1) > 0
    --           FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    --           WHERE table_name = '%s' AND column_name = '%s'
    --         """, source_project, source_schema, source_table, tmp_id) INTO exists_col;

    --         IF exists_col THEN
    --           SET id_field = tmp_id;
    --         ELSE
    --           -- 3) strip _history + _id (adgroup_history -> adgroup_id)
    --           SET tmp_id = CONCAT(REGEXP_REPLACE(source_table, r'_history$', ''), '_id');
    --           EXECUTE IMMEDIATE FORMAT("""
    --             SELECT COUNT(1) > 0
    --             FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    --             WHERE table_name = '%s' AND column_name = '%s'
    --           """, source_project, source_schema, source_table, tmp_id) INTO exists_col;

    --           IF exists_col THEN
    --             SET id_field = tmp_id;
    --           ELSE
    --             -- 4) fallback 'id'
    --             SET id_field = 'id';
    --           END IF;
    --         END IF;
    --       END IF;
    --     END IF;

    --     -- =========================
    --     -- Final validation: chosen id exists
    --     -- =========================
    --     EXECUTE IMMEDIATE FORMAT("""
    --       SELECT COUNT(1) > 0
    --       FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    --       WHERE table_name = '%s' AND column_name = '%s'
    --     """, source_project, source_schema, source_table, id_field)
    --     INTO final_exists;

    --     IF NOT final_exists THEN
    --       RAISE USING MESSAGE = FORMAT(
    --         "No usable primary key found for %s.%s.%s. Chosen '%s' not present.",
    --         source_project, source_schema, source_table, id_field
    --       );
    --     END IF;

    --     -- =========================
    --     -- Timestamp-ish cols & ORDER BY
    --     -- =========================
    --     EXECUTE IMMEDIATE FORMAT("""
    --       SELECT COUNT(1) > 0 FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    --       WHERE table_name = '%s' AND column_name = 'updated_time'
    --     """, source_project, source_schema, source_table) INTO has_updated_time;

    --     EXECUTE IMMEDIATE FORMAT("""
    --       SELECT COUNT(1) > 0 FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    --       WHERE table_name = '%s' AND column_name = 'updated_at'
    --     """, source_project, source_schema, source_table) INTO has_updated_at;

    --     EXECUTE IMMEDIATE FORMAT("""
    --       SELECT COUNT(1) > 0 FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    --       WHERE table_name = '%s' AND column_name = '_fivetran_synced'
    --     """, source_project, source_schema, source_table) INTO has_fivetran_synced;

    --     IF NOT (has_updated_time OR has_updated_at OR has_fivetran_synced) THEN
    --       RAISE USING MESSAGE = FORMAT(
    --         "Neither 'updated_time', 'updated_at', nor '_fivetran_synced' in %s.%s.%s. Cannot dedupe reliably.",
    --         source_project, source_schema, source_table
    --       );
    --     END IF;

    --     SET order_by_clause = '';
    --     IF has_updated_time THEN
    --       SET order_by_clause = 'SAFE_CAST(updated_time AS TIMESTAMP) DESC';
    --     END IF;
    --     IF has_updated_at THEN
    --       SET order_by_clause = IF(order_by_clause = '', 'SAFE_CAST(updated_at AS TIMESTAMP) DESC',
    --                               order_by_clause || ', SAFE_CAST(updated_at AS TIMESTAMP) DESC');
    --     END IF;
    --     IF has_fivetran_synced THEN
    --       SET order_by_clause = IF(order_by_clause = '', 'SAFE_CAST(_fivetran_synced AS TIMESTAMP) DESC',
    --                               order_by_clause || ', SAFE_CAST(_fivetran_synced AS TIMESTAMP) DESC');
    --     END IF;

    --     -- =========================
    --     -- Pre-run audit: duplicate counts in SOURCE
    --     -- =========================
    --     EXECUTE IMMEDIATE FORMAT("""
    --       CREATE OR REPLACE TABLE `%s.%s.audit__%s__dup_counts` AS
    --       SELECT `%s` AS entity_id, COUNT(*) AS n
    --       FROM `%s`
    --       GROUP BY `%s`
    --       HAVING COUNT(*) > 1
    --       ORDER BY n DESC
    --       LIMIT 200
    --     """, target_project, target_schema, source_table, id_field, source_full, id_field);

    --     -- =========================
    --     -- Deduplicate into TARGET
    --     -- =========================
    --     EXECUTE IMMEDIATE FORMAT("""
    --       CREATE OR REPLACE TABLE `%s` AS
    --       SELECT *
    --       FROM (
    --         SELECT
    --           *,
    --           ROW_NUMBER() OVER (
    --             PARTITION BY `%s`
    --             ORDER BY %s
    --           ) AS dedupe
    --         FROM `%s`
    --       )
    --       WHERE dedupe = 1
    --     """, target_full, id_field, order_by_clause, source_full);

    --     -- =========================
    --     -- Run log
    --     -- =========================
    --     EXECUTE IMMEDIATE FORMAT("SELECT COUNT(*) FROM `%s`", target_full) INTO rows_out;
    --     IF row_cnt = 0 THEN
    --       EXECUTE IMMEDIATE FORMAT("SELECT COUNT(*) FROM `%s`", source_full) INTO row_cnt;
    --     END IF;

    --     IF approx_selected IS NULL THEN
    --       EXECUTE IMMEDIATE FORMAT("SELECT APPROX_COUNT_DISTINCT(`%s`) FROM `%s`", id_field, source_full)
    --       INTO approx_selected;
    --       SET dup_ratio_selected = SAFE_DIVIDE(row_cnt, NULLIF(approx_selected, 0));
    --     END IF;

    --     EXECUTE IMMEDIATE FORMAT("""
    --       CREATE TABLE IF NOT EXISTS `%s.%s.audit__%s__run_log` (
    --         run_ts TIMESTAMP,
    --         source_full STRING,
    --         target_full STRING,
    --         id_field STRING,
    --         order_by STRING,
    --         rows_in INT64,
    --         rows_out INT64,
    --         approx_distinct INT64,
    --         dup_ratio FLOAT64
    --       )
    --     """, target_project, target_schema, source_table);

    --     EXECUTE IMMEDIATE FORMAT("""
    --       INSERT INTO `%s.%s.audit__%s__run_log`
    --       (run_ts, source_full, target_full, id_field, order_by, rows_in, rows_out, approx_distinct, dup_ratio)
    --       VALUES (CURRENT_TIMESTAMP(), @src, @tgt, @idf, @ord, @rin, @rout, @ad, @dr)
    --     """, target_project, target_schema, source_table)
    --     USING
    --       source_full AS src,
    --       target_full AS tgt,
    --       id_field AS idf,
    --       order_by_clause AS ord,
    --       row_cnt AS rin,
    --       rows_out AS rout,
    --       approx_selected AS ad,
    --       dup_ratio_selected AS dr;

    --   END;

--
-- v3
  -- CREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_util.dedupe_table_by_primary_id_v3` (
  --   IN source_project STRING,
  --   IN source_schema  STRING,
  --   IN source_table   STRING,
  --   IN target_project STRING,
  --   IN target_schema  STRING
  -- )
  -- BEGIN
  --   /*
  --     --------------------------------------------------------------------------------
  --     🔁 Stored Procedure: dedupe_table_by_primary_id_v3

  --     📌 Description:
  --       Deduplicates a source table using ROW_NUMBER() by dynamically detecting the
  --       primary ID column with these strategies (in order):
  --         1) first_part_of_table_name || '_id'
  --         2) all_but_last_part_of_table_name || '_id'
  --         3) remove trailing '_history' from table_name, then || '_id'
  --         4) fallback: 'id'
  --       Partitions by that ID and orders by any of: updated_time, updated_at, _fivetran_synced
  --       (in that priority order if present). Errors if none of these timestamp columns exist.

  --     📤 Output:
  --       `{target_project}.{target_schema}.stg__{source_table}_deduped`
  --     --------------------------------------------------------------------------------
  --   */

  --   -- Vars
  --   DECLARE id_field            STRING;
  --   DECLARE temp_id             STRING;
  --   DECLARE col_exists          BOOL DEFAULT FALSE;
  --   DECLARE id_exists           BOOL DEFAULT FALSE;
  --   DECLARE source_full         STRING;
  --   DECLARE target_full         STRING;
  --   DECLARE has_updated_time    BOOL DEFAULT FALSE;
  --   DECLARE has_updated_at      BOOL DEFAULT FALSE;
  --   DECLARE has_fivetran_synced BOOL DEFAULT FALSE;
  --   DECLARE order_by_clause     STRING;

  --   -- 1) Try first part + _id (e.g., "campaign_history" -> "campaign_id")
  --   SET temp_id = FORMAT('%s_id', SPLIT(source_table, '_')[OFFSET(0)]);
  --   EXECUTE IMMEDIATE FORMAT("""
  --     SELECT COUNT(1) > 0
  --     FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --     WHERE table_name = '%s' AND column_name = '%s'
  --   """, source_project, source_schema, source_table, temp_id)
  --   INTO col_exists;

  --   IF col_exists THEN
  --     SET id_field = temp_id;
  --   ELSE
  --     -- 2) Join all but the last part + _id (e.g., "ad_group_history" -> "ad_group_id")
  --     SET temp_id = CONCAT(
  --       ARRAY_TO_STRING(
  --         ARRAY_SLICE(SPLIT(source_table, '_'), 0, ARRAY_LENGTH(SPLIT(source_table, '_')) - 1),
  --         '_'
  --       ),
  --       '_id'
  --     );
  --     EXECUTE IMMEDIATE FORMAT("""
  --       SELECT COUNT(1) > 0
  --       FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --       WHERE table_name = '%s' AND column_name = '%s'
  --     """, source_project, source_schema, source_table, temp_id)
  --     INTO col_exists;

  --     IF col_exists THEN
  --       SET id_field = temp_id;
  --     ELSE
  --       -- 3) Remove trailing "_history" then + _id (e.g., "adgroup_history" -> "adgroup_id")
  --       SET temp_id = CONCAT(REGEXP_REPLACE(source_table, r'_history$', ''), '_id');
  --       EXECUTE IMMEDIATE FORMAT("""
  --         SELECT COUNT(1) > 0
  --         FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --         WHERE table_name = '%s' AND column_name = '%s'
  --       """, source_project, source_schema, source_table, temp_id)
  --       INTO col_exists;

  --       IF col_exists THEN
  --         SET id_field = temp_id;
  --       ELSE
  --         -- 4) Fallback to plain 'id'
  --         SET id_field = 'id';
  --       END IF;

  --     END IF;

  --   END IF;

  --   -- Verify the chosen id_field actually exists; raise a clear error if not
  --   EXECUTE IMMEDIATE FORMAT("""
  --     SELECT COUNT(1) > 0
  --     FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --     WHERE table_name = '%s' AND column_name = '%s'
  --   """, source_project, source_schema, source_table, id_field)
  --   INTO id_exists;

  --   IF NOT id_exists THEN
  --     RAISE USING MESSAGE = FORMAT(
  --       "Could not find a primary ID column in %s.%s.%s. Tried '%s'.",
  --       source_project, source_schema, source_table, id_field
  --     );
  --   END IF;

  --   -- Fully-qualified names
  --   SET source_full = FORMAT("%s.%s.%s",           source_project, source_schema, source_table);
  --   SET target_full = FORMAT("%s.%s.stg__%s_deduped", target_project, target_schema, source_table);

  --   -- Detect timestamp-ish columns
  --   EXECUTE IMMEDIATE FORMAT("""
  --     SELECT COUNT(1) > 0
  --     FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --     WHERE table_name = '%s' AND column_name = 'updated_time'
  --   """, source_project, source_schema, source_table) INTO has_updated_time;

  --   EXECUTE IMMEDIATE FORMAT("""
  --     SELECT COUNT(1) > 0
  --     FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --     WHERE table_name = '%s' AND column_name = 'updated_at'
  --   """, source_project, source_schema, source_table) INTO has_updated_at;

  --   EXECUTE IMMEDIATE FORMAT("""
  --     SELECT COUNT(1) > 0
  --     FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --     WHERE table_name = '%s' AND column_name = '_fivetran_synced'
  --   """, source_project, source_schema, source_table) INTO has_fivetran_synced;

  --   -- Error if none exist
  --   IF NOT (has_updated_time OR has_updated_at OR has_fivetran_synced) THEN
  --     RAISE USING MESSAGE = FORMAT(
  --       "Neither 'updated_time', 'updated_at', nor '_fivetran_synced' found in %s.%s.%s. Cannot deduplicate reliably.",
  --       source_project, source_schema, source_table
  --     );
  --   END IF;

  --   -- Build ORDER BY clause in priority order:
  --   -- updated_time, then updated_at, then _fivetran_synced (only those that exist)
  --   SET order_by_clause = '';
  --   IF has_updated_time THEN
  --     SET order_by_clause = 'SAFE_CAST(updated_time AS TIMESTAMP) DESC';
  --   END IF;

  --   IF has_updated_at THEN
  --     SET order_by_clause = IF(
  --       order_by_clause = '',
  --       'SAFE_CAST(updated_at AS TIMESTAMP) DESC',
  --       order_by_clause || ', SAFE_CAST(updated_at AS TIMESTAMP) DESC'
  --     );
  --   END IF;

  --   IF has_fivetran_synced THEN
  --     SET order_by_clause = IF(
  --       order_by_clause = '',
  --       'SAFE_CAST(_fivetran_synced AS TIMESTAMP) DESC',
  --       order_by_clause || ', SAFE_CAST(_fivetran_synced AS TIMESTAMP) DESC'
  --     );
  --   END IF;

  --   -- Execute deduplication
  --   EXECUTE IMMEDIATE FORMAT("""
  --     CREATE OR REPLACE TABLE `%s` AS
  --     SELECT *
  --     FROM (
  --       SELECT
  --         *,
  --         ROW_NUMBER() OVER (
  --           PARTITION BY %s
  --           ORDER BY %s
  --         ) AS dedupe
  --       FROM `%s`
  --     )
  --     WHERE dedupe = 1
  --   """, target_full, id_field, order_by_clause, source_full);

  -- END;
--
--v2
  --   CREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_util.dedupe_table_by_primary_id_v3` (
  --     IN source_project STRING,
  --     IN source_schema STRING,
  --     IN source_table STRING,
  --     IN target_project STRING,
  --     IN target_schema STRING
  --   )
  --   BEGIN
  --   /*
  --     --------------------------------------------------------------------------------
  --     🔁 Stored Procedure: dedupe_table_by_primary_id_v3

  --     📌 Description:
  --       Deduplicates a source table using ROW_NUMBER() by dynamically detecting the primary 
  --       ID column. The procedure uses a sequence of strategies to find the ID column:  
  --         1. `{first_part}_id` (e.g., "adgroup_history" → "ad_id")
  --         2. `{all_but_last_part}_id` (e.g., "ad_group_history" → "ad_group_id")
  --         3. `{table_name minus '_history'}_id` (e.g., "adgroup_history" → "adgroup_id")
  --         4. Fallback to "id" if none of the above exist as columns.
  --       The deduplication partitions by the ID column and orders by `updated_at` and/or `_fivetran_synced` 
  --       (if either or both columns exist). If neither exists, an error is raised.

  --     📥 Input Parameters:
  --       - source_project:  GCP project containing the source table.
  --       - source_schema:   Dataset/schema containing the source table.
  --       - source_table:    Source table to deduplicate.
  --       - target_project:  GCP project where the deduplicated table will be written.
  --       - target_schema:   Dataset/schema for the deduplicated output.

  --     📤 Output:
  --       Writes the deduplicated result to:
  --         `{target_project}.{target_schema}.stg__{source_table}_deduped`

  --     🧪 Example Calls:
  --       CALL `looker-studio-pro-452620.repo_util.dedupe_table_by_primary_id_v3`(
  --         'giant-spoon-299605', 
  --         'google_ads_olipop', 
  --         'campaign_history', 
  --         'looker-studio-pro-452620',
  --         'repo_google_ads'
  --       )
  --     --------------------------------------------------------------------------------
  --   */

  --     -- Fallback logic for detecting the best primary key
  --     DECLARE id_field STRING;
  --     DECLARE temp_id STRING;
  --     DECLARE col_exists BOOL DEFAULT FALSE;
  --     DECLARE source_full STRING;
  --     DECLARE target_full STRING;
  --     DECLARE has_updated_time BOOL DEFAULT FALSE;
  --     DECLARE has_updated_at BOOL DEFAULT FALSE;
  --     DECLARE has_fivetran_synced BOOL DEFAULT FALSE;
  --     DECLARE order_by_clause STRING;

  --     -- 1. Try simple prefix + _id
  --     SET temp_id = FORMAT('%s_id', SPLIT(source_table, '_')[OFFSET(0)]);
  --     EXECUTE IMMEDIATE FORMAT("""
  --       SELECT COUNT(1) > 0
  --       FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --       WHERE table_name = '%s' AND column_name = '%s'
  --     """, source_project, source_schema, source_table, temp_id)
  --     INTO col_exists;

  --     IF col_exists THEN
  --       SET id_field = temp_id;

  --     ELSE
  --       -- 2. Try joining all but last SPLIT part + _id
  --       SET temp_id = CONCAT(
  --         ARRAY_TO_STRING(
  --           ARRAY_SLICE(SPLIT(source_table, '_'), 0, ARRAY_LENGTH(SPLIT(source_table, '_')) - 1),
  --           '_'
  --         ),
  --         '_id'
  --       );
  --       EXECUTE IMMEDIATE FORMAT("""
  --         SELECT COUNT(1) > 0
  --         FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --         WHERE table_name = '%s' AND column_name = '%s'
  --       """, source_project, source_schema, source_table, temp_id)
  --       INTO col_exists;

  --       IF col_exists THEN
  --         SET id_field = temp_id;

  --       ELSE
  --         -- 3. Try regex method
  --         SET temp_id = CONCAT(REGEXP_REPLACE(source_table, r'_history$', ''), '_id');
  --         EXECUTE IMMEDIATE FORMAT("""
  --           SELECT COUNT(1) > 0
  --           FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --           WHERE table_name = '%s' AND column_name = '%s'
  --         """, source_project, source_schema, source_table, temp_id)
  --         INTO col_exists;

  --         IF col_exists THEN
  --           SET id_field = temp_id;

  --         ELSE
  --           -- 4. Fallback: just 'id'
  --           SET id_field = 'id';
  --         END IF;

  --       END IF;

  --     END IF;

  --     -- Build fully-qualified table names
  --     SET source_full = FORMAT("%s.%s.%s", source_project, source_schema, source_table);
  --     SET target_full = FORMAT("%s.%s.stg__%s_deduped", target_project, target_schema, source_table);

  --     -- Check for 'updated_time' column
  --     EXECUTE IMMEDIATE
  --       FORMAT("""
  --         SELECT COUNT(1) > 0
  --         FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --         WHERE table_name = '%s' AND column_name = 'updated_time'
  --       """, source_project, source_schema, source_table) INTO has_updated_time;
  --     -- Check for 'updated_at' column
  --     EXECUTE IMMEDIATE
  --       FORMAT("""
  --         SELECT COUNT(1) > 0
  --         FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --         WHERE table_name = '%s' AND column_name = 'updated_at'
  --       """, source_project, source_schema, source_table) INTO has_updated_at;
  --     -- Check for '_fivetran_synced' column
  --     EXECUTE IMMEDIATE
  --       FORMAT("""
  --         SELECT COUNT(1) > 0
  --         FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --         WHERE table_name = '%s' AND column_name = '_fivetran_synced'
  --       """, source_project, source_schema, source_table) INTO has_fivetran_synced;
  --     -- Error if none of the timestamp columns exist
  --     IF NOT (has_updated_time
  --         OR has_updated_at
  --         OR has_fivetran_synced) THEN RAISE
  --     USING
  --       MESSAGE = FORMAT( "Neither 'updated_time', 'updated_at' nor '_fivetran_synced' columns found in %s.%s.%s. Cannot deduplicate reliably.", source_project, source_schema, source_table);
  --     END IF
  --       ;
  --     -- Build ORDER BY clause dynamically
  --     SET
  --       order_by_clause = (
  --       SELECT
  --         CASE
  --           WHEN has_updated_time AND has_updated_at AND has_fivetran_synced THEN 'cast(updated_time as date) DESC, cast(updated_at as date) DESC, cast(_fivetran_synced as date) DESC'
  --           WHEN has_updated_time
  --         AND has_updated_at THEN 'cast(updated_time as date) DESC, cast(updated_at as date) DESC'
  --           WHEN has_updated_time AND has_fivetran_synced THEN 'cast(updated_time as date) DESC, cast(_fivetran_synced as date) DESC'
  --           WHEN has_updated_at
  --         AND has_fivetran_synced THEN 'cast(updated_at as date) DESC, cast(_fivetran_synced as date) DESC'
  --           WHEN has_updated_time THEN 'cast(updated_time as date) DESC'
  --           WHEN has_updated_at THEN 'cast(updated_at as date) DESC'
  --           WHEN has_fivetran_synced THEN 'cast(_fivetran_synced as date) DESC'
  --       END
  --         );
  --     -- -- Check for 'updated_at' column
  --     -- EXECUTE IMMEDIATE FORMAT("""
  --     --   SELECT COUNT(1) > 0
  --     --   FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --     --   WHERE table_name = '%s' AND column_name = 'updated_at'
  --     -- """, source_project, source_schema, source_table)
  --     -- INTO has_updated_at;

  --     -- -- Check for '_fivetran_synced' column
  --     -- EXECUTE IMMEDIATE FORMAT("""
  --     --   SELECT COUNT(1) > 0
  --     --   FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  --     --   WHERE table_name = '%s' AND column_name = '_fivetran_synced'
  --     -- """, source_project, source_schema, source_table)
  --     -- INTO has_fivetran_synced;

  --     -- -- Error if neither exists (don't silently use ORDER BY 1)
  --     -- IF NOT (has_updated_at OR has_fivetran_synced) THEN
  --     --   RAISE USING MESSAGE = FORMAT(
  --     --     "Neither 'updated_at' nor '_fivetran_synced' columns found in %s.%s.%s. Cannot deduplicate reliably.",
  --     --     source_project, source_schema, source_table
  --     --   );
  --     -- END IF;

  --     -- -- Build ORDER BY clause dynamically
  --     -- SET order_by_clause = (
  --     --   SELECT
  --     --     CASE
  --     --       WHEN has_updated_at AND has_fivetran_synced THEN 'cast(updated_at as date) DESC, cast(_fivetran_synced as date) DESC'
  --     --       WHEN has_updated_at THEN 'cast(updated_at as date) DESC'
  --     --       WHEN has_fivetran_synced THEN 'cast(_fivetran_synced as date) DESC'
  --     --     END
  --     -- );

  --     -- Execute deduplication query
  --     EXECUTE IMMEDIATE FORMAT("""
  --       CREATE OR REPLACE TABLE `%s` AS
  --       SELECT *
  --       FROM (
  --         SELECT *, 
  --               ROW_NUMBER() OVER (
  --                 PARTITION BY %s 
  --                 ORDER BY %s
  --               ) AS dedupe
  --         FROM `%s`
  --       )
  --       WHERE dedupe = 1
  --     """, target_full, id_field, order_by_clause, source_full);

  --   END;


-- --
--v1
  /* --ORIGINAL CODE-- SCRAP--
  CREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_util.dedupe_table_by_primary_id_v3`(IN source_project STRING, IN source_schema STRING, IN source_table STRING, IN target_ STRING)
  BEGIN
  */
  /*
    --------------------------------------------------------------------------------
    🔁 Stored Procedure: dedupe_table_by_primary_id_v3

    📌 Description:
      Deduplicates a table using ROW_NUMBER() by dynamically detecting the primary 
      ID column from the table name prefix (e.g., "adgroup_history" → "adgroup_id").
      Checks for `updated_at` and `_fivetran_synced` columns; orders by both if available,
      otherwise whichever is present. Raises an error if neither column exists.

    📥 Input Parameters:
      - source_project: The GCP project containing the source table.
      - source_schema:  The dataset/schema containing the source table.
      - source_table:   The source table to deduplicate.
      - target_:  The [project].[schema] where the deduped table will be written.

    📤 Output:
      Writes the deduplicated result to:
      `{source_project}.{target_schema}.stg__{source_table}_deduped`

    🧪 Example Calls:
      CALL `looker-studio-pro-452620.repo_util.dedupe_table_by_primary_id_v3`(
        'giant-spoon-299605', 'tiktok_ads', 'adgroup_history', 'repo_tiktok'
      );
      CALL `looker-studio-pro-452620.repo_util.dedupe_table_by_primary_id_v3`(
        'giant-spoon-299605', 'tiktok_ads', 'campaign_history', 'repo_tiktok'
      );
    --------------------------------------------------------------------------------
  */
  /*
    DECLARE id_field STRING;
    DECLARE source_full STRING;
    DECLARE target_full STRING;
    DECLARE has_updated_at BOOL DEFAULT FALSE;
    DECLARE has_fivetran_synced BOOL DEFAULT FALSE;
    DECLARE order_by_clause STRING;

    -- Derive key and table names
    SET id_field = CONCAT(REGEXP_REPLACE(source_table, r'_history$', ''), '_id');
    SET source_full = FORMAT("%s.%s.%s", source_project, source_schema, source_table);
    SET target_full = FORMAT("%s.stg__%s_deduped", target_,source_table);

  -- Check for 'updated_at' column
  EXECUTE IMMEDIATE FORMAT("""
    SELECT COUNT(1) > 0
    FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '%s' AND column_name = 'updated_at'
  """, source_project, source_schema, source_table)
  INTO has_updated_at;

  -- Check for '_fivetran_synced' column
  EXECUTE IMMEDIATE FORMAT("""
    SELECT COUNT(1) > 0
    FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '%s' AND column_name = '_fivetran_synced'
  """, source_project, source_schema, source_table)
  INTO has_fivetran_synced;

    -- Build ORDER BY clause dynamically
    SET order_by_clause = (
      SELECT
        CASE
          WHEN has_updated_at AND has_fivetran_synced THEN 'cast(updated_at as date) DESC, cast(_fivetran_synced as date) DESC'
          WHEN has_updated_at THEN 'cast(updated_at as date) DESC'
          WHEN has_fivetran_synced THEN 'cast(_fivetran_synced as date) DESC'
          ELSE '1'
        END
    );

    -- Execute deduplication query
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TABLE `%s` AS
      SELECT *
      FROM (
        SELECT *, 
              ROW_NUMBER() OVER (
                PARTITION BY %s 
                ORDER BY %s
              ) AS dedupe
        FROM `%s`
      )
      WHERE dedupe = 1
    """, target_full, id_field, order_by_clause, source_full);

  END;
  */