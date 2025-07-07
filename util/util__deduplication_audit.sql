CREATE OR REPLACE PROCEDURE `repo_util.util__deduplication_audit`(
    p_source_table  STRING,          -- Fully-qualified table to de-duplicate
    p_join_table    STRING,          -- Fully-qualified table to LEFT JOIN
    p_key_cols      ARRAY<STRING>,   -- Keys used for dedup + join + grouping
    p_order_col     STRING           -- NULL ⇒ keep any duplicate; else ORDER BY
)
BEGIN
  /* ============================================================================
     PROCEDURE  : repo_util.util__deduplication_audit
     PURPOSE    : 1) De-duplicate p_source_table on p_key_cols
                  2) LEFT JOIN the de-duped rows to p_join_table
                  3) Return an audit-friendly aggregate (SUM row_num)
     ----------------------------------------------------------------------------
     PARAMETERS :
       p_source_table   STRING         e.g. 'project.dataset.source_tbl'
       p_join_table     STRING         e.g. 'project.dataset.side_tbl'
       p_key_cols       ARRAY<STRING>  columns that:
                                         • partition ROW_NUMBER()
                                         • build JOIN predicate
                                         • appear in SELECT / GROUP BY
       p_order_col      STRING         NULL  → nondeterministic pick
                                         'col' → keep highest 'col' per key set
     OUTPUT     :
       TEMP TABLE  deduped_joined  (session-scoped)

     USAGE (pattern)
       CALL repo_util.util__deduplication_audit(
         '<source_table>',
         '<join_table>',
         ['key1','key2', …],
         <order_col_or_NULL>
       );

     USAGE (example)
       CALL repo_util.util__deduplication_audit(
         'giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report',
         'looker-studio-pro-452620.repo_mart.stg__videoviews',
         ['project_id','source_relation','date_day',
          'campaign_id','ad_id','ad_group_id'],
         NULL            -- or '_ingested_at' for deterministic ordering
       );

     NOTES      :
       • All DECLAREs appear before any other statement (BigQuery rule).
       • Dynamic SQL (EXECUTE IMMEDIATE) drives flexibility.
       • Result lives only for the session—CTAS it out if you need it later.
  ============================================================================ */

  -- 1️⃣ Declarations ──────────────────────────────────────────────────────────
  DECLARE v_key_list STRING;   -- "col1, col2, …"
  DECLARE v_order_by STRING;   -- "" or "ORDER BY <p_order_col> DESC"
  DECLARE v_join_on  STRING;   -- "a.col1 = b.col1 AND …"

  -- 2️⃣ Derive helper strings ────────────────────────────────────────────────
  SET v_key_list = ARRAY_TO_STRING(p_key_cols, ', ');

  IF p_order_col IS NULL THEN
    SET v_order_by = '';                       -- no ORDER BY (random pick)
  ELSE
    SET v_order_by = FORMAT('ORDER BY %s DESC', p_order_col);
  END IF;

  SET v_join_on = (
    SELECT STRING_AGG(
             FORMAT('a.%s = b.%s', col, col), ' AND ')
    FROM UNNEST(p_key_cols) AS col
  );

  -- 3️⃣ Dynamic query ────────────────────────────────────────────────────────
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE deduped_joined AS
    /* Stage 1: tag duplicates */
    WITH tagged AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY %s
          %s
        ) AS row_num
      FROM `%s`
    ),
    /* Stage 2: enrich via LEFT JOIN */
    joined AS (
      SELECT a.*
      FROM tagged a
      LEFT JOIN `%s` b
      ON %s
    )
    /* Stage 3: aggregate for audit */
    SELECT
      %s,
      SUM(row_num) AS row_num        -- 1 = unique, >1 = duplicates collapsed
    FROM joined
    GROUP BY %s
    ORDER BY row_num DESC;
  """,
    v_key_list,     -- %s #1
    v_order_by,     -- %s #2
    p_source_table, -- %s #3
    p_join_table,   -- %s #4
    v_join_on,      -- %s #5
    v_key_list,     -- %s #6
    v_key_list      -- %s #7
  );

END;