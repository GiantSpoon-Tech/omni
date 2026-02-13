-- ────────────────────────────────────────────────────────────────────────────────
-- @title:        UTIL – Find Primary Key Candidates
-- @description:  Profiles a table to identify strong primary-key candidates.
--                Returns ranked single-column candidates and (optionally) 2-column
--                combos. Supports excluding data types and sampling large tables.
--
-- @inputs:
--    in_project                STRING    Project of target table
--    in_dataset                STRING    Dataset of target table
--    in_table                  STRING    Table name
--
--    enable_pairs              BOOL      TRUE → also evaluate 2-column combos
--    max_pair_cols             INT64     Max columns to consider for pairs
--
--    exclude_types             ARRAY<STRING>
--                               Data types to skip (case-insensitive), e.g.:
--                               ['FLOAT64','JSON','GEOGRAPHY','RECORD','NUMERIC']
--
--    use_sample                BOOL      TRUE → sample very large tables
--    sample_percent            FLOAT64   Percent for TABLESAMPLE SYSTEM, e.g., 10.0
--    sample_repeatable_seed    INT64     Seed for REPEATABLE(...). 0/NULL → no seed
--    sample_threshold_rows     INT64     Only sample if row_cnt > this many rows
--    sample_max_rows           INT64     Optional ~cap after sampling (approximate)
--
-- @outputs:
--    Result set #1: Best single-column candidates
--    Result set #2: Best 2-column candidates (if enable_pairs)
--
-- @notes:
--  - Score = uniqueness_ratio - null_ratio.
--  - Pair distincts are computed via GROUP BY (BQ disallows COUNT(DISTINCT STRUCT)).
--  - Sampling uses TABLESAMPLE SYSTEM (block-based; approximate). Use REPEATABLE(seed)
--    for determinism. If sample_max_rows is set and exceeded, a second pass reduces size.
-- @last_updated: 2025-08-20

-- @EXAMPLE
-- CALL `looker-studio-pro-452620.repo_util.find_primary_key_candidates`(
  
--   'looker-studio-pro-452620',
--   'repo_mart',
--   'mart__olipop__crossplatform',
--   TRUE, 4,                                -- enable_pairs, max_pair_cols
--   ['JSON','GEOGRAPHY','FLOAT64','NUMERIC','INT64'],-- exclude_types
--   false, 10.0, 12345, 500000, 50000 -- sample: 10% if >50M rows; seed=12345; cap ~5M rows

-- ────────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_util.find_primary_key_candidates` (
  IN in_project                STRING,
  IN in_dataset                STRING,
  IN in_table                  STRING,

  IN enable_pairs              BOOL,
  IN max_pair_cols             INT64,

  IN exclude_types             ARRAY<STRING>,

  IN use_sample                BOOL,
  IN sample_percent            FLOAT64,
  IN sample_repeatable_seed    INT64,
  IN sample_threshold_rows     INT64,
  IN sample_max_rows           INT64
)
BEGIN
  -- Declarations
  DECLARE full_table STRING;
  DECLARE base_table STRING;
  DECLARE row_cnt INT64;
  DECLARE sql STRING;

  DECLARE col_name STRING;
  DECLARE col_dtype STRING;

  DECLARE exclude_types_upper ARRAY<STRING>;
  DECLARE col_list ARRAY<STRING>;
  DECLARE i INT64;
  DECLARE j INT64;
  DECLARE col_i STRING;
  DECLARE col_j STRING;

  -- Fully qualified table
  SET base_table = FORMAT('`%s.%s.%s`', in_project, in_dataset, in_table);
  SET full_table = base_table;

  -- Count rows
  SET sql = FORMAT('SELECT COUNT(1) FROM %s', full_table);
  EXECUTE IMMEDIATE sql INTO row_cnt;

  -- Normalize exclude_types to UPPER() array
  SET exclude_types_upper = (
    SELECT IFNULL(ARRAY_AGG(UPPER(t)), ARRAY<STRING>[])
    FROM UNNEST(COALESCE(exclude_types, ARRAY<STRING>[])) AS t
  );

  -- Optional sampling for large tables
  IF use_sample AND row_cnt > sample_threshold_rows THEN
    -- First pass
    SET sql = FORMAT(
      '''
      CREATE TEMP TABLE sampled__tmp AS
      SELECT *
      FROM %s
      TABLESAMPLE SYSTEM (%.6f PERCENT) %s
      ''',
      full_table,
      sample_percent,
      CASE
        WHEN sample_repeatable_seed IS NULL OR sample_repeatable_seed = 0
          THEN ''
        ELSE FORMAT('REPEATABLE (%d)', sample_repeatable_seed)
      END
    );
    EXECUTE IMMEDIATE sql;

    SET full_table = '`sampled__tmp`';
    SET sql = 'SELECT COUNT(1) FROM `sampled__tmp`';
    EXECUTE IMMEDIATE sql INTO row_cnt;

    -- Optional second pass to approximate a hard row cap
    IF sample_max_rows IS NOT NULL AND sample_max_rows > 0 AND row_cnt > sample_max_rows THEN
      SET sql = FORMAT(
        '''
        CREATE TEMP TABLE sampled__tmp2 AS
        SELECT *
        FROM `sampled__tmp`
        TABLESAMPLE SYSTEM (%.6f PERCENT) %s
        ''',
        100.0 * CAST(sample_max_rows AS FLOAT64) / NULLIF(row_cnt,0),
        CASE
          WHEN sample_repeatable_seed IS NULL OR sample_repeatable_seed = 0
            THEN ''
          ELSE FORMAT('REPEATABLE (%d)', sample_repeatable_seed)
        END
      );
      EXECUTE IMMEDIATE sql;

      SET full_table = '`sampled__tmp2`';
      SET sql = 'SELECT COUNT(1) FROM `sampled__tmp2`';
      EXECUTE IMMEDIATE sql INTO row_cnt;
    END IF;
  END IF;

  -- Temp tables (create empty with schema)
  CREATE TEMP TABLE column_stats AS
  SELECT
    '' AS column_name,
    '' AS data_type,
    0  AS null_count,
    0  AS distinct_count,
    0.0 AS uniqueness_ratio,
    0.0 AS null_ratio,
    FALSE AS is_unique,
    FALSE AS is_constant,
    0.0  AS score
  FROM UNNEST([1]) LIMIT 0;

  CREATE TEMP TABLE pair_stats AS
  SELECT
    '' AS col1,
    '' AS col2,
    0  AS null_rows,
    0  AS distinct_pairs,
    0.0 AS uniqueness_ratio,
    0.0 AS null_ratio,
    FALSE AS is_unique,
    0.0  AS score
  FROM UNNEST([1]) LIMIT 0;

  -- Per-column stats (honor excludes)
  FOR rec IN (
    SELECT column_name, data_type
    FROM `region-us`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_catalog = in_project
      AND table_schema  = in_dataset
      AND table_name    = in_table
      AND (ARRAY_LENGTH(exclude_types_upper) = 0
           OR UPPER(data_type) NOT IN UNNEST(exclude_types_upper))
  ) DO
    SET col_name  = rec.column_name;
    SET col_dtype = rec.data_type;

    SET sql = FORMAT(
      '''
      INSERT INTO column_stats (column_name, data_type, null_count, distinct_count)
      SELECT
        @col_name,
        @col_dtype,
        SUM(CASE WHEN %s IS NULL THEN 1 ELSE 0 END),
        COUNT(DISTINCT %s)
      FROM %s
      ''',
      FORMAT('`%s`', col_name),
      FORMAT('`%s`', col_name),
      full_table
    );
    EXECUTE IMMEDIATE sql USING col_name AS col_name, col_dtype AS col_dtype;
  END FOR;

  -- Derive ratios/flags/scores for columns
  UPDATE column_stats
  SET
    uniqueness_ratio = SAFE_DIVIDE(distinct_count, row_cnt),
    null_ratio       = SAFE_DIVIDE(null_count, row_cnt),
    is_unique        = (distinct_count = row_cnt AND null_count = 0),
    is_constant      = (distinct_count = 1),
    score            = SAFE_DIVIDE(distinct_count, row_cnt) - SAFE_DIVIDE(null_count, row_cnt)
  WHERE TRUE;

  -- Optional: evaluate 2-column combos
  IF enable_pairs THEN
    -- Build list of candidate columns (dynamic for parameterized LIMIT)
    SET sql = '''
      SELECT ARRAY_AGG(column_name ORDER BY uniqueness_ratio DESC)
      FROM (
        SELECT column_name, uniqueness_ratio
        FROM column_stats
        WHERE uniqueness_ratio < 0.9999
          AND NOT is_constant
        ORDER BY uniqueness_ratio DESC
        LIMIT @lim
      )
    ''';
    EXECUTE IMMEDIATE sql INTO col_list USING max_pair_cols AS lim;
    SET col_list = COALESCE(col_list, ARRAY<STRING>[]);

    -- Pair loop
    SET i = 1;
    WHILE i < ARRAY_LENGTH(col_list) DO
      SET col_i = col_list[OFFSET(i-1)];
      SET j = i + 1;

      WHILE j <= ARRAY_LENGTH(col_list) DO
        SET col_j = col_list[OFFSET(j-1)];

        -- Insert pair stats (GROUP BY subquery to compute distinct_pairs)
        SET sql = FORMAT(
          '''
          INSERT INTO pair_stats (col1, col2, null_rows, distinct_pairs)
          WITH src AS (
            SELECT %s AS c1, %s AS c2
            FROM %s
          ),
          pair_distincts AS (
            SELECT COUNT(*) AS cnt
            FROM (
              SELECT c1, c2
              FROM src
              WHERE c1 IS NOT NULL AND c2 IS NOT NULL
              GROUP BY c1, c2
            )
          )
          SELECT
            @c1, @c2,
            SUM(CASE WHEN c1 IS NULL OR c2 IS NULL THEN 1 ELSE 0 END) AS null_rows,
            (SELECT cnt FROM pair_distincts) AS distinct_pairs
          FROM src
          ''',
          FORMAT('`%s`', col_i),
          FORMAT('`%s`', col_j),
          full_table
        );
        EXECUTE IMMEDIATE sql USING col_i AS c1, col_j AS c2;

        SET j = j + 1;
      END WHILE;

      SET i = i + 1;
    END WHILE;

    -- Derive ratios/flags/scores for pairs
    UPDATE pair_stats
    SET
      uniqueness_ratio = SAFE_DIVIDE(distinct_pairs, row_cnt),
      null_ratio       = SAFE_DIVIDE(null_rows, row_cnt),
      is_unique        = (distinct_pairs = row_cnt AND null_rows = 0),
      score            = SAFE_DIVIDE(distinct_pairs, row_cnt) - SAFE_DIVIDE(null_rows, row_cnt)
    WHERE TRUE;
  END IF;

  -- Result set #1: single-column candidates
  SELECT
    column_name,
    data_type,
    row_cnt       AS table_rows,
    null_count,
    distinct_count,
    ROUND(uniqueness_ratio, 6) AS uniqueness_ratio,
    ROUND(null_ratio, 6)       AS null_ratio,
    is_unique,
    is_constant,
    ROUND(score, 6)            AS score
  FROM column_stats
  ORDER BY is_unique DESC, score DESC, null_count ASC
  LIMIT 100;

  -- Result set #2: 2-column candidates
  IF enable_pairs THEN
    SELECT
      col1,
      col2,
      row_cnt AS table_rows,
      null_rows,
      distinct_pairs,
      ROUND(uniqueness_ratio, 6) AS uniqueness_ratio,
      ROUND(null_ratio, 6)       AS null_ratio,
      is_unique,
      ROUND(score, 6)            AS score
    FROM pair_stats
    ORDER BY is_unique DESC, score DESC, null_rows ASC
    LIMIT 200;
  END IF;

END;