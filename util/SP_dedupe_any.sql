CREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_util.dedupe_table_by_primary_id_v3` (
  IN source_project STRING,
  IN source_schema STRING,
  IN source_table STRING,
  IN target_project STRING,
  IN target_schema STRING
)
BEGIN
/*
  --------------------------------------------------------------------------------
  🔁 Stored Procedure: dedupe_table_by_primary_id_v3

  📌 Description:
     Deduplicates a source table using ROW_NUMBER() by dynamically detecting the primary 
     ID column. The procedure uses a sequence of strategies to find the ID column:  
       1. `{first_part}_id` (e.g., "adgroup_history" → "ad_id")
       2. `{all_but_last_part}_id` (e.g., "ad_group_history" → "ad_group_id")
       3. `{table_name minus '_history'}_id` (e.g., "adgroup_history" → "adgroup_id")
       4. Fallback to "id" if none of the above exist as columns.
     The deduplication partitions by the ID column and orders by `updated_at` and/or `_fivetran_synced` 
     (if either or both columns exist). If neither exists, an error is raised.

  📥 Input Parameters:
     - source_project:  GCP project containing the source table.
     - source_schema:   Dataset/schema containing the source table.
     - source_table:    Source table to deduplicate.
     - target_project:  GCP project where the deduplicated table will be written.
     - target_schema:   Dataset/schema for the deduplicated output.

  📤 Output:
     Writes the deduplicated result to:
       `{target_project}.{target_schema}.stg__{source_table}_deduped`

  🧪 Example Calls:
     CALL `looker-studio-pro-452620.repo_util.dedupe_table_by_primary_id_v3`(
       'giant-spoon-299605', 
       'google_ads_olipop', 
       'campaign_history', 
       'looker-studio-pro-452620',
       'repo_google_ads'
     )
  --------------------------------------------------------------------------------
*/

  -- Fallback logic for detecting the best primary key
  DECLARE id_field STRING;
  DECLARE temp_id STRING;
  DECLARE col_exists BOOL DEFAULT FALSE;
  DECLARE source_full STRING;
  DECLARE target_full STRING;
  DECLARE has_updated_at BOOL DEFAULT FALSE;
  DECLARE has_fivetran_synced BOOL DEFAULT FALSE;
  DECLARE order_by_clause STRING;

  -- 1. Try simple prefix + _id
  SET temp_id = FORMAT('%s_id', SPLIT(source_table, '_')[OFFSET(0)]);
  EXECUTE IMMEDIATE FORMAT("""
    SELECT COUNT(1) > 0
    FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '%s' AND column_name = '%s'
  """, source_project, source_schema, source_table, temp_id)
  INTO col_exists;

  IF col_exists THEN
    SET id_field = temp_id;

  ELSE
    -- 2. Try joining all but last SPLIT part + _id
    SET temp_id = CONCAT(
      ARRAY_TO_STRING(
        ARRAY_SLICE(SPLIT(source_table, '_'), 0, ARRAY_LENGTH(SPLIT(source_table, '_')) - 1),
        '_'
      ),
      '_id'
    );
    EXECUTE IMMEDIATE FORMAT("""
      SELECT COUNT(1) > 0
      FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '%s' AND column_name = '%s'
    """, source_project, source_schema, source_table, temp_id)
    INTO col_exists;

    IF col_exists THEN
      SET id_field = temp_id;

    ELSE
      -- 3. Try regex method
      SET temp_id = CONCAT(REGEXP_REPLACE(source_table, r'_history$', ''), '_id');
      EXECUTE IMMEDIATE FORMAT("""
        SELECT COUNT(1) > 0
        FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '%s' AND column_name = '%s'
      """, source_project, source_schema, source_table, temp_id)
      INTO col_exists;

      IF col_exists THEN
        SET id_field = temp_id;

      ELSE
        -- 4. Fallback: just 'id'
        SET id_field = 'id';
      END IF;

    END IF;

  END IF;

  -- Build fully-qualified table names
  SET source_full = FORMAT("%s.%s.%s", source_project, source_schema, source_table);
  SET target_full = FORMAT("%s.%s.stg__%s_deduped", target_project, target_schema, source_table);

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

  -- Error if neither exists (don't silently use ORDER BY 1)
  IF NOT (has_updated_at OR has_fivetran_synced) THEN
    RAISE USING MESSAGE = FORMAT(
      "Neither 'updated_at' nor '_fivetran_synced' columns found in %s.%s.%s. Cannot deduplicate reliably.",
      source_project, source_schema, source_table
    );
  END IF;

  -- Build ORDER BY clause dynamically
  SET order_by_clause = (
    SELECT
      CASE
        WHEN has_updated_at AND has_fivetran_synced THEN 'updated_at DESC, _fivetran_synced DESC'
        WHEN has_updated_at THEN 'updated_at DESC'
        WHEN has_fivetran_synced THEN '_fivetran_synced DESC'
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
        WHEN has_updated_at AND has_fivetran_synced THEN 'updated_at DESC, _fivetran_synced DESC'
        WHEN has_updated_at THEN 'updated_at DESC'
        WHEN has_fivetran_synced THEN '_fivetran_synced DESC'
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