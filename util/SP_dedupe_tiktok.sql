--V2--------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_tiktok.dedupe_table_by_primary_id_v2`(
  IN source_table STRING
)
BEGIN
  DECLARE id_field STRING;
  DECLARE source_full STRING;
  DECLARE target_full STRING;
  DECLARE order_by_clause STRING;
  DECLARE has_updated_at BOOL;
  DECLARE has_fivetran_synced BOOL;

  -- Derive key and table names
  SET id_field = FORMAT('%s_id', SPLIT(source_table, '_')[OFFSET(0)]);
  SET source_full = FORMAT("giant-spoon-299605.tiktok_ads.%s", source_table);
  SET target_full = FORMAT("looker-studio-pro-452620.repo_tiktok.stg__%s_deduped", source_table);

  -- Check if columns exist in the tiktok_ads dataset
  SET has_updated_at = EXISTS (
    SELECT 1
    FROM `giant-spoon-299605.tiktok_ads.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = source_table
      AND table_schema = 'tiktok_ads'
      AND column_name = 'updated_at'
  );

  SET has_fivetran_synced = EXISTS (
    SELECT 1
    FROM `giant-spoon-299605.tiktok_ads.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = source_table
      AND table_schema = 'tiktok_ads'
      AND column_name = '_fivetran_synced'
  );

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

----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_tiktok.dedupe_table_by_primary_id`(
  IN source_table STRING
)
BEGIN
  /*
  --------------------------------------------------------------------------------
  🔁 Stored Procedure: dedupe_table_by_primary_id

  📌 Description:
     Deduplicates a table using ROW_NUMBER() by dynamically detecting the primary 
     ID column from the table name prefix (e.g., "adgroup_history" → "adgroup_id").
     Orders by `updated_at DESC, _fivetran_synced DESC`.

  📤 Output:
     Writes the deduplicated result to:
     `looker-studio-pro-452620.repo_tiktok.stg__{source_table}_deduped`

  🧪 Example Calls:
     CALL `looker-studio-pro-452620.repo_tiktok.dedupe_table_by_primary_id`('adgroup_history');
     CALL `looker-studio-pro-452620.repo_tiktok.dedupe_table_by_primary_id`('campaign_history');
  --------------------------------------------------------------------------------
  */

  DECLARE id_field STRING;
  DECLARE source_full STRING;
  DECLARE target_full STRING;

  -- Derive ID field and full table names
  SET id_field = FORMAT('%s_id', SPLIT(source_table, '_')[OFFSET(0)]);
  SET source_full = FORMAT("giant-spoon-299605.tiktok_ads.%s", source_table);
  SET target_full = FORMAT("looker-studio-pro-452620.repo_tiktok.stg__%s_deduped", source_table);

  -- Validate required fields exist
  IF NOT EXISTS (
    SELECT 1
    FROM `giant-spoon-299605.tiktok_ads.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = source_table
      AND column_name = 'updated_at'
  ) THEN
    RAISE USING MESSAGE = FORMAT("Column 'updated_at' is missing from %s", source_table);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM `giant-spoon-299605.tiktok_ads.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = source_table
      AND column_name = '_fivetran_synced'
  ) THEN
    RAISE USING MESSAGE = FORMAT("Column '_fivetran_synced' is missing from %s", source_table);
  END IF;

  -- Run deduplication query
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `%s` AS
    SELECT *
    FROM (
      SELECT *, 
             ROW_NUMBER() OVER (
               PARTITION BY %s 
               ORDER BY updated_at DESC, _fivetran_synced DESC
             ) AS dedupe
      FROM `%s`
    )
    WHERE dedupe = 1
  """, target_full, id_field, source_full);

END;


-- CREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_tiktok.dedupe_table_by_primary_id`(
--   IN source_table STRING
-- )
 
-- BEGIN

--   -- Derive ID field name (e.g., "adgroup_history" → "adgroup_id")
--   DECLARE id_field STRING;
--   DECLARE source_full STRING;
--   DECLARE target_full STRING;

--   SET id_field = FORMAT('%s_id', SPLIT(source_table, '_')[OFFSET(0)]);
--   SET source_full = FORMAT("giant-spoon-299605.tiktok_ads.%s", source_table);
--   SET target_full = FORMAT("looker-studio-pro-452620.repo_tiktok.stg__%s_deduped", source_table);

--   -- Execute dynamic SQL
--   EXECUTE IMMEDIATE FORMAT("""
--     CREATE OR REPLACE TABLE `%s` AS
--     SELECT *
--     FROM (
--       SELECT *, 
--              ROW_NUMBER() OVER (
--                PARTITION BY %s 
--                ORDER BY updated_at DESC, _fivetran_synced DESC
--              ) AS dedupe
--       FROM `%s`
--     )
--     WHERE dedupe = 1
--   """, target_full, id_field, source_full);
-- -- EXAMPLE CALLS
--   -- Dedupe adgroup_history
--     -- CALL `looker-studio-pro-452620.repo_tiktok.dedupe_table_by_primary_id`('adgroup_history');

--   -- Dedupe campaign_history
--     --CALL `looker-studio-pro-452620.repo_tiktok.dedupe_table_by_primary_id`('campaign_history');
-- END;