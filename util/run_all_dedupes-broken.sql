anchor_clicksCREATE OR REPLACE PROCEDURE `looker-studio-pro-452620.repo_tiktok.run_all_dedupes`()
BEGIN
  /*
  --------------------------------------------------------------------------------
  🔁 Stored Procedure: run_all_dedupes

  📌 Description:
     Loops through all tables in the `repo_tiktok` dataset ending with `_history`,
     and for each one, calls `dedupe_table_by_primary_id`.
     Source data comes from `giant-spoon-299605.tiktok_ads`.

  🧪 Example:
     CALL `looker-studio-pro-452620.repo_tiktok.run_all_dedupes`();
  --------------------------------------------------------------------------------
  */

  -- Use FOR-IN to iterate directly over results
  FOR record IN (
    SELECT table_name
    FROM `giant-spoon-299605.tiktok_ads.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE '%_history'
  )
  DO
    EXECUTE IMMEDIATE FORMAT("""
      CALL `looker-studio-pro-452620.repo_tiktok.dedupe_table_by_primary_id`('%s')
    """, record.table_name);
  END FOR;

END;