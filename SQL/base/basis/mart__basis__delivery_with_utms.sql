-- @file: mart__basis__delivery_with_utms.sql
-- @layer: marts
-- @description: Joins Basis delivery data with parsed UTM parameters to produce an enriched
--               delivery table for reporting. Generates a join key (`a_id`) from the placement
--               name or fallback logic, and enriches with UTM metadata.
-- @source: giant-spoon-299605.data_model_2025.basis_master2
-- @source: looker-studio-pro-452620.20250327_data_model.basis_utms_stg
-- @target: looker-studio-pro-452620.repo_tables.basis

create or replace table `looker-studio-pro-452620.repo_tables.basis` as
WITH
  a AS (
  SELECT
    *,
    IFNULL(REGEXP_EXTRACT(placement, r'CP_(\d+)'),CONCAT(placement," - ", creative_name)) AS a_id,
  FROM
    giant-spoon-299605.data_model_2025.basis_master2 )
SELECT
  *,
FROM
  a
LEFT JOIN
  `looker-studio-pro-452620.20250327_data_model.basis_utms_stg` b
ON
  a.a_id = b.id
ORDER BY
  utm_medium DESC
NULLS LAST
 