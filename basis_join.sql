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
 