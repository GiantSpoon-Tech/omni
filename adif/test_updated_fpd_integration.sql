-- Test query to verify the updated FPD integration SQL works correctly
-- This runs the full logic but limits output for testing

WITH

-- STEP 1: Get existing staging view data (ALL existing columns preserved)
existing_view AS (
  SELECT *
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
),

-- STEP 2: Get updated FPD data (aggregated by package_id + date)
updated_fpd AS (
  SELECT
    package_id,
    date,
    SUM(daily_fpd_impressions) AS updated_fpd_impressions,
    SUM(daily_fpd_spend) AS updated_fpd_spend,
    STRING_AGG(DISTINCT supplier_name, ', ' ORDER BY supplier_name) AS updated_fpd_suppliers,
    STRING_AGG(DISTINCT initiative, ', ' ORDER BY initiative) AS updated_fpd_initiatives,
    MAX(data_update_datetime) AS updated_fpd_data_timestamp
  FROM `looker-studio-pro-452620.landing.adif_updated_fpd_daily`
  GROUP BY package_id, date
),

-- STEP 3: Join existing view with updated FPD
combined AS (
  SELECT
    e.*,
    -- Add updated FPD columns
    u.updated_fpd_impressions,
    u.updated_fpd_spend,
    u.updated_fpd_suppliers,
    u.updated_fpd_initiatives,
    u.updated_fpd_data_timestamp
  FROM existing_view e
  LEFT JOIN updated_fpd u
    ON e.package_id_joined = u.package_id
   AND e.date = u.date
),

-- STEP 4: Recalculate final_* columns with updated FPD priority
final_with_updated_priority AS (
  SELECT
    *,

    -- Recalculated final_impressions: Updated FPD → Original FPD → DCM
    COALESCE(
      updated_fpd_impressions,
      fpd_impressions,
      d_daily_recalculated_imps
    ) AS final_impressions_new,

    -- Recalculated final_spend: Updated FPD → Original FPD → DCM
    COALESCE(
      updated_fpd_spend,
      fpd_spend,
      d_daily_recalculated_cost
    ) AS final_spend_new,

    -- Data source indicator (for transparency)
    CASE
      WHEN updated_fpd_impressions IS NOT NULL OR updated_fpd_spend IS NOT NULL THEN 'updated_fpd'
      WHEN fpd_impressions IS NOT NULL OR fpd_spend IS NOT NULL THEN 'original_fpd'
      WHEN d_daily_recalculated_imps IS NOT NULL OR d_daily_recalculated_cost IS NOT NULL THEN 'dcm'
      ELSE 'planned_only'
    END AS data_source_primary

  FROM combined
),

-- STEP 5: Recalculate package-level rollups with updated FPD
pkg_rollups_updated AS (
  SELECT
    package_id_joined,
    SUM(final_impressions_new) AS pkg_act_imps_new,
    SUM(final_spend_new) AS pkg_act_spend_new,
    SUM(updated_fpd_impressions) AS pkg_updated_fpd_impressions,
    SUM(updated_fpd_spend) AS pkg_updated_fpd_spend
  FROM final_with_updated_priority
  GROUP BY package_id_joined
),

-- FINAL OUTPUT
final_output AS (
  SELECT
    f.* EXCEPT(final_impressions, final_spend, pkg_act_imps, pkg_act_spend),

    -- Replace final_* columns with recalculated values
    f.final_impressions_new AS final_impressions,
    f.final_spend_new AS final_spend,

    -- Replace package rollup columns with recalculated values
    p.pkg_act_imps_new AS pkg_act_imps,
    p.pkg_act_spend_new AS pkg_act_spend,

    -- Add new package-level summary columns for updated FPD
    p.pkg_updated_fpd_impressions,
    p.pkg_updated_fpd_spend

  FROM final_with_updated_priority f
  LEFT JOIN pkg_rollups_updated p
    ON f.package_id_joined = p.package_id_joined
)

-- Test queries
SELECT
  '=== TEST 1: Basic Statistics ===' AS test_section,
  'Total rows' AS metric_name,
  CAST(COUNT(*) AS STRING) AS metric_value
FROM final_output

UNION ALL

SELECT
  '=== TEST 1: Basic Statistics ===' AS test_section,
  'Unique packages' AS metric_name,
  CAST(COUNT(DISTINCT package_id_joined) AS STRING) AS metric_value
FROM final_output

UNION ALL

SELECT
  '=== TEST 2: Data Source Distribution ===' AS test_section,
  data_source_primary AS metric_name,
  CAST(COUNT(*) AS STRING) AS metric_value
FROM final_output
GROUP BY data_source_primary

UNION ALL

SELECT
  '=== TEST 3: Updated FPD Coverage ===' AS test_section,
  'Rows with updated FPD' AS metric_name,
  CAST(COUNT(*) AS STRING) AS metric_value
FROM final_output
WHERE updated_fpd_impressions IS NOT NULL OR updated_fpd_spend IS NOT NULL

UNION ALL

SELECT
  '=== TEST 4: Metric Totals ===' AS test_section,
  'Total final_impressions' AS metric_name,
  FORMAT("%'.0f", SUM(final_impressions)) AS metric_value
FROM final_output

UNION ALL

SELECT
  '=== TEST 4: Metric Totals ===' AS test_section,
  'Total final_spend' AS metric_name,
  FORMAT("$%'.2f", SUM(final_spend)) AS metric_value
FROM final_output

UNION ALL

SELECT
  '=== TEST 4: Metric Totals ===' AS test_section,
  'Total updated_fpd_impressions' AS metric_name,
  FORMAT("%'.0f", IFNULL(SUM(updated_fpd_impressions), 0)) AS metric_value
FROM final_output

UNION ALL

SELECT
  '=== TEST 4: Metric Totals ===' AS test_section,
  'Total updated_fpd_spend' AS metric_name,
  FORMAT("$%'.2f", IFNULL(SUM(updated_fpd_spend), 0)) AS metric_value
FROM final_output

ORDER BY test_section, metric_name;
