/*******************************************************************************
DETAILED VALIDATION: Compare Original vs New View with Source Breakdown
********************************************************************************
Purpose: Compare metrics from original staging view vs new integrated view
         Breakdown by data source: DCM, Original FPD, Updated FPD

Shows:
- Impressions and spend totals for each data source
- Original view totals (adif__prisma_expanded_plus_dcm_view_v3_test)
- New view totals (with updated FPD integration)
- Delta (change from original to new)

Last Updated: 2026-01-23
*******************************************************************************/

WITH

-- Original view metrics by source
original_view_metrics AS (
  SELECT
    'adif__prisma_expanded_plus_dcm_view_v3_test' AS view_name,
    'DCM' AS data_source,
    COUNT(*) AS row_count,
    SUM(d_daily_recalculated_imps) AS impressions,
    SUM(d_daily_recalculated_cost) AS spend
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
  WHERE d_daily_recalculated_imps IS NOT NULL OR d_daily_recalculated_cost IS NOT NULL

  UNION ALL

  SELECT
    'adif__prisma_expanded_plus_dcm_view_v3_test' AS view_name,
    'Original FPD' AS data_source,
    COUNT(*) AS row_count,
    SUM(fpd_impressions) AS impressions,
    SUM(fpd_spend) AS spend
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
  WHERE fpd_impressions IS NOT NULL OR fpd_spend IS NOT NULL

  UNION ALL

  SELECT
    'adif__prisma_expanded_plus_dcm_view_v3_test' AS view_name,
    'Updated FPD' AS data_source,
    0 AS row_count,
    0 AS impressions,
    0 AS spend

  UNION ALL

  SELECT
    'adif__prisma_expanded_plus_dcm_view_v3_test' AS view_name,
    'TOTAL (final_*)' AS data_source,
    COUNT(*) AS row_count,
    SUM(final_impressions) AS impressions,
    SUM(final_spend) AS spend
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
),

-- New view with updated FPD integration
new_view AS (
  WITH
  existing_view AS (
    SELECT *
    FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
  ),

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

  combined AS (
    SELECT
      e.*,
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

  final_with_updated_priority AS (
    SELECT
      *,
      COALESCE(updated_fpd_impressions, fpd_impressions, d_daily_recalculated_imps) AS final_impressions_new,
      COALESCE(updated_fpd_spend, fpd_spend, d_daily_recalculated_cost) AS final_spend_new,
      CASE
        WHEN updated_fpd_impressions IS NOT NULL OR updated_fpd_spend IS NOT NULL THEN 'updated_fpd'
        WHEN fpd_impressions IS NOT NULL OR fpd_spend IS NOT NULL THEN 'original_fpd'
        WHEN d_daily_recalculated_imps IS NOT NULL OR d_daily_recalculated_cost IS NOT NULL THEN 'dcm'
        ELSE 'planned_only'
      END AS data_source_primary
    FROM combined
  ),

  pkg_rollups_updated AS (
    SELECT
      package_id_joined,
      SUM(final_impressions_new) AS pkg_act_imps_new,
      SUM(final_spend_new) AS pkg_act_spend_new,
      SUM(updated_fpd_impressions) AS pkg_updated_fpd_impressions,
      SUM(updated_fpd_spend) AS pkg_updated_fpd_spend
    FROM final_with_updated_priority
    GROUP BY package_id_joined
  )

  SELECT
    f.* EXCEPT(final_impressions, final_spend, pkg_act_imps, pkg_act_spend),
    f.final_impressions_new AS final_impressions,
    f.final_spend_new AS final_spend,
    p.pkg_act_imps_new AS pkg_act_imps,
    p.pkg_act_spend_new AS pkg_act_spend,
    p.pkg_updated_fpd_impressions,
    p.pkg_updated_fpd_spend
  FROM final_with_updated_priority f
  LEFT JOIN pkg_rollups_updated p
    ON f.package_id_joined = p.package_id_joined
),

-- New view metrics by source
new_view_metrics AS (
  SELECT
    'adif__prisma_expanded_plus_dcm_updated_fpd_view (NEW)' AS view_name,
    'DCM' AS data_source,
    COUNT(*) AS row_count,
    SUM(d_daily_recalculated_imps) AS impressions,
    SUM(d_daily_recalculated_cost) AS spend
  FROM new_view
  WHERE d_daily_recalculated_imps IS NOT NULL OR d_daily_recalculated_cost IS NOT NULL

  UNION ALL

  SELECT
    'adif__prisma_expanded_plus_dcm_updated_fpd_view (NEW)' AS view_name,
    'Original FPD' AS data_source,
    COUNT(*) AS row_count,
    SUM(fpd_impressions) AS impressions,
    SUM(fpd_spend) AS spend
  FROM new_view
  WHERE fpd_impressions IS NOT NULL OR fpd_spend IS NOT NULL

  UNION ALL

  SELECT
    'adif__prisma_expanded_plus_dcm_updated_fpd_view (NEW)' AS view_name,
    'Updated FPD' AS data_source,
    COUNT(*) AS row_count,
    SUM(updated_fpd_impressions) AS impressions,
    SUM(updated_fpd_spend) AS spend
  FROM new_view
  WHERE updated_fpd_impressions IS NOT NULL OR updated_fpd_spend IS NOT NULL

  UNION ALL

  SELECT
    'adif__prisma_expanded_plus_dcm_updated_fpd_view (NEW)' AS view_name,
    'TOTAL (final_*)' AS data_source,
    COUNT(*) AS row_count,
    SUM(final_impressions) AS impressions,
    SUM(final_spend) AS spend
  FROM new_view
),

-- Combine both views for comparison
combined_metrics AS (
  SELECT * FROM original_view_metrics
  UNION ALL
  SELECT * FROM new_view_metrics
),

-- Calculate deltas
delta_metrics AS (
  SELECT
    'DELTA (New - Original)' AS view_name,
    n.data_source,
    n.row_count - o.row_count AS row_count,
    n.impressions - o.impressions AS impressions,
    n.spend - o.spend AS spend
  FROM new_view_metrics n
  INNER JOIN original_view_metrics o
    ON n.data_source = o.data_source
)

-- Final output: Original + New + Delta
SELECT
  view_name,
  data_source,
  FORMAT("%'d", row_count) AS row_count,
  FORMAT("%'0.0f", IFNULL(impressions, 0)) AS impressions,
  FORMAT("$%'0.2f", IFNULL(spend, 0)) AS spend
FROM combined_metrics

UNION ALL

SELECT
  view_name,
  data_source,
  FORMAT("%'+d", row_count) AS row_count,
  FORMAT("%'+0.0f", IFNULL(impressions, 0)) AS impressions,
  FORMAT("$%'+0.2f", IFNULL(spend, 0)) AS spend
FROM delta_metrics

ORDER BY
  CASE
    WHEN view_name = 'adif__prisma_expanded_plus_dcm_view_v3_test' THEN 1
    WHEN view_name = 'adif__prisma_expanded_plus_dcm_updated_fpd_view (NEW)' THEN 2
    WHEN view_name = 'DELTA (New - Original)' THEN 3
  END,
  CASE
    WHEN data_source = 'DCM' THEN 1
    WHEN data_source = 'Original FPD' THEN 2
    WHEN data_source = 'Updated FPD' THEN 3
    WHEN data_source = 'TOTAL (final_*)' THEN 4
  END;
