/*******************************************************************************
STAGING VIEW: ADIF with Updated FPD Integration v3 (Combined FPD Metrics)
********************************************************************************
Purpose: Extends the existing staging view with updated FPD data
         FPD metrics show COMBINED totals (original + updated)

Schema Changes:
- fpd_impressions = fpd_orig_impressions + fpd_updated_impressions (COMBINED)
- fpd_spend = fpd_orig_spend + fpd_updated_spend (COMBINED)
- fpd_clicks = fpd_orig_clicks (no updated FPD clicks available)
- NEW: fpd_orig_impressions, fpd_orig_spend, fpd_orig_clicks (original FPD only)
- NEW: fpd_updated_impressions, fpd_updated_spend (updated FPD only)

Priority for final_* columns:
- Combined FPD → DCM → Planned

Last Updated: 2026-01-23
*******************************************************************************/

WITH

-- STEP 1: Get existing staging view data
existing_view AS (
  SELECT *
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
),

-- STEP 2: Get updated FPD data
updated_fpd AS (
  SELECT
    package_id,
    date,
    SUM(daily_fpd_impressions) AS fpd_updated_impressions,
    SUM(daily_fpd_spend) AS fpd_updated_spend,
    STRING_AGG(DISTINCT supplier_name, ', ' ORDER BY supplier_name) AS fpd_updated_suppliers,
    STRING_AGG(DISTINCT initiative, ', ' ORDER BY initiative) AS fpd_updated_initiatives,
    MAX(data_update_datetime) AS fpd_updated_data_timestamp
  FROM `looker-studio-pro-452620.landing.adif_updated_fpd_daily`
  GROUP BY package_id, date
),

-- STEP 3: Join and rename original FPD columns
combined AS (
  SELECT
    e.* EXCEPT(fpd_impressions, fpd_spend, fpd_clicks, fpd_sends, fpd_opens, fpd_benchmark, fpd_benchmark_metric, fpd_creative),

    -- Rename original FPD columns to fpd_orig_*
    e.fpd_impressions AS fpd_orig_impressions,
    e.fpd_spend AS fpd_orig_spend,
    e.fpd_clicks AS fpd_orig_clicks,
    e.fpd_sends AS fpd_orig_sends,
    e.fpd_opens AS fpd_orig_opens,
    e.fpd_benchmark AS fpd_orig_benchmark,
    e.fpd_benchmark_metric AS fpd_orig_benchmark_metric,
    e.fpd_creative AS fpd_orig_creative,

    -- Add updated FPD columns (renamed from updated_fpd_* to fpd_updated_*)
    u.fpd_updated_impressions,
    u.fpd_updated_spend,
    u.fpd_updated_suppliers,
    u.fpd_updated_initiatives,
    u.fpd_updated_data_timestamp,

    -- Create COMBINED FPD columns (sum of original + updated)
    COALESCE(e.fpd_impressions, 0) + COALESCE(u.fpd_updated_impressions, 0) AS fpd_impressions,
    COALESCE(e.fpd_spend, 0) + COALESCE(u.fpd_updated_spend, 0) AS fpd_spend,
    e.fpd_clicks AS fpd_clicks,  -- No updated FPD clicks, so use original
    e.fpd_sends AS fpd_sends,     -- No updated FPD sends
    e.fpd_opens AS fpd_opens,     -- No updated FPD opens
    e.fpd_benchmark AS fpd_benchmark,
    e.fpd_benchmark_metric AS fpd_benchmark_metric,
    e.fpd_creative AS fpd_creative

  FROM existing_view e
  LEFT JOIN updated_fpd u
    ON e.package_id_joined = u.package_id
   AND e.date = u.date
),

-- STEP 4: Recalculate final_* columns with combined FPD priority
final_with_combined_fpd AS (
  SELECT
    *,

    -- Recalculated final_impressions: Combined FPD → DCM
    COALESCE(
      NULLIF(fpd_impressions, 0),  -- Use combined FPD (original + updated)
      d_daily_recalculated_imps
    ) AS final_impressions_new,

    -- Recalculated final_spend: Combined FPD → DCM
    COALESCE(
      NULLIF(fpd_spend, 0),  -- Use combined FPD (original + updated)
      d_daily_recalculated_cost
    ) AS final_spend_new,

    -- final_clicks stays the same (no updated FPD clicks)
    -- Keep original: COALESCE(fpd_clicks, d_clicks)

    -- Data source indicator
    CASE
      WHEN fpd_updated_impressions IS NOT NULL OR fpd_updated_spend IS NOT NULL THEN
        CASE
          WHEN fpd_orig_impressions IS NOT NULL OR fpd_orig_spend IS NOT NULL THEN 'fpd_combined'
          ELSE 'fpd_updated_only'
        END
      WHEN fpd_orig_impressions IS NOT NULL OR fpd_orig_spend IS NOT NULL THEN 'fpd_original_only'
      WHEN d_daily_recalculated_imps IS NOT NULL OR d_daily_recalculated_cost IS NOT NULL THEN 'dcm'
      ELSE 'planned_only'
    END AS data_source_primary

  FROM combined
),

-- STEP 5: Recalculate package-level rollups
pkg_rollups_updated AS (
  SELECT
    package_id_joined,
    SUM(final_impressions_new) AS pkg_act_imps_new,
    SUM(final_spend_new) AS pkg_act_spend_new,
    SUM(fpd_orig_impressions) AS pkg_fpd_orig_impressions,
    SUM(fpd_orig_spend) AS pkg_fpd_orig_spend,
    SUM(fpd_updated_impressions) AS pkg_fpd_updated_impressions,
    SUM(fpd_updated_spend) AS pkg_fpd_updated_spend,
    SUM(fpd_impressions) AS pkg_fpd_combined_impressions,
    SUM(fpd_spend) AS pkg_fpd_combined_spend
  FROM final_with_combined_fpd
  GROUP BY package_id_joined
)

-- FINAL OUTPUT
SELECT
  f.* EXCEPT(final_impressions, final_spend, pkg_act_imps, pkg_act_spend),

  -- Replace final_* columns with recalculated values
  f.final_impressions_new AS final_impressions,
  f.final_spend_new AS final_spend,

  -- Replace package rollup columns
  p.pkg_act_imps_new AS pkg_act_imps,
  p.pkg_act_spend_new AS pkg_act_spend,

  -- Add package-level FPD breakdown columns
  p.pkg_fpd_orig_impressions,
  p.pkg_fpd_orig_spend,
  p.pkg_fpd_updated_impressions,
  p.pkg_fpd_updated_spend,
  p.pkg_fpd_combined_impressions,
  p.pkg_fpd_combined_spend

FROM final_with_combined_fpd f
LEFT JOIN pkg_rollups_updated p
  ON f.package_id_joined = p.package_id_joined
ORDER BY package_id_joined, date;

/*******************************************************************************
COLUMN MAPPING SUMMARY
********************************************************************************

ORIGINAL FPD (from partner sheets):
- fpd_orig_impressions (was: fpd_impressions)
- fpd_orig_spend (was: fpd_spend)
- fpd_orig_clicks (was: fpd_clicks)
- fpd_orig_sends, fpd_orig_opens, fpd_orig_benchmark, etc.

UPDATED FPD (from new Google Sheet):
- fpd_updated_impressions (was: updated_fpd_impressions)
- fpd_updated_spend (was: updated_fpd_spend)
- fpd_updated_suppliers, fpd_updated_initiatives, fpd_updated_data_timestamp

COMBINED FPD (sum of both sources):
- fpd_impressions = fpd_orig_impressions + fpd_updated_impressions
- fpd_spend = fpd_orig_spend + fpd_updated_spend
- fpd_clicks = fpd_orig_clicks (no updated clicks available)

FINAL METRICS (priority: Combined FPD → DCM):
- final_impressions = COALESCE(fpd_impressions, d_daily_recalculated_imps)
- final_spend = COALESCE(fpd_spend, d_daily_recalculated_cost)
- final_clicks = COALESCE(fpd_clicks, d_clicks)

PACKAGE ROLLUPS:
- pkg_fpd_orig_impressions, pkg_fpd_orig_spend
- pkg_fpd_updated_impressions, pkg_fpd_updated_spend
- pkg_fpd_combined_impressions, pkg_fpd_combined_spend

*******************************************************************************/
