/*******************************************************************************
DEPLOYMENT SCRIPT: Create Updated FPD Integration View
********************************************************************************
Purpose: Deploy the new staging view with updated FPD integration
Options:
  1. Create as NEW view for testing (recommended first)
  2. Replace existing view after validation

Usage:
  Step 1: Deploy as new view for testing
    bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false < deploy_updated_fpd_view.sql

  Step 2: After validation, uncomment the DROP/REPLACE section at bottom

Last Updated: 2026-01-23
*******************************************************************************/

-- CREATE NEW VIEW FOR TESTING (recommended first step)
CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view` AS

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
)

-- FINAL OUTPUT: All original columns + new columns + updated final_* columns
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
ORDER BY package_id_joined, date;

/*******************************************************************************
POST-DEPLOYMENT VALIDATION
********************************************************************************
Run these queries after deployment to verify the view works correctly:

-- 1. Check row count (should be 11,418)
SELECT COUNT(*) AS row_count FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`;

-- 2. Check data source distribution
SELECT
  data_source_primary,
  COUNT(*) AS row_count,
  SUM(final_impressions) AS total_impressions,
  SUM(final_spend) AS total_spend
FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`
GROUP BY data_source_primary
ORDER BY total_spend DESC;

-- 3. Verify updated FPD coverage
SELECT
  COUNT(*) AS rows_with_updated_fpd,
  COUNT(DISTINCT package_id_joined) AS packages_with_updated_fpd,
  SUM(updated_fpd_impressions) AS total_updated_fpd_imps,
  SUM(updated_fpd_spend) AS total_updated_fpd_spend
FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`
WHERE updated_fpd_impressions IS NOT NULL OR updated_fpd_spend IS NOT NULL;

-- 4. Compare with original view totals
WITH original AS (
  SELECT
    SUM(final_impressions) AS orig_final_imps,
    SUM(final_spend) AS orig_final_spend
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
),
updated AS (
  SELECT
    SUM(final_impressions) AS new_final_imps,
    SUM(final_spend) AS new_final_spend
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`
)
SELECT
  orig_final_imps,
  new_final_imps,
  new_final_imps - orig_final_imps AS imps_change,
  orig_final_spend,
  new_final_spend,
  new_final_spend - orig_final_spend AS spend_change
FROM original, updated;

*******************************************************************************/

/*******************************************************************************
OPTIONAL: REPLACE EXISTING VIEW
********************************************************************************
After validation, uncomment and run this block to replace the original view:

CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test` AS

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
ORDER BY package_id_joined, date;

*******************************************************************************/
