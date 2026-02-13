/*******************************************************************************
STAGING VIEW: ADIF with Updated FPD Integration
********************************************************************************
Purpose: Extends the existing adif__prisma_expanded_plus_dcm_view_v3_test
         staging view to include updated FPD data from the new Google Sheet

Data Sources:
1. DCM delivery data (d_* columns)
2. Original FPD from partner sheets (fpd_* columns)
3. Prisma planning data (planned_* columns)
4. **NEW** Updated FPD from package-level sheet (updated_fpd_* columns)

Priority Hierarchy (final_* columns):
1. Updated FPD (highest priority - most recent partner data)
2. Original FPD (second priority)
3. DCM (third priority)
4. NULL

Usage: Replace existing staging view or create as new view
       `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`

Last Updated: 2026-01-23
*******************************************************************************/

-- UPDATED FPD DATA (new source)
WITH updated_fpd AS (
  SELECT
    package_id,
    date,
    SUM(daily_fpd_impressions) AS updated_fpd_impressions,
    SUM(daily_fpd_spend) AS updated_fpd_spend,
    STRING_AGG(DISTINCT initiative, ', ' ORDER BY initiative) AS updated_fpd_initiatives,
    STRING_AGG(DISTINCT supplier_name, ', ' ORDER BY supplier_name) AS updated_fpd_suppliers
  FROM `looker-studio-pro-452620.landing.adif_updated_fpd_daily`
  GROUP BY package_id, date
),

-- DCM NORMALIZATION (from existing view)
dcm_normalized AS (
  SELECT
    CASE
      WHEN package_id = 'P3923KK' THEN 'P37K96P'
      WHEN package_id = 'P37K96T' THEN 'P37K96P'
      WHEN package_id = 'P37MLHQ' THEN 'P37K96P'
      WHEN package_id = 'P37MLHP' THEN 'P37K96P'
      WHEN package_id = 'P37K96S' THEN 'P37K96P'
      WHEN package_id = 'P37DSDJ' THEN 'P37DSDR'
      ELSE package_id
    END AS package_id,
    flight_status_flag,
    DATE(date) AS date,
    SUM(impressions) AS d_impressions,
    SUM(clicks) AS d_clicks,
    SUM(media_cost) AS d_media_cost,
    SUM(rich_media_video_plays) AS d_video_plays,
    SUM(rich_media_video_completions) AS d_video_completions
  FROM `looker-studio-pro-452620.DCM.20250505_costModel_v5`
  GROUP BY 1, 2, 3
),

-- ORIGINAL FPD (from existing adif_fpd_data_ranged)
fpd_original AS (
  SELECT
    CASE
      WHEN package_id = 'P3923KK' THEN 'P37K96P'
      WHEN package_id = 'P37K96T' THEN 'P37K96P'
      WHEN package_id = 'P37MLHQ' THEN 'P37K96P'
      WHEN package_id = 'P37MLHP' THEN 'P37K96P'
      WHEN package_id = 'P37K96S' THEN 'P37K96P'
      WHEN package_id = 'P37DSDJ' THEN 'P37DSDR'
      ELSE package_id
    END AS package_id,
    DATE(date_final) AS date,
    SUM(impressions) AS fpd_orig_impressions,
    SUM(spend) AS fpd_orig_spend,
    SUM(clicks) AS fpd_orig_clicks,
    SUM(sends) AS fpd_orig_sends,
    SUM(opens) AS fpd_orig_opens,
    STRING_AGG(DISTINCT partner_creative_name, ', ' ORDER BY partner_creative_name) AS fpd_orig_creative
  FROM `looker-studio-pro-452620.landing.adif_fpd_data_ranged`
  WHERE package_id IS NOT NULL
  GROUP BY 1, 2
),

-- PRISMA PLANNING (from existing source)
prisma AS (
  SELECT
    package_id,
    DATE(date) AS date,
    SUM(planned_daily_spend_pk) AS planned_daily_spend,
    SUM(planned_daily_impressions_pk) AS planned_daily_impressions
  FROM `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
  WHERE advertiser_name = 'Forevermark US'
    AND package_type != 'Child'
  GROUP BY package_id, date
),

-- MULTI-SOURCE JOIN: DCM + Updated FPD
dcm_updated_fpd AS (
  SELECT
    COALESCE(d.package_id, u.package_id) AS package_id_joined,
    COALESCE(d.date, u.date) AS date,
    d.* EXCEPT(package_id, date),
    u.* EXCEPT(package_id, date)
  FROM dcm_normalized AS d
  FULL OUTER JOIN updated_fpd AS u
    ON d.package_id = u.package_id
   AND d.date = u.date
),

-- MULTI-SOURCE JOIN: (DCM + Updated FPD) + Original FPD
dcm_all_fpd AS (
  SELECT
    COALESCE(du.package_id_joined, f.package_id) AS package_id_joined,
    COALESCE(du.date, f.date) AS date,
    du.* EXCEPT(package_id_joined, date),
    f.* EXCEPT(package_id, date)
  FROM dcm_updated_fpd AS du
  FULL OUTER JOIN fpd_original AS f
    ON du.package_id_joined = f.package_id
   AND du.date = f.date
),

-- FINAL JOIN: (DCM + All FPD) + Prisma
combined AS (
  SELECT
    COALESCE(df.package_id_joined, p.package_id) AS package_id_joined,
    COALESCE(df.date, p.date) AS date,
    df.* EXCEPT(package_id_joined, date),
    p.* EXCEPT(package_id, date)
  FROM dcm_all_fpd AS df
  FULL OUTER JOIN prisma AS p
    ON df.package_id_joined = p.package_id
   AND df.date = p.date
),

-- CALCULATE FINAL METRICS WITH UPDATED PRIORITY
final AS (
  SELECT
    *,
    -- Final impressions: Updated FPD → Original FPD → DCM
    COALESCE(
      updated_fpd_impressions,
      fpd_orig_impressions,
      d_impressions
    ) AS final_impressions,

    -- Final spend: Updated FPD → Original FPD → DCM
    COALESCE(
      updated_fpd_spend,
      fpd_orig_spend,
      d_media_cost
    ) AS final_spend,

    -- Final clicks: Original FPD → DCM (updated FPD doesn't have clicks)
    COALESCE(
      fpd_orig_clicks,
      d_clicks
    ) AS final_clicks,

    -- Data source indicator (for transparency)
    CASE
      WHEN updated_fpd_impressions IS NOT NULL OR updated_fpd_spend IS NOT NULL THEN 'updated_fpd'
      WHEN fpd_orig_impressions IS NOT NULL OR fpd_orig_spend IS NOT NULL THEN 'original_fpd'
      WHEN d_impressions IS NOT NULL OR d_media_cost IS NOT NULL THEN 'dcm'
      ELSE 'planned_only'
    END AS data_source_primary
  FROM combined
),

-- PACKAGE-LEVEL ROLLUPS
pkg_totals AS (
  SELECT
    package_id_joined,
    SUM(final_impressions) AS pkg_total_impressions,
    SUM(final_spend) AS pkg_total_spend,
    SUM(updated_fpd_impressions) AS pkg_updated_fpd_impressions,
    SUM(updated_fpd_spend) AS pkg_updated_fpd_spend,
    SUM(fpd_orig_impressions) AS pkg_orig_fpd_impressions,
    SUM(fpd_orig_spend) AS pkg_orig_fpd_spend,
    SUM(d_impressions) AS pkg_dcm_impressions,
    SUM(d_media_cost) AS pkg_dcm_spend
  FROM final
  GROUP BY package_id_joined
)

-- FINAL OUTPUT
SELECT
  f.*,
  p.pkg_total_impressions,
  p.pkg_total_spend,
  p.pkg_updated_fpd_impressions,
  p.pkg_updated_fpd_spend,
  p.pkg_orig_fpd_impressions,
  p.pkg_orig_fpd_spend,
  p.pkg_dcm_impressions,
  p.pkg_dcm_spend
FROM final f
LEFT JOIN pkg_totals p
  ON f.package_id_joined = p.package_id_joined
ORDER BY package_id_joined, date;

/*******************************************************************************
VALIDATION QUERIES
********************************************************************************/

-- 1. Check data source distribution
-- SELECT
--   data_source_primary,
--   COUNT(*) AS row_count,
--   SUM(final_impressions) AS total_impressions,
--   SUM(final_spend) AS total_spend
-- FROM `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`
-- GROUP BY data_source_primary
-- ORDER BY total_spend DESC;

-- 2. Compare updated FPD vs original FPD for overlapping packages
-- SELECT
--   package_id_joined,
--   COUNT(*) AS days,
--   SUM(updated_fpd_impressions) AS updated_imps,
--   SUM(fpd_orig_impressions) AS orig_imps,
--   SUM(updated_fpd_spend) AS updated_spend,
--   SUM(fpd_orig_spend) AS orig_spend
-- FROM `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`
-- WHERE updated_fpd_impressions IS NOT NULL
--    OR fpd_orig_impressions IS NOT NULL
-- GROUP BY package_id_joined
-- HAVING SUM(updated_fpd_impressions) > 0 OR SUM(fpd_orig_impressions) > 0
-- ORDER BY updated_spend DESC;

-- 3. Verify no data loss - package totals should match source tables
-- SELECT
--   'updated_fpd_source' AS source,
--   SUM(daily_fpd_impressions) AS impressions,
--   SUM(daily_fpd_spend) AS spend
-- FROM `landing.adif_updated_fpd_daily`
-- UNION ALL
-- SELECT
--   'staging_view' AS source,
--   SUM(updated_fpd_impressions) AS impressions,
--   SUM(updated_fpd_spend) AS spend
-- FROM `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`;
