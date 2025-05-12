-- @file: test__costmodel__cost_underdelivery_unexplained_by_impressions.sql
-- @layer: tests
-- @description: Flags ended packages where total recalculated cost is significantly below planned cost,
--               but the discrepancy is not explained by underdelivery of impressions.
--               This may indicate billing gaps, rate misapplication, or data loss.
-- @source: 
-- @target: QA diagnostic (non-materialized)

-- --AI refactored version
  --   SELECT
  --     package_id,
  --     p_package_friendly,
  --     flight_status_flag,
      
  --     -- Total recalculated cost from daily delivery-level model
  --     SUM(daily_recalculated_cost) AS total_recalculated_cost,

  --     -- Package-level metadata for planned values
  --     MAX(p_pkg_total_planned_cost) AS planned_cost_pk,
  --     MAX(p_cost_method) AS cost_method,
  --     MAX(p_advertiser_name) AS advertiser,

  --     -- Impression comparison
  --     MAX(total_inflight_impressions) AS total_recalculated_imps,
  --     MAX(p_pkg_total_planned_imps) AS planned_imps_pk,

  --     -- Percent differences
  --     ROUND(SAFE_DIVIDE(MAX(total_inflight_impressions), MAX(p_pkg_total_planned_imps)), 2) AS imps_diff_pct,
  --     ROUND(SAFE_DIVIDE(SUM(daily_recalculated_cost), MAX(p_pkg_total_planned_cost)), 2) AS cost_diff_pct,

  --     -- Raw cost delta (rounded to 2 decimal places)
  --     ROUND(ABS(ROUND(SUM(daily_recalculated_cost)) - MAX(p_pkg_total_planned_cost)), 2) AS cost_diff

  --   FROM
  --     `looker-studio-pro-452620.DCM.20250505_costModel_v5`

  --   WHERE
  --     flight_status_flag = 'ended'
  --     -- AND placement_id = 'P2T86SL' -- Optional filter for debugging

  --   GROUP BY
  --     package_id,
  --     p_package_friendly,
  --     flight_status_flag

  --   HAVING
  --     cost_diff_pct > 0.1

  --   ORDER BY
  --     cost_diff DESC;

-- Original version
with a as(
  SELECT
      package_id,
    p_package_friendly,
    flight_status_flag,
    --daily_recalculated_cost_flag,
    SUM(daily_recalculated_cost) AS total_recalculated_cost,
    MAX(p_pkg_total_planned_cost) AS planned_cost_pk,
    MAX(p_cost_method) AS cost_method,
    MAX(p_advertiser_name) AS advertiser,
    max(total_inflight_impressions) AS total_recalculated_imps,
    MAX(p_pkg_total_planned_imps) AS planned_imps_pk,
    round(safe_divide(max(total_inflight_impressions),MAX(p_pkg_total_planned_imps)),2) AS imps_diff_pct,
    round(safe_divide(SUM(daily_recalculated_cost),MAX(p_pkg_total_planned_cost)),2) AS cost_diff_pct,
    round(ABS(round(SUM(daily_recalculated_cost)) - MAX(p_pkg_total_planned_cost)),2) AS cost_diff
  --FROM looker-studio-pro-452620.DCM.20250505_costModel_v5
  FROM looker-studio-pro-452620.repo_tables.cost_model
  where flight_status_flag = ''
   --AND placement_id ='P2T86SL'
  GROUP BY 1,2,3
  HAVING cost_diff_pct > 1
  ORDER BY cost_diff DESC)

  select * from a 
  where a.cost_diff_pct - a.imps_diff_pct  > 0 


  -- SELECT * FROM `looker-studio-pro-452620.dcm_costModel_scratch.20250504_dcmCostModel_v4_Scratch`
  -- WHERE package_id = 'P2PVZS4'