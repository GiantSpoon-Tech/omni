-- @file: test__costmodel__package_discrepancies.sql
-- @layer: tests
-- @description: Compares recalculated delivery cost vs. planned cost for each media package.
--               Flags packages with a cost discrepancy greater than 10%. Also compares impressions.
-- @source: looker-studio-pro-452620.DCM.20250505_costModel_v5
-- @target: QA-only output (not materialized)
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
FROM looker-studio-pro-452620.DCM.20250505_costModel_v5
where flight_status_flag = 'ended'
 --AND placement_id ='P2T86SL'
GROUP BY 1,2,3
HAVING cost_diff_pct > 0.1
ORDER BY cost_diff DESC;

-- SELECT * FROM `looker-studio-pro-452620.dcm_costModel_scratch.20250504_dcmCostModel_v4_Scratch`
-- WHERE package_id = 'P2PVZS4'

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
FROM looker-studio-pro-452620.DCM.cost_model_temp
WHERE package_id = 'P2PDLSJ'
GROUP BY 1,2,3
HAVING cost_diff_pct > 0.1
ORDER BY cost_diff DESC;

-- SELECT * FROM `looker-studio-pro-452620.dcm_costModel_scratch.20250504_dcmCostModel_v4_Scratch`
-- WHERE package_id = 'P2PVZS4';

SELECT * FROM looker-studio-pro-452620.DCM.cost_model_temp
where package_id = 'P2SXFYS' AND placement_id ='P2T86SL';

SELECT
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
FROM looker-studio-pro-452620.DCM.cost_model_temp
--WHERE package_id = 'P2SXFYS'
GROUP BY daily_recalculated_cost_flag
ORDER BY cost_diff DESC;

SELECT
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
FROM looker-studio-pro-452620.DCM.20250430_dcmCostmodel_v4
--WHERE package_id = 'P2SXFYS'
ORDER BY cost_diff DESC;
