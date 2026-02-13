-- ────────────────────────────────────────────────────────────────────────────────
-- @title:        FCT – Cross-Platform Pacing (Daily)  [TABLE]
-- @description:  Daily ad-level pacing KPIs. Materialized as a table for speed.
-- @grain:        ad_id × date × platform
-- @target:       looker-studio-pro-452620.repo_mart.fct_crossplatform_pacing_daily
-- @notes:
--   - Partitioned by date; clustered for common filters/joins.
--   - Requires `platform` present in repo_int.crossplatform_pacing.
-- ────────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `looker-studio-pro-452620.repo_mart.fct_crossplatform_pacing_daily`
PARTITION BY date
CLUSTER BY platform, campaign_id, ad_group_id, ad_id AS

WITH data AS (
  SELECT
    ad_id,
    ad_group_id,
    campaign_id,
    platform,
    SAFE_CAST(date_day AS DATE) AS date,
    SUM(spend)                  AS spend
  FROM `giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report`
  GROUP BY 1,2,3,4,5
),
joined AS (
  SELECT
    d.ad_id,
    d.ad_group_id,
    d.campaign_id,
    d.platform,
    d.date,
    d.spend,

    p.a_id          AS p_a_id,
    p.ag_id         AS p_ag_id,
    p.c_id          AS p_c_id,
    p.platform      AS p_platform,
    p.ad_name       AS p_ad_name,
    p.adgroup_name  AS p_adgroup_name,
    p.campaign_name AS p_campaign_name,
    p.ag_start_date AS p_ag_start_date,
    p.ag_end_date   AS p_ag_end_date,
    p.c_start_date  AS p_c_start_date,
    p.c_end_date    AS p_c_end_date,
    p.start_date    AS p_entity_start_date,
    p.end_date      AS p_entity_end_date,
    p.ag_budget     AS p_ag_budget,
    p.c_budget      AS p_c_budget,
    p.final_budget  AS p_final_budget
  FROM data d
  LEFT JOIN `looker-studio-pro-452620.repo_int.crossplatform_pacing` p
    ON d.ad_id    = p.a_id
   AND d.platform = p.platform
),
calc AS (
  SELECT
    *,
    p_entity_start_date AS start_date,
    p_entity_end_date   AS end_date,
    CASE
      WHEN (p_entity_start_date IS NULL OR date >= p_entity_start_date)
       AND (p_entity_end_date   IS NULL OR date <= p_entity_end_date)
      THEN TRUE ELSE FALSE
    END AS is_active_day,
    CASE
      WHEN p_entity_start_date IS NOT NULL AND p_entity_end_date IS NOT NULL
        THEN DATE_DIFF(p_entity_end_date, p_entity_start_date, DAY) + 1
      ELSE NULL
    END AS total_days,
    CASE
      WHEN p_entity_start_date IS NULL OR p_entity_end_date IS NULL THEN NULL
      WHEN date <  p_entity_start_date THEN 0
      WHEN date >  p_entity_end_date   THEN DATE_DIFF(p_entity_end_date, p_entity_start_date, DAY) + 1
      ELSE DATE_DIFF(date, p_entity_start_date, DAY) + 1
    END AS progress_days
  FROM joined
),
kpis AS (
  SELECT
    *,
    SAFE_DIVIDE(progress_days, total_days)                            AS progress_ratio,
    (SAFE_DIVIDE(progress_days, total_days) * p_final_budget)         AS expected_spend_to_date,
    SAFE_DIVIDE(
      spend,
      NULLIF(SAFE_DIVIDE(progress_days, total_days) * p_final_budget, 0)
    )                                                                 AS pace_ratio,
    spend - (SAFE_DIVIDE(progress_days, total_days) * p_final_budget) AS variance_amount,
    SAFE_DIVIDE(spend, p_final_budget)                                AS spend_to_budget_pct,
    CASE
      WHEN p_final_budget IS NULL OR progress_days IS NULL THEN 'unknown'
      WHEN SAFE_DIVIDE(
             spend,
             NULLIF(SAFE_DIVIDE(progress_days, total_days) * p_final_budget, 0)
           ) < 0.90 THEN 'under'
      WHEN SAFE_DIVIDE(
             spend,
             NULLIF(SAFE_DIVIDE(progress_days, total_days) * p_final_budget, 0)
           ) > 1.10 THEN 'over'
      ELSE 'on_target'
    END                                                               AS status_flag
  FROM calc
)

SELECT
  -- Keys & date
  ad_id, ad_group_id, campaign_id, platform, date,

  -- Descriptives
  p_ad_name, p_adgroup_name, p_campaign_name,

  -- Windows
  start_date, end_date, is_active_day,

  -- Financials & KPIs
  spend, p_ag_budget, p_c_budget, p_final_budget,
  total_days, progress_days, progress_ratio,
  expected_spend_to_date, spend_to_budget_pct, pace_ratio, variance_amount, status_flag
FROM kpis;