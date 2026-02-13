-- * SECTION [1]: TABLE REBUILD
--   Recreate the target table from the updated-FPD ADIF view (schema + base rows).
CREATE OR REPLACE TABLE `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_with_social_tbl` AS
SELECT
  *
FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`;


-- * SECTION [2]: SOCIAL APPEND
--   Append WP social rows at date+campaign+platform grain using campaign-ID budget matching.
INSERT INTO `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_with_social_tbl` (
  package_id_joined,
  date,
  flight_status_flag,
  d_daily_recalculated_cost,
  d_daily_recalculated_imps,
  d_clicks,
  d_video_plays,
  d_video_comps,
  d_min_date,
  d_max_date,
  d_min_flight_date,
  d_max_flight_date,
  d_daily_cpm,
  d_total_delivered_imps,
  d_total_del_inflight_imps,
  planned_daily_spend_pk,
  planned_daily_impressions_pk,
  final_spend,
  final_impressions,
  final_clicks,
  pkg_est_spend,
  pkg_act_spend,
  pkg_est_imps,
  pkg_act_imps,
  pkg_over_bool,
  pkg_over_flag,
  package_type,
  cost_method,
  buy_type,
  buy_category,
  advertiser_name,
  campaign_name,
  supplier_code,
  supplier_name,
  planned_clicks,
  planned_impressions,
  channel_if_buy_category_custom_45,
  package_id,
  package_name,
  PlacementName,
  start_date,
  end_date,
  report_date,
  script_run_date,
  channel,
  channel_raw,
  channel_group,
  campaign_friendly,
  p_package_friendly,
  n_of_placements,
  planned_imps_pk,
  planned_cost_pk
)
WITH

-- * SECTION [2.1]: SOCIAL DAILY
--   Normalize social source to date+campaign+platform grain.
  social_daily AS (
    SELECT
      date_day,
      social_platform,
      CAST(campaign_id AS STRING) AS campaign_id,
      campaign_name,
      account_name,
      SUM(spend) AS spend,
      SUM(impressions) AS impressions,
      SUM(clicks) AS clicks,
      SUM(video_play) AS video_play,
      SUM(video_view) AS video_view,
      SUM(video_views_p_100) AS video_complete_proxy
    FROM `looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform`
    GROUP BY 1,2,3,4,5
  ),


-- * SECTION [2.2]: PACING DEDUPE
--   Keep WP campaign pacing rows, deduped at adgroup flight-window level.
  pacing_dedup AS (
    SELECT
      CASE
        WHEN LOWER(platform) = 'facebook' THEN 'meta'
        ELSE LOWER(platform)
      END AS social_platform,
      CAST(c_id AS STRING) AS campaign_id,
      CAST(ag_id AS STRING) AS ad_group_id,
      ag_start_date,
      ag_end_date,
      MAX(final_budget) AS final_budget
    FROM `looker-studio-pro-452620.repo_int.crossplatform_pacing`
    WHERE REGEXP_CONTAINS(IFNULL(campaign_name, ''), r'WP_')
      AND final_budget > 0
      AND ag_start_date IS NOT NULL
      AND ag_end_date IS NOT NULL
    GROUP BY 1,2,3,4,5
  ),


-- * SECTION [2.3]: PACING DAILY
--   Convert pacing budgets into daily planned spend per campaign and platform.
  pacing_daily AS (
    SELECT
      day AS date_day,
      social_platform,
      campaign_id,
      SUM(
        final_budget / NULLIF(DATE_DIFF(ag_end_date, ag_start_date, DAY) + 1, 0)
      ) AS planned_daily_spend
    FROM pacing_dedup,
      UNNEST(GENERATE_DATE_ARRAY(ag_start_date, ag_end_date)) AS day
    GROUP BY 1,2,3
  ),


-- * SECTION [2.4]: SOCIAL + PLAN JOIN
--   Join social facts to pacing plan by date + platform + campaign_id.
  social_joined AS (
    SELECT
      s.*,
      p.planned_daily_spend
    FROM social_daily AS s
    LEFT JOIN pacing_daily AS p
      ON s.date_day = p.date_day
      AND s.social_platform = p.social_platform
      AND s.campaign_id = p.campaign_id
  ),


-- * SECTION [2.5]: BASE MAPPING
--   Map social rows into ADIF-compatible fields.
  social_mapped AS (
    SELECT
      CONCAT('WP_SOCIAL|', social_platform, '|', campaign_id) AS package_id_joined,
      date_day AS date,
      spend AS final_spend,
      CAST(impressions AS FLOAT64) AS final_impressions,
      CAST(clicks AS FLOAT64) AS final_clicks,
      spend AS d_daily_recalculated_cost,
      CAST(impressions AS INT64) AS d_daily_recalculated_imps,
      CAST(clicks AS INT64) AS d_clicks,
      SAFE_CAST(ROUND(video_play) AS INT64) AS d_video_plays,
      SAFE_CAST(ROUND(video_complete_proxy) AS INT64) AS d_video_comps,
      CAST(planned_daily_spend AS FLOAT64) AS planned_daily_spend_pk,
      CAST(NULL AS FLOAT64) AS planned_daily_impressions_pk,
      account_name AS supplier_name,
      UPPER(social_platform) AS supplier_code,
      campaign_name,
      social_platform,
      campaign_id
    FROM social_joined
  ),


-- * SECTION [2.6]: ROLLUPS
--   Compute package-level totals used by pacing and over/under flags.
  social_with_rollups AS (
    SELECT
      *,
      MIN(date) OVER (
        PARTITION BY package_id_joined
      ) AS start_date_pkg,
      MAX(date) OVER (
        PARTITION BY package_id_joined
      ) AS end_date_pkg,
      SUM(final_spend) OVER (
        PARTITION BY package_id_joined
      ) AS pkg_act_spend,
      SUM(final_impressions) OVER (
        PARTITION BY package_id_joined
      ) AS pkg_act_imps,
      SUM(planned_daily_spend_pk) OVER (
        PARTITION BY package_id_joined
      ) AS pkg_est_spend
    FROM social_mapped
  )


-- * SECTION [2.7]: FINAL SOCIAL ROWS
--   Output social rows aligned to target table columns.
SELECT
  package_id_joined,
  date,
  CASE
    WHEN CURRENT_DATE() > end_date_pkg THEN 'ended'
    ELSE 'live'
  END AS flight_status_flag,
  d_daily_recalculated_cost,
  d_daily_recalculated_imps,
  d_clicks,
  d_video_plays,
  d_video_comps,
  start_date_pkg AS d_min_date,
  end_date_pkg AS d_max_date,
  start_date_pkg AS d_min_flight_date,
  end_date_pkg AS d_max_flight_date,
  CASE
    WHEN final_impressions > 0 THEN (final_spend * 1000) / final_impressions
    ELSE NULL
  END AS d_daily_cpm,
  SAFE_CAST(ROUND(pkg_act_imps) AS INT64) AS d_total_delivered_imps,
  SAFE_CAST(ROUND(pkg_act_imps) AS INT64) AS d_total_del_inflight_imps,
  planned_daily_spend_pk,
  planned_daily_impressions_pk,
  final_spend,
  final_impressions,
  final_clicks,
  pkg_est_spend,
  pkg_act_spend,
  CAST(NULL AS FLOAT64) AS pkg_est_imps,
  pkg_act_imps,
  CASE
    WHEN pkg_est_spend IS NULL OR pkg_est_spend = 0 THEN NULL
    ELSE pkg_act_spend > pkg_est_spend
  END AS pkg_over_bool,
  CASE
    WHEN pkg_est_spend IS NULL OR pkg_est_spend = 0 THEN NULL
    WHEN pkg_act_spend > pkg_est_spend THEN 1
    ELSE 0
  END AS pkg_over_flag,
  'Standalone' AS package_type,
  CAST(NULL AS STRING) AS cost_method,
  CAST(NULL AS STRING) AS buy_type,
  CAST(NULL AS STRING) AS buy_category,
  'Forevermark US' AS advertiser_name,
  campaign_name,
  supplier_code,
  supplier_name,
  CAST(NULL AS INT64) AS planned_clicks,
  CAST(NULL AS INT64) AS planned_impressions,
  social_platform AS channel_if_buy_category_custom_45,
  package_id_joined AS package_id,
  campaign_name AS package_name,
  campaign_name AS PlacementName,
  start_date_pkg AS start_date,
  end_date_pkg AS end_date,
  CURRENT_DATE() AS report_date,
  CURRENT_TIMESTAMP() AS script_run_date,
  CASE
    WHEN COALESCE(d_video_plays, 0) > 0 OR COALESCE(d_video_comps, 0) > 0 THEN 'social_paid video'
    ELSE 'social_paid display'
  END AS channel,
  'social' AS channel_raw,
  'social' AS channel_group,
  campaign_name AS campaign_friendly,
  package_id_joined AS p_package_friendly,
  1 AS n_of_placements,
  CAST(NULL AS INT64) AS planned_imps_pk,
  pkg_est_spend AS planned_cost_pk
FROM social_with_rollups;
