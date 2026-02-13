CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.dcm_plus_utms` AS
WITH utm_exact AS (
  SELECT * EXCEPT(rn_exact)
  FROM (
    SELECT
      utm.*,
      ROW_NUMBER() OVER (
        PARTITION BY utm.placement_id, utm.creative_assignment
        ORDER BY utm.last_updated DESC, utm.placement_end_date DESC, utm.start DESC
      ) AS rn_exact
    FROM `looker-studio-pro-452620.final_views.utms_view` AS utm
  )
  WHERE rn_exact = 1
),
utm_norm AS (
  SELECT * EXCEPT(rn_norm)
  FROM (
    SELECT
      utm.*,
      LOWER(TRIM(utm.campaign)) AS campaign_norm,
      LOWER(TRIM(utm.placement_id)) AS placement_id_norm,
      REGEXP_REPLACE(LOWER(TRIM(utm.creative_assignment)), r'px', '') AS creative_norm,
      ROW_NUMBER() OVER (
        PARTITION BY
          LOWER(TRIM(utm.campaign)),
          LOWER(TRIM(utm.placement_id)),
          REGEXP_REPLACE(LOWER(TRIM(utm.creative_assignment)), r'px', '')
        ORDER BY utm.last_updated DESC, utm.placement_end_date DESC, utm.start DESC
      ) AS rn_norm
    FROM `looker-studio-pro-452620.final_views.utms_view` AS utm
  )
  WHERE rn_norm = 1
),
joined AS (
  SELECT
    dcm.date,
    dcm.campaign,
    dcm.package_roadblock,
    dcm.package_id,
    dcm.placement_id,
    dcm.impressions,
    dcm.KEY,
    dcm.ad,
    dcm.click_rate,
    dcm.clicks,
    dcm.creative,
    dcm.media_cost,
    dcm.rich_media_video_completions,
    dcm.rich_media_video_plays,
    dcm.total_conversions,
    dcm.p_cost_method,
    dcm.p_package_friendly,
    dcm.p_start_date,
    dcm.p_end_date,
    dcm.p_total_days,
    dcm.p_pkg_daily_planned_cost,
    dcm.p_pkg_total_planned_cost,
    dcm.p_pkg_daily_planned_imps,
    dcm.p_pkg_total_planned_imps,
    dcm.p_channel_group,
    dcm.p_advertiser_name,
    dcm.flight_date_flag,
    dcm.flight_status_flag,
    dcm.rate_raw,
    dcm.n_of_placements,
    dcm.d_min_date,
    dcm.d_max_date,
    dcm.min_flight_date,
    dcm.max_flight_date,
    dcm.pkg_total_imps,
    dcm.total_inflight_impressions,
    dcm.pkg_daily_imps,
    dcm.pkg_daily_imps_perc,
    dcm.pkg_total_imps_perc,
    dcm.pkg_inflight_imps_perc,
    dcm.days_live,
    dcm.prorated_planned_cost_pk,
    dcm.prorated_planned_imps_pk,
    dcm.cpm_overdelivery_flag,
    dcm.daily_cpm,
    dcm.daily_recalculated_cost,
    dcm.daily_recalculated_cost_flag,
    dcm.daily_recalculated_imps,
    CONCAT(dcm.placement_id, ' || ', dcm.creative) AS utm_key,
    COALESCE(utm_exact.utm_source, utm_norm.utm_source) AS utm_source,
    COALESCE(utm_exact.ad_name, utm_norm.ad_name) AS utm_ad_name,
    COALESCE(utm_exact.placement_name, utm_norm.placement_name) AS placement_name,
    COALESCE(utm_exact.placement_id, utm_norm.placement_id) AS utm_placement_id,
    COALESCE(utm_exact.utm_campaign, utm_norm.utm_campaign) AS utm_campaign,
    COALESCE(utm_exact.utm_medium, utm_norm.utm_medium) AS utm_medium,
    COALESCE(utm_exact.utm_content, utm_norm.utm_content) AS utm_content,
    COALESCE(utm_exact.utm_term, utm_norm.utm_term) AS utm_term,
    COALESCE(utm_exact.creative_assignment, utm_norm.creative_assignment) AS utm_creative_assignment,
    CONCAT(
      COALESCE(utm_exact.placement_id, utm_norm.placement_id),
      ' || ',
      COALESCE(utm_exact.creative_assignment, utm_norm.creative_assignment)
    ) AS utm_utm_key
  FROM `looker-studio-pro-452620.final_views.dcm` AS dcm
  LEFT JOIN utm_exact
    ON utm_exact.placement_id = dcm.placement_id
   AND utm_exact.creative_assignment = dcm.creative
  LEFT JOIN utm_norm
    ON utm_exact.placement_id IS NULL
   AND dcm.campaign IN ('MassMutual20252026Media', 'MassMutualLVGP2025')
   AND utm_norm.campaign_norm = LOWER(TRIM(dcm.campaign))
   AND utm_norm.placement_id_norm = LOWER(TRIM(dcm.placement_id))
   AND utm_norm.creative_norm = REGEXP_REPLACE(LOWER(TRIM(dcm.creative)), r'px', '')
),
ranked AS (
  SELECT
    joined.*,
    ROW_NUMBER() OVER (
      PARTITION BY TO_JSON_STRING(joined)
      ORDER BY placement_id
    ) AS rn
  FROM joined
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;
