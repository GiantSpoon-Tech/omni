-- @file: mart__delivery__final_join_dcm_utms_prisma.sql
-- @layer: marts
-- @description: Final unified delivery model. Joins delivery data (DCM) with UTM enrichment
--               and Prisma metadata. Aligns placement and creative-level reporting with
--               campaign planning, actualized delivery, and cost model metrics.
-- @source: final_views.dcm
-- @source: final_views.utms_view

-- @target: repo_stg.dcm_plus_utms
create or replace view `looker-studio-pro-452620.repo_stg.dcm_plus_utms` as

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
  concat(dcm.placement_id, " || ", dcm.creative) as utm_key,
  utm._UTM_Source as utm_source,
  utm.Ad_Name as ad_name,
  utm.Placement_Name as placement_name,
  utm._UTM_Campaign as utm_campaign,
  utm._UTM_Medium as utm_medium,
  utm._UTM_Content as utm_content,
  utm._UTM_Term as utm_term,
  --prsma.placement_name as p_placement_name


FROM
  `looker-studio-pro-452620`.`final_views`.`dcm` AS dcm
left join looker-studio-pro-452620.final_views.utms_view as utm
-- on concat(dcm.placement_id, " || ", dcm.creative) = utm.placement_creative
on concat(dcm.ad) = utm.ad_name
--left join giant-spoon-299605.Prisma_Master.prisma2024 as prsma ON dcm.placement_id = prsma.placement_number
