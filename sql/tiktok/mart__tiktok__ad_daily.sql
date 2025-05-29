create or replace view looker-studio-pro-452620.repo_tiktok.mart__tiktok__ad_daily as (

with 
agh as( -- AD GROUP HISTORY CTE
  select 
    adgroup_id, 
    optimization_goal as agh_optimization_goal,
    optimization_event as agh_optimization_event
  from looker-studio-pro-452620.repo_tiktok.stg2__adgroup_history_deduped_filtered
),

ah as ( -- AD HISTORY CTE
  select 
    ad_id as ah_id, 
    optimization_event as ah_optimization_event
  from looker-studio-pro-452620.repo_tiktok.stg2__ad_history_deduped_filtered
),

ch as ( -- CAMPAIGN HISTORY CTE
  select 
    campaign_id as ch_id, 
    objective_type as c_objective_type
  from looker-studio-pro-452620.repo_tiktok.stg2__campaign_history_deduped_filtered
),

ard as ( -- ADS REPORT DAILY CTE
  select
    ad_id as ard_id,
    stat_time_day as date,
    video_views_p_100
  from looker-studio-pro-452620.repo_tiktok.stg2__ad_report_daily_deduped_filtered
)


SELECT 
  *
  --distinct ad_group_id, ad_group_name, agh_optimization_goal,agh.agh_optimization_event, ah.ah_optimization_event, ch.c_objective_type

FROM `giant-spoon-299605.tiktok_ads_tiktok_ads.tiktok_ads__ad_report` as main
left join agh ON agh.adgroup_id = main.ad_group_id
left join ah ON ah.ah_id = main.ad_id
left join ch ON ch.ch_id = main.campaign_id
left join ard on ard.ard_id = main.ad_id and ard.date = date_day 
 --where main.ad_id = 1831027660807202
)