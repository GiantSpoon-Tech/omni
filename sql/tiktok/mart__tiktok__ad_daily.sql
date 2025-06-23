create or replace view looker-studio-pro-452620.repo_tiktok.mart__tiktok__ad_daily as (

WITH
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

ard as ( -- ADS DELIVERY DAILY CTE
  select
    ad_id as ard_id,
    stat_time_day as date,
    video_views_p_100,
    video_play_actions
  from looker-studio-pro-452620.repo_tiktok.stg2__ad_report_daily_deduped_filtered
),

alft as ( -- ADS DELIVERY DAILY CTE
  select
    ad_id as alft_id,
    reach as reach_alft
  from looker-studio-pro-452620.repo_tiktok.stg__ad_report_lifetime_deduped
),

clft as (
  select
    campaign_id as clft_id,
    max(reach) as reach_clft
  from looker-studio-pro-452620.repo_tiktok.stg3__lifetime_unioned
  where source_table = "stg2__campaign_report_lifetime_deduped_filtered"
  group by 1
  
),

aglft as (
  select
    adgroup_id as aglft_id,
    max(reach) as reach_aglft
  from looker-studio-pro-452620.repo_tiktok.stg3__lifetime_unioned
  where source_table = "stg2__adgroup_report_lifetime_deduped_filtered"
  group by 1
)

SELECT 
  * except (
    audience_type
    ,category
    ,total_purchase_value
    ,total_sales_lead_value
    ,total_conversion_value
    ,adgroup_id
    ,ah_id
    ,ah_optimization_event
    ,ch_id
    ,ard_id
    ,date
  )
  --distinct ad_group_id, ad_group_name, agh_optimization_goal,agh.agh_optimization_event, ah.ah_optimization_event, ch.c_objective_type

FROM `giant-spoon-299605.tiktok_ads_tiktok_ads.tiktok_ads__ad_report` as main
left join agh ON agh.adgroup_id = main.ad_group_id
left join ah ON ah.ah_id = main.ad_id
left join ch ON ch.ch_id = main.campaign_id
left join ard on ard.ard_id = main.ad_id and ard.date = date_day
left join alft on alft.alft_id = main.ad_id 
left join clft on clft.clft_id = main.campaign_id
left join aglft on aglft.aglft_id = main.ad_group_id
 --where main.ad_id = 1831027660807202
)

