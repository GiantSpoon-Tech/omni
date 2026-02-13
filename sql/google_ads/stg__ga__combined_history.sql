-- ────────────────────────────────────────────────────────────────────────────────
-- @title:        Google Ads Combined History
-- @description:  Creates a combined view of Google Ads data by joining ads, 
--                ad group history, campaign budget history, and campaign history. 
--                Provides unified ad-level records with campaign budget logic.
--
-- @author:       [Your Name]
-- @last_updated: [Insert Date]
-- @target:       repo_google_ads.stg__ga_combined_history
-- ────────────────────────────────────────────────────────────────────────────────

create or replace view repo_google_ads.stg__ga_combined_history as

-- ============================================================================
-- Ads Data CTE: Pulls distinct ad-level data from video stats view
-- ============================================================================
with ads_data as (
  select distinct
    ad_id as a_id,
    ad_name as ad_name,
    ad_group_id as ag_id,
    ad_group_name as aggroup_name,
    campaign_id as c_id,
    campaign_name,
    --creative_id as crtv_id
  from `looker-studio-pro-452620.repo_google_ads.google_ads_video_stats_vw`
),

-- ============================================================================
-- Ad Group History CTE: Metadata for ad groups (budgets, dates, etc.)
-- ============================================================================
agh as (
  select
    id as ag_id,
    --name as ag_name,
    --daily_budget as ag_daily_budget,
    --daily_spend_cap as ag_daily_cap,
    --budget as ag_lifetime_budget,
    --lifetime_imps as ag_lifetime_imps,
    --budget_remaining as ag_budget_remaining,
    --cast(sta as date) as ag_start_date,
    --cast(end_time as date) as ag_end_date
  from `looker-studio-pro-452620.repo_google_ads.stg__ad_group_history_deduped`
),

-- ============================================================================
-- Campaign Budget History CTE: Pulls micros → budget conversions
-- ============================================================================
cbh as (
  select
    campaign_id as c_id,
    --name as campaign_name,
    safe_divide(amount_micros,1000000) as c_budget,
    --amount_micros,
    --total_amount_micros,
    safe_divide(total_amount_micros,1000000) as c_budget_1,
    --cast(start_time as date) as c_start_date,
    --cast(stop_time as date) as c_end_date
  from `looker-studio-pro-452620.repo_google_ads.stg__campaign_budget_history_deduped`
),

-- ============================================================================
-- Campaign History CTE: Campaign-level metadata and active dates
-- ============================================================================
ch as (
  select
    id as c_id,
    --name as campaign_name,
    --lifetime_budget as c_budget,
    cast(start_date as date) as c_start_date,
    cast(end_date as date) as c_end_date
  from `looker-studio-pro-452620.repo_google_ads.stg__campaign_history_deduped`
),

-- ============================================================================
-- Raw Join CTE: Combines ads, ad groups, campaigns, and budgets
-- ============================================================================
raw as (
  select 
    ads_data.* , 
    --agh.* except (ag_id), 
    ch.* except (c_id),
    cbh.* except (c_id)
  from ads_data 
  left join agh on ads_data.ag_id = agh.ag_id
  left join ch on ads_data.c_id = ch.c_id
  left join cbh on ch.c_id = cbh.c_id
)

-- ============================================================================
-- Final Output: Adds a unified final_budget field
-- ============================================================================
select *,
  --coalesce(c_budget,c_budget_1) as final_budget,
  case 
    when c_budget > 0 then c_budget else c_budget_1
  end as final_budget
from raw;