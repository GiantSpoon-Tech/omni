-- ────────────────────────────────────────────────────────────────────────────────
-- @title:        Cross-Platform Pacing
-- @description:  Combines TikTok, Facebook, and Google Ads unified history models 
--                into a single cross-platform pacing dataset. Ensures consistent 
--                fields (ad, ad group, campaign, budget, dates).
--
-- @author:       [Your Name]
-- @last_updated: [YYYY-MM-DD]
-- @target:       repo_stg.crossplatform_pacing
-- @notes:
--   - TikTok source filters to campaigns containing "_gs_".
--   - Final budget is derived from campaign budget where possible, 
--     otherwise ad group lifetime budget.
-- ────────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_int.crossplatform_pacing` AS

-- ============================================================================
-- TikTok Subset: Combined history with campaign name filter
-- ============================================================================
WITH tt AS (
  SELECT 
    safe_cast(a_ad_id as string) as a_id,
    safe_cast(a_ad_name as string)                      AS ad_name,
    safe_cast(ag_adgroup_id as string)                  AS ag_id,
    ag_adgroup_name                AS adgroup_name,
    ag_start_date,
    ag_end_date,
    ag_budget,
    safe_cast(c_campaign_id as string)                  AS c_id,
    c_campaign_name                AS campaign_name,
    SAFE_CAST(NULL AS DATE)        AS c_start_date,
    SAFE_CAST(NULL AS DATE)        AS c_end_date,
    ag_start_date                  AS start_date,
    ag_end_date                    AS end_date,
    c_budget,
    CASE WHEN c_budget > 0 THEN c_budget ELSE ag_budget END AS final_budget,
    'tiktok' AS platform
  FROM `looker-studio-pro-452620.repo_tables.int__tiktok__combined_history_dedupe_view`
  WHERE CONTAINS_SUBSTR(c_campaign_name, '_gs_')
),

-- ============================================================================
-- Facebook Subset: Combined history with budget fallback logic
-- ============================================================================
fb AS (
  SELECT 
    safe_cast(a_id as string) as a_id,
    a_name                         AS ad_name, 
    safe_cast(ag_id as string) as ag_id, 
    ag_name                        AS adgroup_name,
    ag_start_date,
    ag_end_date,
    safe_divide(ag_lifetime_budget,100)             AS ag_budget,
    safe_cast(c_id as string) as c_id,
    campaign_name,
    c_start_date,
    c_end_date,
    COALESCE(c_start_date, ag_start_date) AS start_date,
    COALESCE(c_end_date,   ag_end_date)   AS end_date,
    safe_divide(c_budget,100)            as c_budget ,
    CASE WHEN c_budget > 0 THEN safe_divide(c_budget,100)  ELSE safe_divide(ag_lifetime_budget,100)  END AS final_budget,
    'facebook' AS platform 
  FROM `looker-studio-pro-452620.repo_facebook.stg__fb_combined_history`
),

-- ============================================================================
-- Google Ads Subset: Combined history standardized to pacing schema
-- ============================================================================
ga AS (
  SELECT
    safe_cast(a_id as string) as a_id,
    ad_name,
    safe_cast(ag_id as string) as ag_id,
    adgroup_name,
    SAFE_CAST(NULL AS DATE)        AS ag_start_date,
    SAFE_CAST(NULL AS DATE)        AS ag_end_date,
    SAFE_CAST(NULL AS INT64)       AS ag_budget,
    safe_cast(c_id as string) as c_id,
    campaign_name,
    c_start_date,
    c_end_date,
    c_start_date                   AS start_date,
    c_end_date                     AS end_date,
    final_budget                   AS c_budget,
    final_budget                   AS final_budget,
    'google_ads' AS platform
  FROM `looker-studio-pro-452620.repo_google_ads.stg__ga_combined_history`
)

-- ============================================================================
-- Final Union: Consolidate TikTok, Facebook, Google Ads into pacing table
-- ============================================================================
SELECT * FROM tt
UNION ALL
SELECT * FROM fb
UNION ALL
SELECT * FROM ga;