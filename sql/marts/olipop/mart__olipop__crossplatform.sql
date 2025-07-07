CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw` AS

-- ═════════════════════════════════════════════════════════════════════════════
-- View         : mart__olipop__crossplatform
-- ---------------------------------------------------------------------------
-- Purpose      : Unite core delivery metrics with cross-platform video-view
--                engagement for the Olipop account, giving Looker & downstream
--                marts a single ad-day grain fact table.
--
-- Input tables :
--   • `giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report` AS a
--       – Delivery data (impressions, clicks, spend, etc.)
--   • `looker-studio-pro-452620.repo_stg.stg__olipop_videoviews_crossplatform` AS b
--       – Normalised video metrics (views at 25 / 50 / 75 / 100 %, play count,
--         hook-rate numerator)
--
-- Join keys    : source_relation · date_day · campaign_id · ad_group_id · ad_id
--
-- Output       : • All columns from table a  
--                • Selected video metrics from table b →  
--                    video_play, video_views_p_25, video_views_p_50,  
--                    video_views_p_75, video_views_p_100, hookrate_num
--
-- Refresh      : Hourly via Cloud Composer.  Confirm both staging sources are
--                complete before queries run.
--
-- Author       : Gene Tsenter | Last updated 2025-06-23
-- Maintenance  : Validate upstream schema changes before altering this view.
--                Tests live in repo_tests.mart__olipop__crossplatform.
-- ═════════════════════════════════════════════════════════════════════════════
 
WITH
a AS (
  SELECT
    *,
    -- ── video_flag: mark rows from any “video”-named campaign, ad set, or ad ──
    CASE
      WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'video')
        OR REGEXP_CONTAINS(LOWER(ad_group_name), r'video')
        OR REGEXP_CONTAINS(LOWER(ad_name),        r'video')
      THEN 'video'
      ELSE NULL        -- keeps column nullable; change to '' if you prefer
    END AS video_flag
  FROM `giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report`
),


b as ( 
  select
  *
  from  `looker-studio-pro-452620.repo_stg.stg__olipop_videoviews_crossplatform`  
  )

select a.*,
    b.video_play, 
    b.video_view,
    b.video_views_p_25, 
    b.video_views_p_50, 
    b.video_views_p_75, 
    b.video_views_p_100, 
    b.hookrate_num  FROM a
LEFT JOIN b
ON
  a.source_relation = b.source_relation
  AND a.date_day     = b.date_day
  AND a.campaign_id  = b.campaign_id
  AND a.ad_group_id  = b.ad_group_id
  AND a.ad_id        = b.ad_id ;