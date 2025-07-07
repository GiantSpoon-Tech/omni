CREATE OR REPLACE VIEW
  `looker-studio-pro-452620.repo_stg.stg__olipop_videoviews_crossplatform` AS

/* ╭───────────────────────────────────────────────────────────╮
   │  View: stg__olipop_videoviews_crossplatform              │
   │  Layer / Dataset: repo_stg                               │
   │  Purpose:                                                │
   │     • Standardise video-view metrics from Google Ads      │
   │       (YouTube), TikTok Ads, and Facebook Ads            │
   │     • Provide a single cross-platform fact table for      │
   │       Looker Studio & downstream reporting                │
   │  Owner:  Gene – Marketing Data Analytics                  │
   │  Created: 2025-06-23                                      │
   │  Last-edited: 2025-06-23 (ChatGPT)                        │
   │  Output columns (in order):                               │
   │     1  source_relation  – hard-coded platform tag         │
   │     2  date_day         – UTC date of metric              │
   │     3  advertiser_id    – account / advertiser key        │
   │     4-9 campaign / ad_group / ad ids & names (STRING)     │
   │    10  ad_name                                             │
   │    11  video_view       – completed views (platform def)  │
   │    12  hookrate_num     – 2-sec views / 3-sec plays, etc. │
   │    13  video_play       – impression / play count         │
   │    14-18 quartile view counts (25/50/75/100 %)            │
   │  Design notes:                                            │
   │     • Numeric IDs are cast to STRING so UNION schemas     │
   │       align across sources.                               │
   │     • NULLs are inserted where a platform lacks a metric. │
   ╰───────────────────────────────────────────────────────────╯ */
-- ────────────────────────────
-- Consolidated source CTEs
-- ────────────────────────────
WITH

/* ── Google Ads (YouTube) video stats ─────────────────────── */
yt AS (
  SELECT
      "google_ads_olipop"                       AS source_relation
    , date                                      AS date_day
    , advertiser_id
    , CAST(campaign_id AS STRING)               AS campaign_id
    , campaign_name
    , CAST(ad_group_id AS STRING)               AS ad_group_id
    , ad_group_name
    , CAST(ad_id AS STRING)                     AS ad_id
    , ad_name
    , SUM(video_views)                          AS video_view          -- completed views
    , CAST(NULL AS INT64)                       AS hookrate_num        -- not available in GA
    , SUM(impressions)                          AS video_play          -- impressions / plays
    , SUM(video_views_p_25)                     AS video_views_p_25
    , SUM(video_views_p_50)                     AS video_views_p_50
    , SUM(video_views_p_75)                     AS video_views_p_75
    , SUM(video_views_p_100)                    AS video_views_p_100
  FROM `looker-studio-pro-452620.repo_google_ads.google_ads_video_stats_vw`
  GROUP BY 1,2,3,4,5,6,7,8,9
),

/* ── TikTok Ads daily metrics ─────────────────────────────── */
tt AS (
  SELECT
      "tiktok_ads"                              AS source_relation           -- already present in mart view
    , date_day                                  AS date                -- rename to match union schema
    , advertiser_id
    , CAST(campaign_id AS STRING)               AS campaign_id
    , campaign_name
    , CAST(ad_group_id AS STRING)               AS ad_group_id
    , ad_group_name
    , CAST(ad_id AS STRING)                     AS ad_id
    , ad_name
    , CAST(NULL AS INT64)                       AS video_view          -- not provided; placeholder
    , video_watched_2_s                         AS hookrate_num
    , video_play_actions                        AS video_play
    , video_views_p_25
    , video_views_p_50
    , video_views_p_75
    , video_views_p_100
  FROM `looker-studio-pro-452620.repo_tiktok.mart__tiktok__ad_daily`
),

/* ── Facebook Ads daily & lifetime metrics ────────────────── */
fb AS (
  SELECT
      "facebook_ads"                            AS source_relation
    , date_day
    , account_id                                AS advertiser_id
    , CAST(campaign_id AS STRING)               AS campaign_id
    , campaign_name
    , CAST(ad_set_id AS STRING)                 AS ad_group_id
    , ad_set_name                               AS ad_group_name
    , CAST(ad_id AS STRING)                     AS ad_id
    , ad_name
    , video_play                                -- impressions / plays
    , video_view                                -- completed views
    , video_view_3_sec                          AS hookrate_num
    , video_p25                                 AS video_views_p_25
    , video_p50                                 AS video_views_p_50
    , video_p75                                 AS video_views_p_75
    --, video_p95                               -- excluded to match schema
    , video_p100                                AS video_views_p_100
  FROM `looker-studio-pro-452620.repo_facebook.facebook_daily_and_lifetime_vw`
)

-- ────────────────────────────
-- Final unioned fact set
-- ────────────────────────────
SELECT * FROM yt
UNION ALL
SELECT * FROM tt
UNION ALL
SELECT * FROM fb;