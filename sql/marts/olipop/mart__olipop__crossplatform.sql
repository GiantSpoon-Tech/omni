CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_mart.mart__olipop__crossplatform` AS
-- ═════════════════════════════════════════════════════════════════════════════
-- View : mart__olipop__crossplatform
-- ---------------------------------------------------------------------------
-- Purpose      : Combine core delivery stats with cross-platform video-view
--                metrics for the Olipop account, providing a single source of
--                truth for downstream mart & Looker models.
--
-- Input tables :
--   • `giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report` AS a
--       - Delivery-level data (impressions, clicks, cost, etc.)
--   • `looker-studio-pro-452620.repo_mart.stg__videoviews_v2`              AS b
--       - Normalised 25/50/75/100-percent video-view counts
--
-- Join keys    : source_relation · date_day · campaign_id · ad_group_id · ad_id
--
-- Output       : All columns from table a (see SELECT below).  
--                ⚠️  Currently no fields from b are surfaced; the LEFT JOIN
--                is used only for row-level alignment.  Add b.* or specific
--                columns if you need the video-view metrics exposed.
--
-- Refresh      : Scheduled via Cloud Composer (hourly).  Ensure both staging
--                sources finish upstream before the view is queried.
--
-- Author       : Gene Tsenter  |  Last updated: 2025-06-23
-- Maintenance  : Validate new columns in upstream tables before altering this
--                view.  Use strict column naming to avoid accidental schema
--                drift.  Tests live in repo_tests.mart__olipop__crossplatform.
-- ═════════════════════════════════════════════════════════════════════════════

SELECT
  a.*              -- delivery-level fields only; see header note above
FROM
  `giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report` AS a
LEFT JOIN
  `looker-studio-pro-452620.repo_mart.stg__videoviews_v2`              AS b
ON
  a.source_relation = b.source_relation
  AND a.date_day     = b.date_day
  AND a.campaign_id  = b.campaign_id
  AND a.ad_group_id  = b.ad_group_id
  AND a.ad_id        = b.ad_id ;