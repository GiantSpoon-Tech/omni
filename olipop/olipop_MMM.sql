-- @title: Olipop MMM Data View
-- @description: Weekly aggregated marketing data for Olipop accounts
-- @outputs: looker-studio-pro-452620.repo_mart.olipop_MMM
-- @last_updated: 2025-01-28

CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_mart.olipop_MMM` AS
SELECT 
    "Olipop" as Product,
    case when platform = 'google_ads' then 'OLV'
    when platform = 'tiktok_ads' then 'TikTok'
    when platform = 'facebook_ads' then 'Meta'
    when platform = 'linkedin_ads' then 'LinkedIn'
    else platform
    end as Tactic,
    DATE_TRUNC(date_day, WEEK(SUNDAY)) AS date_week,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(spend) AS spend
FROM `looker-studio-pro-452620.repo_mart.mart__olipop__crossplatform`
WHERE date_day >= '2025-01-01' 
  AND LOWER(account_name) LIKE '%olipop%'  -- contains 'olipop' (case insensitive)
GROUP BY 1,2,3
ORDER BY 1,2,3;
