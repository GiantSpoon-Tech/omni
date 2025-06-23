-- ─────────────────────────────────────────────────────────────
-- @layer:         analysis
-- @description:   Returns the latest snapshot per campaign from TikTok's 
--                 lifetime report, enriched with the latest campaign history metadata.
--                 Includes a derived 'hook rate' metric (2s views ÷ impressions).
-- 
-- @source_tables: 
--     - giant-spoon-299605.tiktok_ads.campaign_report_lifetime
--     - giant-spoon-299605.tiktok_ads.campaign_history
--
-- @output:        One row per campaign_id (latest date, deduped metadata)

-- ─────────────────────────────────────────────────────────────


-- 🔁 Deduplicate campaign history: keep only the most recent `updated_at` per campaign
WITH campaign_history_dedupe AS (
  SELECT b.*
  FROM (
    SELECT 
      a.*,
      ROW_NUMBER() OVER (
        PARTITION BY campaign_id 
        ORDER BY updated_at DESC
      ) AS row_num_cHist
    FROM `giant-spoon-299605.tiktok_ads.campaign_history` a
  ) b
  WHERE row_num_cHist = 1
)

-- 📊 Main query: get latest daily metrics per campaign + metadata
SELECT 
  a.*, 
  SAFE_DIVIDE(video_watched_2_s, impressions) AS hook_rate,  -- ⚠️ Avoids division by zero
  b.*  -- ⚠️ Duplicates campaign_id column — may want to explicitly select fields to avoid conflict
FROM (
  SELECT 
    campaign_id as campaign_id_cLftm, 
    date, 
    reach, 
    video_watched_2_s, 
    impressions,
    clicks,
    result,
    engagements,
    total_pageview,
    total_landing_page_view,
    cost_per_1000_reached,
    landing_page_view_rate,
    
    ROW_NUMBER() OVER (
      PARTITION BY campaign_id 
      ORDER BY date DESC
    ) AS row_num_Clftm
  FROM `giant-spoon-299605.tiktok_ads.campaign_report_lifetime`
  ORDER BY campaign_id, date  -- 📌 Helps with row_number determinism
) a
LEFT JOIN campaign_history_dedupe b 
  ON a.campaign_id_Clftm = b.campaign_id
WHERE a.row_num_Clftm = 1  -- 📌 Only keep most recent report per campaign
;