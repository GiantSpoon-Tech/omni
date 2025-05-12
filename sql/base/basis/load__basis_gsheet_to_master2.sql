-- @file: load__basis_gsheet_to_master2.sql
-- @layer: base
-- @description: Deduplicates and merges Basis gsheet delivery logs into basis_master2 table.
--               Applies 30-day filter and selects latest_record per unique row key.
--               When matched, updates performance metrics. When not matched, inserts new rows.
-- @source: giant-spoon-299605.data_model_2025.basis_gsheet2
-- @target: giant-spoon-299605.data_model_2025.basis_master2
MERGE INTO
  `giant-spoon-299605.data_model_2025.basis_master2` AS TARGET
USING (
  SELECT *
  FROM (
    SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY
              day, campaign_name, line_item_name, basis_tactic, placement,
              creative_name, creative_grouping_creative_grouping, basis_dsp_tactic_group
            --ORDER BY day DESC  -- Or another reliable tiebreaker
          ) AS row_num
    FROM `giant-spoon-299605.data_model_2025.basis_gsheet2`
    WHERE latest_record = 1
      AND day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  )
  WHERE row_num = 1
) AS SOURCE
ON
  TARGET.date = SOURCE.day
  AND TARGET.campaign = SOURCE.campaign_name
  AND TARGET.package_roadblock = SOURCE.line_item_name
  AND TARGET.tactic = SOURCE.basis_tactic
  AND TARGET.placement = SOURCE.placement
  AND TARGET.creative_name = SOURCE.creative_name
  AND TARGET.creative_grouping = SOURCE.creative_grouping_creative_grouping
  AND TARGET.basis_dsp_tactic_group = SOURCE.basis_dsp_tactic_group

WHEN MATCHED THEN
  UPDATE SET
    TARGET.impressions = SOURCE.impressions,
    TARGET.clicks = SOURCE.clicks,
    TARGET.media_cost = SOURCE.delivered_spend,
    TARGET.video_audio_plays = SOURCE.video_audio_plays,
    TARGET.video_views = SOURCE.video_views,
    TARGET.video_audio_fully_played = SOURCE.video_audio_fully_played,
    TARGET.viewable_impressions = SOURCE.viewable_impressions

WHEN NOT MATCHED THEN
  INSERT (
    date,
    campaign,
    package_roadblock,
    tactic,
    placement,
    creative_name,
    creative_grouping,
    basis_dsp_tactic_group,
    impressions,
    clicks,
    media_cost,
    video_audio_plays,
    video_views,
    video_audio_fully_played,
    viewable_impressions
  )
  VALUES (
    SOURCE.day,
    SOURCE.campaign_name,
    SOURCE.line_item_name,
    SOURCE.basis_tactic,
    SOURCE.placement,
    SOURCE.creative_name,
    SOURCE.creative_grouping_creative_grouping,
    SOURCE.basis_dsp_tactic_group,
    SOURCE.impressions,
    SOURCE.clicks,
    SOURCE.delivered_spend,
    SOURCE.video_audio_plays,
    SOURCE.video_views,
    SOURCE.video_audio_fully_played,
    SOURCE.viewable_impressions
  );
