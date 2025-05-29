-- ────────────────────────────────────────────────────────────────────────────────
-- @title:        TikTok History Deduplication Query
-- @description:  Extracts the most recent records for campaigns, ad groups, ads, 
--                and video creatives from TikTok ad history tables. Deduplication 
--                is performed using `row_number()` over the unique ID and `updated_at`, 
--                ensuring only the latest version of each entity is retained.
-- 
--                Tables used:
--                - tiktok_ads.campaign_history
--                - tiktok_ads.adgroup_history
--                - tiktok_ads.ad_history
--                - tiktok_ads.video_history
--
--                Final output returns the latest ad-level records.
--
-- @author:       [Your Name]
-- @last_updated: [Insert Date]
-- @target:       combined_history_dedupe (final SELECT)
-- ────────────────────────────────────────────────────────────────────────────────
create or replace table `looker-studio-pro-452620.repo_tables.int__tiktok__combined_history_dedupe` as
-- ────────────────────────────────────────────────────────────────────────────────
-- STEP 1: Deduplicate campaign-level history
-- Logic: For each `campaign_id`, select the most recent row based on `updated_at`
-- Only relevant columns retained for downstream joining and reporting
-- ────────────────────────────────────────────────────────────────────────────────
WITH campaign_history_dedupe AS (
  SELECT
    campaign_id
-- ,updated_at
,advertiser_id
,campaign_name
,campaign_type
,budget
,budget_mode
,operation_status
,objective_type
--,is_new_structure
--,split_test_variable
,secondary_status
-- ,create_time
,buying_type
--,app_promotion_type
--,is_search_campaign
--,is_smart_performance_campaign
--,app_id
--,is_advanced_dedicated_campaign
--,campaign_app_profile_page_state
,rf_campaign_type
--,campaign_product_source
--,budget_optimize_on
--,bid_type
--,deep_bid_type
--,roas_bid
--,optimization_goal
--,rta_id
--,rta_product_selection_enabled
--,postback_window_mode
,objective
-- ,_fivetran_synced
-- ,dedupe

  FROM (
    SELECT a.*,
      ROW_NUMBER() OVER (
        PARTITION BY campaign_id 
        ORDER BY updated_at DESC, _fivetran_synced DESC
      ) AS dedupe
    FROM giant-spoon-299605.tiktok_ads.campaign_history a
  )
  WHERE dedupe = 1
),

-- ────────────────────────────────────────────────────────────────────────────────
-- STEP 2: Deduplicate adgroup-level history
-- Logic: Similar to campaigns — get most recent adgroup entry
-- Keeps budget, optimization, and targeting data
-- ────────────────────────────────────────────────────────────────────────────────
adgroup_history_dedupe AS (
  SELECT
    adgroup_id
    -- ,updated_at
    ,advertiser_id
    ,campaign_id
    -- ,create_time
    ,adgroup_name
    ,placement_type
    --,profile_image
    -- ,landing_page_url
    -- ,display_name
    --,app_type
    --,app_download_url
    --,app_name
    ,optimization_event
    --,secondary_optimization_event
    --,creative_material_mode
    --,audience_type
    --,gender
    --,min_android_version
    --,min_ios_version
    ,budget_mode
    ,schedule_type
    --,dayparting
    ,optimization_goal
    --,cpv_video_duration
    ,pacing
    ,billing_event
    ,bid_type
    --,deep_bid_type
    ,secondary_status
    ,operation_status
    --,statistic_type
    --,video_download
    --,open_url
    --,open_url_type
    --,fallback_type
    ,budget
    ,bid_price
    --,conversion_bid_price
    --,deep_cpa_bid
    -- ,schedule_start_time
    -- ,schedule_end_time
    --,app_id
    --,pixel_id
    --,inventory_filter_enabled
    --,is_hfss
    --,is_new_structure
    --,category
    --,is_comment_disable
    ,skip_learning_phase
    ,frequency
    ,frequency_schedule
    --,action_days
    --,audience
    --,excluded_audience
    --,location
    --,interest_category_v_2
    --,pangle_block_app_list_id
    --,action_categories
    ,placements
    --,keywords
    ,age_groups
    -- ,languages
    --,operating_systems
    --,network_types
    --,video_actions
    ,buying_type
    --,is_smart_performance_campaign
    --,shopping_ads_type
    --,identity_id
    --,identity_type
    --,identity_authorized_bc_id
    --,product_source
    --,store_id
    --,store_authorized_bc_id
    ,promotion_type
    --,promotion_target_type
    --,promotion_website_type
    ,tiktok_subplacements
    ,search_result_enabled
    --,comment_disabled
    ,video_download_disabled
    --,share_disabled
    --,shopping_ads_retargeting_type
    --,shopping_ads_retargeting_actions_days
    --,shopping_ads_retargeting_custom_audience_relation
    --,spending_power
    --,smart_audience_enabled
    --,smart_interest_behavior_enabled
    --,ios_14_targeting
    --,ios_14_quota_type
    ,saved_audience_id
    ,brand_safety_type
    --,brand_safety_partner
    --,vertical_sensitivity_id
    --,scheduled_budget
    --,predict_impression
    --,pre_discount_cpm
    --,cpm
    --,discount_type
    --,discount_amount
    --,discount_percentage
    --,pre_discount_budget
    --,delivery_mode
    --,roas_bid
    --,vbo_window
    ,bid_display_mode
    --,next_day_retention
    --,click_attribution_window
    -- ,engaged_view_attribution_window
    ,view_attribution_window
    ,attribution_event_count
    --,adgroup_app_profile_page_state
    --,feed_type
    --,rf_purchased_type
    -- ,purchased_impression
    -- ,purchased_reach
    -- ,rf_estimated_cpr
    -- ,rf_estimated_frequency
    --,split_test_group_id
    --,split_test_status
    --,package
    ,_fivetran_synced
  FROM (
    SELECT a.*,
      ROW_NUMBER() OVER (
        PARTITION BY adgroup_id 
        ORDER BY updated_at DESC, _fivetran_synced DESC
      ) AS dedupe
    FROM giant-spoon-299605.tiktok_ads.adgroup_history a
  )
  WHERE dedupe = 1
),

-- ────────────────────────────────────────────────────────────────────────────────
-- STEP 3: Deduplicate ad-level history
-- Logic: Pull the latest creative/ad object per ad_id
-- Includes creative metadata, CTA, landing URLs, and identity info
-- ────────────────────────────────────────────────────────────────────────────────
ad_history_dedupe AS (
  SELECT
    ad_id
    -- ,updated_at
    ,advertiser_id
    ,adgroup_id
    ,campaign_id
    ,video_id
    ,create_time
    ,tiktok_item_id
    ,ad_name
    ,call_to_action
    ,secondary_status
    ,operation_status
    ,ad_text
    ,app_name
    ,deeplink
    ,landing_page_url
    ,display_name
    ,profile_image_url
    --,impression_tracking_url
    --,click_tracking_url
    --,playable_url
    --,is_aco
    ,creative_authorized
    --,is_new_structure
    ,image_ids
    --,ad_format
    ,buying_type
    ,identity_id
    ,identity_type
    ,identity_authorized_bc_id
    --,product_specific_type
    --,product_set_id
    --,vertical_video_strategy
    --,dynamic_format
    --,carousel_image_index
    --,music_id
    --,promotional_music_disabled
    --,item_duet_status
    --,item_stitch_status
    ,dark_post_status
    --,branded_content_disabled
    --,shopping_ads_video_package_id
    ,call_to_action_id
    --,card_id
    --,page_id
    --,cpp_url
    --,tiktok_page_category
    --,phone_region_code
    --,phone_region_calling_code
    --,phone_number
    --,deeplink_type
    --,shopping_ads_deeplink_type
    --,shopping_ads_fallback_type
    --,fallback_type
    --,dynamic_destination
    --,aigc_disclosure_type
    ,tracking_pixel_id
    --,tracking_app_id
    --,vast_moat_enabled
    --,viewability_postbid_partner
    --,viewability_vast_url
    --,brand_safety_postbid_partner
    --,brand_safety_vast_url
    --,creative_type
    ,avatar_icon_web_uri
    ,optimization_event
    -- ,_fivetran_synced
    -- ,dedupe
  FROM (
    SELECT a.*,
      ROW_NUMBER() OVER (
        PARTITION BY ad_id 
        ORDER BY updated_at DESC, _fivetran_synced DESC
      ) AS dedupe
    FROM giant-spoon-299605.tiktok_ads.ad_history a
  )
  WHERE dedupe = 1
),

-- ────────────────────────────────────────────────────────────────────────────────
-- STEP 4: Deduplicate video-level history (optional, not selected downstream)
-- Logic: Fetch most recent creative asset per video_id
-- ────────────────────────────────────────────────────────────────────────────────
video_history_dedupe AS (
  SELECT
    video_id
    --,updated_at
    ,width
    ,advertiser_id
    ,video_cover_url
    ,bit_rate
    ,format
    ,preview_url
    --,preview_url_expire_time
    ,duration
    ,height
    -- ,signature
    -- ,size
    -- ,material_id
    --,allowed_placements
    --,allow_download
    -- ,file_name
    -- ,create_time
    --,displayable
    -- ,_fivetran_synced
    --,dedupe
  FROM (
    SELECT a.*,
      ROW_NUMBER() OVER (
        PARTITION BY video_id 
        ORDER BY updated_at DESC, _fivetran_synced DESC
      ) AS dedupe
    FROM giant-spoon-299605.tiktok_ads.video_history a
  )
  WHERE dedupe = 1
)

-- ────────────────────────────────────────────────────────────────────────────────
-- FINAL OUTPUT
-- Pull latest ads only. Change this section to join with other deduped tables
-- (e.g., campaign_history_dedupe) if a full joined view is needed.
-- ────────────────────────────────────────────────────────────────────────────────
SELECT *
FROM ad_history_dedupe;