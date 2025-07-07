-- This query consolidates Facebook Ads data from various tables
-- to provide a comprehensive view of ad, ad set, and campaign performance,
-- including daily and lifetime metrics, and historical metadata.

-- Common Table Expressions (CTEs) for deduplicating historical data
-- These CTEs ensure that only the most recent record for each ID
-- from the history tables is used, based on 'updated_time' and '_fivetran_synced'.

WITH
  ad_history_dedupe AS (
  SELECT
      _fivetran_synced,
      account_id,
      ad_set_id,
      ad_source_id,
      bid_amount,
      bid_info_actions,
      -- bid_info_impressions -- Commented out: Not currently used.
      bid_type,
      campaign_id,
      configured_status,
      -- conversion_domain -- Commented out: Not currently used.
      created_time,
      creative_id,
      effective_status,
      -- global_adult_nudity_and_sexual_activity, -- Commented out: Not currently used.
      -- global_advertising_policies, -- Commented out: Not currently used.
      -- global_alcohol, -- Commented out: Not currently used.
      -- global_brand_usage_in_ads, -- Commented out: Not currently used.
      -- global_circumventing_systems, -- Commented out: Not currently used.
      -- global_discriminatory_practices, -- Commented out: Not currently used.
      -- global_grammar_profanity, -- Commented out: Not currently used.
      -- global_non_functional_landing_page, -- Commented out: Not currently used.
      id AS ad_id,
      last_updated_by_app_id,
      name,
      -- placement_specific_facebook_alcohol, -- Commented out: Not currently used.
      -- placement_specific_facebook_brand_usage_in_ads, -- Commented out: Not currently used.
      -- placement_specific_facebook_circumventing_systems, -- Commented out: Not currently used.
      -- placement_specific_facebook_discriminatory_practices, -- Commented out: Not currently used.
      -- placement_specific_facebook_non_functional_landing_page, -- Commented out: Not currently used.
      -- placement_specific_instagram_alcohol, -- Commented out: Not currently used.
      -- placement_specific_instagram_brand_usage_in_ads, -- Commented out: Not currently used.
      -- placement_specific_instagram_circumventing_systems, -- Commented out: Not currently used.
      -- placement_specific_instagram_discriminatory_practices, -- Commented out: Not currently used.
      -- placement_specific_instagram_grammar_profanity, -- Commented out: Not currently used.
      -- placement_specific_instagram_non_functional_landing_page, -- Commented out: Not currently used.
      preview_shareable_link,
      status,
      updated_time
    FROM
      (
        SELECT
            a.*,
            ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_time DESC, _fivetran_synced DESC) AS dedupe
          FROM
            `giant-spoon-299605.facebook_ads.ad_history` AS a
      )
    WHERE
      dedupe = 1
  ),

  ad_set_history_dedupe AS (
  SELECT
      _fivetran_synced,
      account_id,
      adset_source_id,
      -- asset_feed_id, -- Commented out: Not currently used.
      bid_amount,
      bid_info_actions,
      -- bid_info_impressions, -- Commented out: Not currently used.
      bid_strategy,
      billing_event,
      budget_remaining,
      campaign_id,
      configured_status,
      created_time,
      daily_budget,
      -- daily_min_spend_target, -- Commented out: Not currently used.
      -- daily_spend_cap, -- Commented out: Not currently used.
      destination_type,
      effective_status,
      end_time,
      id AS ad_set_id,
      -- instagram_actor_id, -- Commented out: Not currently used.
      -- is_dynamic_creative, -- Commented out: Not currently used.
      -- learning_stage_info_attribution_windows, -- Commented out: Not currently used.
      -- learning_stage_info_conversions, -- Commented out: Not currently used.
      -- learning_stage_info_last_sig_edit_ts, -- Commented out: Not currently used.
      -- learning_stage_info_status, -- Commented out: Not currently used.
      lifetime_budget,
      lifetime_imps,
      -- lifetime_min_spend_target, -- Commented out: Not currently used.
      -- lifetime_spend_cap, -- Commented out: Not currently used.
      -- multi_optimization_goal_weight, -- Commented out: Not currently used.
      name,
      optimization_goal,
      -- optimization_sub_event, -- Commented out: Not currently used.
      -- promoted_object_application_id, -- Commented out: Not currently used.
      -- promoted_object_custom_conversion_id, -- Commented out: Not currently used.
      -- promoted_object_custom_event_str, -- Commented out: Not currently used.
      promoted_object_custom_event_type,
      -- promoted_object_event_id, -- Commented out: Not currently used.
      -- promoted_object_object_store_url, -- Commented out: Not currently used.
      -- promoted_object_offer_id, -- Commented out: Not currently used.
      -- promoted_object_offline_conversion_data_set_id, -- Commented out: Not currently used.
      -- promoted_object_page_id, -- Commented out: Not currently used.
      -- promoted_object_pixel_aggregation_rule, -- Commented out: Not currently used.
      promoted_object_pixel_id,
      -- promoted_object_pixel_rule, -- Commented out: Not currently used.
      -- promoted_object_place_page_set_id, -- Commented out: Not currently used.
      -- promoted_object_product_catalog_id, -- Commented out: Not currently used.
      promoted_object_product_set_id,
      -- promoted_object_retention_days, -- Commented out: Not currently used.
      recurring_budget_semantics,
      -- review_feedback, -- Commented out: Not currently used.
      rf_prediction_id,
      start_time,
      status,
      targeting_age_max,
      targeting_age_min,
      -- targeting_app_install_state, -- Commented out: Not currently used.
      targeting_audience_network_positions,
      -- targeting_behaviors, -- Commented out: Not currently used.
      -- targeting_college_years, -- Commented out: Not currently used.
      -- targeting_connections, -- Commented out: Not currently used.
      targeting_device_platforms,
      -- targeting_education_majors, -- Commented out: Not currently used.
      -- targeting_education_schools, -- Commented out: Not currently used.
      -- targeting_education_statuses, -- Commented out: Not currently used.
      -- targeting_effective_audience_network_positions, -- Commented out: Not currently used.
      -- targeting_excluded_connections, -- Commented out: Not currently used.
      -- targeting_excluded_geo_locations_cities, -- Commented out: Not currently used.
      -- targeting_excluded_geo_locations_countries, -- Commented out: Not currently used.
      -- targeting_excluded_geo_locations_country_groups, -- Commented out: Not currently used.
      -- targeting_excluded_geo_locations_custom_locations, -- Commented out: Not currently used.
      -- targeting_excluded_geo_locations_electoral_district, -- Commented out: Not currently used.
      -- targeting_excluded_geo_locations_geo_markets, -- Commented out: Not currently used.
      targeting_excluded_geo_locations_location_types,
      -- targeting_excluded_geo_locations_places, -- Commented out: Not currently used.
      -- targeting_excluded_geo_locations_regions, -- Commented out: Not currently used.
      -- targeting_excluded_geo_locations_zips, -- Commented out: Not currently used.
      -- targeting_excluded_publisher_categories, -- Commented out: Not currently used.
      -- targeting_excluded_publisher_list_ids, -- Commented out: Not currently used.
      -- targeting_excluded_user_device, -- Commented out: Not currently used.
      targeting_exclusions,
      targeting_facebook_positions,
      -- targeting_family_statuses, -- Commented out: Not currently used.
      targeting_flexible_spec,
      -- targeting_friends_of_connections, -- Commented out: Not currently used.
      targeting_genders,
      targeting_geo_locations_cities,
      -- targeting_geo_locations_countries, -- Commented out: Not currently used.
      -- targeting_geo_locations_country_groups, -- Commented out: Not currently used.
      targeting_geo_locations_custom_locations,
      -- targeting_geo_locations_electoral_district, -- Commented out: Not currently used.
      targeting_geo_locations_geo_markets,
      targeting_geo_locations_location_types,
      -- targeting_geo_locations_places, -- Commented out: Not currently used.
      targeting_geo_locations_regions,
      targeting_geo_locations_zips,
      -- targeting_income, -- Commented out: Not currently used.
      -- targeting_industries, -- Commented out: Not currently used.
      targeting_instagram_positions,
      -- targeting_interests, -- Commented out: Not currently used.
      -- targeting_life_events, -- Commented out: Not currently used.
      targeting_locales,
      targeting_publisher_platforms,
      -- targeting_relationship_statuses, -- Commented out: Not currently used.
      -- targeting_user_adclusters, -- Commented out: Not currently used.
      -- targeting_user_device, -- Commented out: Not currently used.
      targeting_user_os,
      -- targeting_wireless_carrier, -- Commented out: Not currently used.
      -- targeting_work_employers, -- Commented out: Not currently used.
      -- targeting_work_positions, -- Commented out: Not currently used.
      updated_time
      -- use_new_app_click -- Commented out: Not currently used.
    FROM
      (
        SELECT
            a.*,
            ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_time DESC, _fivetran_synced DESC) AS dedupe
          FROM
            `giant-spoon-299605.facebook_ads.ad_set_history` AS a
      )
    WHERE
      dedupe = 1
  ),

  campaign_history_dedupe AS (
  SELECT
      _fivetran_synced,
      account_id,
      -- bid_strategy, -- Commented out: Not currently used.
      boosted_object_id,
      -- budget_rebalance_flag, -- Commented out: Not currently used.
      budget_remaining,
      buying_type,
      -- can_create_brand_lift_study, -- Commented out: Not currently used.
      can_use_spend_cap,
      configured_status,
      created_time,
      daily_budget,
      effective_status,
      id AS campaign_id,
      -- is_skadnetwork_attribution, -- Commented out: Not currently used.
      last_budget_toggling_time,
      lifetime_budget,
      name,
      objective,
      pacing_type,
      -- promoted_object_application_id, -- Commented out: Not currently used.
      -- promoted_object_custom_conversion_id, -- Commented out: Not currently used.
      -- promoted_object_custom_event_str, -- Commented out: Not currently used.
      -- promoted_object_custom_event_type, -- Commented out: Not currently used.
      -- promoted_object_event_id, -- Commented out: Not currently used.
      -- promoted_object_object_store_url, -- Commented out: Not currently used.
      -- promoted_object_offer_id, -- Commented out: Not currently used.
      -- promoted_object_offline_conversion_data_set_id, -- Commented out: Not currently used.
      -- promoted_object_page_id, -- Commented out: Not currently used.
      -- promoted_object_pixel_aggregation_rule, -- Commented out: Not currently used.
      -- promoted_object_pixel_id, -- Commented out: Not currently used.
      -- promoted_object_pixel_rule, -- Commented out: Not currently used.
      -- promoted_object_place_page_set_id, -- Commented out: Not currently used.
      promoted_object_product_catalog_id,
      -- promoted_object_product_set_id, -- Commented out: Not currently used.
      -- promoted_object_retention_days, -- Commented out: Not currently used.
      smart_promotion_type,
      source_campaign_id,
      special_ad_categories,
      special_ad_category,
      special_ad_category_country,
      spend_cap,
      start_time,
      status,
      stop_time,
      -- topline_id, -- Commented out: Not currently used.
      updated_time
    FROM
      (
        SELECT
            a.*,
            ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_time DESC, _fivetran_synced DESC) AS dedupe
          FROM
            `giant-spoon-299605.facebook_ads.campaign_history` AS a
      ) AS b
    WHERE
      dedupe = 1
  ),


  -- CTE for pivoting ad actions to transform rows into columns for action_type
pivots_ad_actions_base AS (
  SELECT
      date,
      CAST(account_id AS INT64) AS account_id,
      campaign_id,
      ad_set_id,
      ad_id,
      action_type,
      SUM(value) AS value
  FROM (
    SELECT
      x.date,
      x.action_type,
      x.value,
      CAST(a.account_id AS INT64) AS account_id,
      CAST(a.campaign_id AS INT64) AS campaign_id,
      CAST(a.ad_set_id AS INT64) AS ad_set_id,
      CAST(x.ad_id AS INT64) AS ad_id
    FROM `giant-spoon-299605.facebook_ads.basic_ad_actions` AS x
    LEFT JOIN (
      SELECT DISTINCT
          date_day,
          account_id,
          campaign_id,
          ad_set_id,
          ad_id
        FROM `giant-spoon-299605.facebook_ads_facebook_ads.facebook_ads__ad_report`
    ) AS a
    ON x.ad_id = CAST(a.ad_id AS STRING)
    AND x.date = a.date_day
  )
  GROUP BY date, account_id, campaign_id, ad_set_id, ad_id, action_type
),
  pivots_ad_actions AS (
  SELECT *
  FROM pivots_ad_actions_base
  PIVOT(
    SUM(value) FOR action_type IN (
      'page_engagement' AS page_engagement,
      'post_reaction' AS post_reaction,
      'post_engagement' AS post_engagement,
      'video_view' AS video_view,
      -- etc.
      'purchase' AS purchase
    )
  )
),

  -- CTE for daily Facebook Ad Report data
  facebook_ads__ad_report AS (
  SELECT
      date_day,
      account_id,
      account_name,
      campaign_id,
      campaign_name,
      ad_set_id,
      ad_set_name,
      ad_id,
      ad_name,
      conversion_domain,
      clicks,
      impressions,
      spend,
      conversions,
      conversions_value
    FROM
      `giant-spoon-299605.facebook_ads_facebook_ads.facebook_ads__ad_report`
  ),

  -- CTE for basic daily Ad data
  basic_ads AS (
  SELECT
      _fivetran_synced,
      account_id,
      CAST(ad_id AS INT64) AS ad_id,
      ad_name,
      adset_name,
      cost_per_inline_link_click,
      cpc,
      cpm,
      ctr,
      date,
      frequency,
      impressions,
      inline_link_click_ctr,
      inline_link_clicks,
      ROUND(SAFE_DIVIDE(spend, cpc), 0) AS clicks_all,
      reach,
      spend
    FROM
      `giant-spoon-299605.facebook_ads.basic_ad`
  ),

  -- CTE for 3-second video views
  video_view_3sec AS (
  SELECT
      v.ad_id,
      x.campaign_id,
      x.ad_set_id,
      v.date,
      v.index,
      v.action_type,
      v.value,
      v._fivetran_synced,
      v.inline,
      v._1_d_view,
      v._7_d_click
    FROM
      `giant-spoon-299605.facebook_ads.video_views_3_seconds_actions` AS v
      LEFT JOIN (
        SELECT DISTINCT
            date_day,
            account_id,
            account_name,
            campaign_id,
            campaign_name,
            ad_set_id,
            ad_set_name,
            CAST(ad_id AS STRING) AS ad_id,
            ad_name
          FROM
            `giant-spoon-299605.facebook_ads_facebook_ads.facebook_ads__ad_report`
      ) AS x
      ON v.ad_id = x.ad_id
      AND v.date = x.date_day
    WHERE
      v.action_type IN ('video_view')
  ),

  -- CTE for daily Facebook Ad Set Report data
  facebook_ads__ad_set_report AS (
  SELECT
      account_id,
      account_name,
      ad_set_id,
      ad_set_name,
      budget_remaining,
      campaign_id,
      campaign_name,
      clicks,
      conversions,
      conversions_value,
      daily_budget,
      date_day,
      end_at,
      impressions,
      optimization_goal,
      spend,
      start_at
    FROM
      `giant-spoon-299605.facebook_ads_facebook_ads.facebook_ads__ad_set_report`
  ),

  -- CTE for basic daily Ad Set data
  basic_ad_set AS (
  SELECT
      account_id,
      CAST(adset_id AS INT64) AS ad_set_id,
      adset_name,
      campaign_name,
      cost_per_inline_link_click,
      cpc,
      cpm,
      ctr,
      date,
      frequency,
      impressions,
      inline_link_click_ctr,
      inline_link_clicks,
      ROUND(SAFE_DIVIDE(spend, cpc), 0) AS clicks_all,
      reach,
      spend
    FROM
      `giant-spoon-299605.facebook_ads.basic_ad_set`
  ),

  -- CTE for daily Facebook Campaign Report data
  facebook_ads__campaign_report AS (
  SELECT
      account_id,
      account_name,
      budget_remaining,
      campaign_id,
      campaign_name,
      clicks,
      conversions,
      conversions_value,
      daily_budget,
      date_day,
      end_at,
      impressions,
      lifetime_budget,
      spend,
      start_at,
      status
    FROM
      `giant-spoon-299605.facebook_ads_facebook_ads.facebook_ads__campaign_report`
  ),

  -- CTE for basic daily Campaign data
  basic_campaign AS (
  SELECT
      _fivetran_synced,
      account_id,
      campaign_id,
      campaign_name,
      cost_per_inline_link_click,
      cpc,
      cpm,
      ctr,
      date,
      frequency,
      impressions,
      inline_link_click_ctr,
      inline_link_clicks,
      ROUND(SAFE_DIVIDE(spend, cpc), 0) AS clicks_all,
      reach,
      spend
    FROM
      `giant-spoon-299605.facebook_ads.basic_campaign`
  ),

  -- CTE for detailed video metrics (25%, 50%, 75%, 95%, 100% watched)
  video_metrics AS (
  SELECT
      a.date,
      g.account_id,
      g.account_name,
      g.campaign_id,
      g.campaign_name,
      g.ad_set_id,
      g.ad_set_name,
      CAST(a.ad_id AS INT64) AS ad_id,
      g.ad_name,
      a.action_video_type,
      a.video_play,
      f.video_p25,
      e.video_p50,
      d.video_p75,
      c.video_p95,
      b.video_p100
    FROM
      (
        SELECT
            date,
            ad_id,
            action_video_type,
            value AS video_play
          FROM
            `giant-spoon-299605.facebook_ads.video_ads_actions_video_play_actions`
          WHERE
            action_video_type = "total"
      ) AS a
      LEFT JOIN (
        SELECT
            date,
            ad_id,
            action_video_type,
            value AS video_p100
          FROM
            `giant-spoon-299605.facebook_ads.video_ads_actions_video_p_100_watched_actions`
          WHERE
            action_video_type = "total"
      ) AS b
      ON a.ad_id = b.ad_id
      AND a.action_video_type = b.action_video_type
      AND a.date = b.date
      LEFT JOIN (
        SELECT
            date,
            ad_id,
            action_video_type,
            value AS video_p95
          FROM
            `giant-spoon-299605.facebook_ads.video_ads_actions_video_p_95_watched_actions`
          WHERE
            action_video_type = "total"
      ) AS c
      ON a.ad_id = c.ad_id
      AND a.action_video_type = c.action_video_type
      AND a.date = c.date
      LEFT JOIN (
        SELECT
            date,
            ad_id,
            action_video_type,
            value AS video_p75
          FROM
            `giant-spoon-299605.facebook_ads.video_ads_actions_video_p_75_watched_actions`
          WHERE
            action_video_type = "total"
      ) AS d
      ON a.ad_id = d.ad_id
      AND a.action_video_type = d.action_video_type
      AND a.date = d.date
      LEFT JOIN (
        SELECT
            date,
            ad_id,
            action_video_type,
            value AS video_p50
          FROM
            `giant-spoon-299605.facebook_ads.video_ads_actions_video_p_50_watched_actions`
          WHERE
            action_video_type = "total"
      ) AS e
      ON a.ad_id = e.ad_id
      AND a.action_video_type = e.action_video_type
      AND a.date = e.date
      LEFT JOIN (
        SELECT
            date,
            ad_id,
            action_video_type,
            value AS video_p25
          FROM
            `giant-spoon-299605.facebook_ads.video_ads_actions_video_p_25_watched_actions`
          WHERE
            action_video_type = "total"
      ) AS f
      ON a.ad_id = f.ad_id
      AND a.action_video_type = f.action_video_type
      AND a.date = f.date
      LEFT JOIN (
        SELECT DISTINCT
            date_day,
            account_id,
            account_name,
            campaign_id,
            campaign_name,
            ad_set_id,
            ad_set_name,
            CAST(ad_id AS STRING) AS ad_id,
            ad_name
          FROM
            `giant-spoon-299605.facebook_ads_facebook_ads.facebook_ads__ad_report`
      ) AS g
      ON a.ad_id = g.ad_id
      AND a.date = g.date_day
  ),

  -- CTE for lifetime campaign reach data (deduplicated)
  campaign_lifetime AS (
  SELECT
      CAST(account_id AS INT64) AS account_id,
      account_name,
      CAST(campaign_id AS INT64) AS campaign_id,
      campaign_name,
      clicks AS lifetime_clicks_all,
      impressions AS lifetime_impressions,
      reach AS lifetime_reach,
      cpc AS lifetime_cpc_all,
      cpm AS lifetime_cpm,
      ctr AS lifetime_ctr_all,
      frequency AS lifetime_frequency,
      spend AS lifetime_spend
    FROM
      (
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY account_id, campaign_id ORDER BY _fivetran_synced DESC) AS dedupe
          FROM
            `giant-spoon-299605.facebook_ads.campaign_reach_lifetime`
      )
    WHERE
      dedupe = 1
  ),

  -- CTE for lifetime ad set reach data (deduplicated)
  ad_set_lifetime AS (
  SELECT
      CAST(account_id AS INT64) AS account_id,
      account_name,
      CAST(campaign_id AS INT64) AS campaign_id,
      campaign_name,
      CAST(adset_id AS INT64) AS ad_set_id,
      adset_name AS ad_set_name,
      clicks AS lifetime_clicks_all,
      impressions AS lifetime_impressions,
      reach AS lifetime_reach,
      cpc AS lifetime_cpc_all,
      cpm AS lifetime_cpm,
      ctr AS lifetime_ctr_all,
      frequency AS lifetime_frequency,
      spend AS lifetime_spend
    FROM
      (
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY account_id, adset_id ORDER BY _fivetran_synced DESC) AS dedupe
          FROM
            `giant-spoon-299605.facebook_ads.ad_set_reach_lifetime`
      )
    WHERE
      dedupe = 1
  ),

  -- CTE for lifetime ad reach data (deduplicated)
  ads_lifetime AS (
  SELECT
      CAST(account_id AS INT64) AS account_id,
      account_name,
      CAST(campaign_id AS INT64) AS campaign_id,
      campaign_name,
      CAST(adset_id AS INT64) AS ad_set_id,
      adset_name AS ad_set_name,
      CAST(ad_id AS INT64) AS ad_id,
      ad_name AS ad_name,
      clicks AS lifetime_clicks_all,
      impressions AS lifetime_impressions,
      reach AS lifetime_reach,
      cpc AS lifetime_cpc_all,
      cpm AS lifetime_cpm,
      ctr AS lifetime_ctr_all,
      frequency AS lifetime_frequency,
      spend AS lifetime_spend
    FROM
      (
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY account_id, ad_id ORDER BY _fivetran_synced DESC) AS dedupe
          FROM
            `giant-spoon-299605.facebook_ads.ad_reach_lifetime`
      )
    WHERE
      dedupe = 1
  ),

  -- Final joined CTE for Ad-level data
  joined_ads_final AS (
  SELECT
      a.date_day,
      a.account_id,
      a.account_name,
      a.campaign_id,
      a.campaign_name,
      a.ad_set_id,
      a.ad_set_name,
      a.ad_id,
      a.ad_name,
      a.conversion_domain,
      a.clicks,
      a.impressions,
      a.spend,
      a.conversions,
      a.conversions_value,
      b.reach AS reach_daily,
      b.frequency AS frequency_daily,
      b.inline_link_clicks,
      b.clicks_all,
      f.value AS video_view_3_sec,
      g.video_view,
      --g.comment,
      --g._like,
      --g.post AS post_share,
      h.video_play,
      h.video_p25,
      h.video_p50,
      h.video_p75,
      h.video_p95,
      h.video_p100,
      i.lifetime_clicks_all,
      i.lifetime_impressions,
      i.lifetime_reach,
      i.lifetime_cpc_all,
      i.lifetime_cpm,
      i.lifetime_ctr_all,
      i.lifetime_frequency,
      i.lifetime_spend,
      c.bid_amount,
      c.bid_type,
      c.preview_shareable_link,
      d.bid_strategy,
      d.billing_event,
      d.optimization_goal,
      e.objective
    FROM
      facebook_ads__ad_report AS a
      LEFT JOIN basic_ads AS b
      ON a.account_id = b.account_id
      AND a.ad_id = b.ad_id
      AND a.date_day = b.date
      LEFT JOIN ad_history_dedupe AS c
      ON a.account_id = c.account_id
      AND a.ad_id = c.ad_id
      LEFT JOIN ad_set_history_dedupe AS d
      ON a.account_id = d.account_id
      AND a.ad_set_id = d.ad_set_id
      LEFT JOIN campaign_history_dedupe AS e
      ON a.account_id = e.account_id
      AND a.campaign_id = e.campaign_id
      LEFT JOIN video_view_3sec AS f
      ON a.ad_id = CAST(f.ad_id AS INT64)
      AND a.date_day = f.date
      LEFT JOIN pivots_ad_actions AS g
      ON a.account_id = g.account_id
      AND a.ad_id = g.ad_id
      AND a.date_day = g.date
      LEFT JOIN video_metrics AS h
      ON a.account_id = h.account_id
      AND a.ad_id = h.ad_id
      AND a.date_day = h.date
      LEFT JOIN ads_lifetime AS i
      ON a.account_id = i.account_id
      AND a.ad_id = i.ad_id
  ),

  -- Final joined CTE for Ad Set-level data
  joined_ad_set_final AS (
  SELECT
      a.date_day,
      a.account_id,
      a.account_name,
      a.campaign_id,
      a.campaign_name,
      a.ad_set_id,
      a.ad_set_name,
      CAST(NULL AS INT64) AS ad_id, -- Ad-level specific column, set to NULL for ad set level
      CAST(NULL AS STRING) AS ad_name, -- Ad-level specific column, set to NULL for ad set level
      CAST(NULL AS STRING) AS conversion_domain, -- Ad-level specific column, set to NULL for ad set level
      a.clicks,
      a.impressions,
      a.spend,
      a.conversions,
      a.conversions_value,
      b.reach AS reach_daily,
      b.frequency AS frequency_daily,
      b.inline_link_clicks,
      b.clicks_all,
      f.value AS video_view_3_sec,
      g.video_view,
      --g.comment,
      --g._like,
      --g.post AS post_share,
      h.video_play,
      h.video_p25,
      h.video_p50,
      h.video_p75,
      h.video_p95,
      h.video_p100,
      i.lifetime_clicks_all,
      i.lifetime_impressions,
      i.lifetime_reach,
      i.lifetime_cpc_all,
      i.lifetime_cpm,
      i.lifetime_ctr_all,
      i.lifetime_frequency,
      i.lifetime_spend,
      d.bid_amount,
      CAST(NULL AS STRING) AS bid_type, -- Ad-level specific column, set to NULL for ad set level
      CAST(NULL AS STRING) AS preview_shareable_link, -- Ad-level specific column, set to NULL for ad set level
      d.bid_strategy,
      d.billing_event,
      d.optimization_goal,
      e.objective
    FROM
      facebook_ads__ad_set_report AS a
      LEFT JOIN basic_ad_set AS b
      ON a.account_id = b.account_id
      AND a.ad_set_id = CAST(b.ad_set_id AS INT64)
      AND a.date_day = b.date
      LEFT JOIN ad_set_history_dedupe AS d
      ON a.account_id = d.account_id
      AND a.ad_set_id = d.ad_set_id
      LEFT JOIN campaign_history_dedupe AS e
      ON a.account_id = e.account_id
      AND a.campaign_id = e.campaign_id
      LEFT JOIN (
        SELECT
            ad_set_id,
            date,
            SUM(value) AS value
          FROM
            video_view_3sec
          GROUP BY
            ad_set_id,
            date
      ) AS f
      ON a.ad_set_id = CAST(f.ad_set_id AS INT64)
      AND a.date_day = f.date
      LEFT JOIN (
        SELECT
            date,
            account_id,
            ad_set_id,
            SUM(video_view) AS video_view,
            --SUM(comment) AS comment,
            --SUM(_like) AS _like,
            --SUM(post) AS post
          FROM
            pivots_ad_actions
          GROUP BY
            date,
            account_id,
            ad_set_id
      ) AS g
      ON a.account_id = g.account_id
      AND a.ad_set_id = g.ad_set_id
      AND a.date_day = g.date
      LEFT JOIN (
        SELECT
            date,
            account_id,
            ad_set_id,
            SUM(video_play) AS video_play,
            SUM(video_p25) AS video_p25,
            SUM(video_p50) AS video_p50,
            SUM(video_p75) AS video_p75,
            SUM(video_p95) AS video_p95,
            SUM(video_p100) AS video_p100
          FROM
            video_metrics
          GROUP BY
            date,
            account_id,
            ad_set_id
      ) AS h
      ON a.account_id = h.account_id
      AND a.ad_set_id = h.ad_set_id
      AND a.date_day = h.date
      LEFT JOIN ad_set_lifetime AS i
      ON a.account_id = i.account_id
      AND a.ad_set_id = i.ad_set_id
  ),

  -- Final joined CTE for Campaign-level data
  joined_campaign_final AS (
  SELECT
      a.date_day,
      a.account_id,
      a.account_name,
      a.campaign_id,
      a.campaign_name,
      CAST(NULL AS INT64) AS ad_set_id, -- Ad set-level specific column, set to NULL for campaign level
      CAST(NULL AS STRING) AS ad_set_name, -- Ad set-level specific column, set to NULL for campaign level
      CAST(NULL AS INT64) AS ad_id, -- Ad-level specific column, set to NULL for campaign level
      CAST(NULL AS STRING) AS ad_name, -- Ad-level specific column, set to NULL for campaign level
      CAST(NULL AS STRING) AS conversion_domain, -- Ad-level specific column, set to NULL for campaign level
      a.clicks,
      a.impressions,
      a.spend,
      a.conversions,
      a.conversions_value,
      b.reach AS reach_daily,
      b.frequency AS frequency_daily,
      b.inline_link_clicks,
      b.clicks_all,
      f.value AS video_view_3_sec,
      g.video_view,
      --g.comment,
      --g._like,
      --g.post AS post_share,
      h.video_play,
      h.video_p25,
      h.video_p50,
      h.video_p75,
      h.video_p95,
      h.video_p100,
      i.lifetime_clicks_all,
      i.lifetime_impressions,
      i.lifetime_reach,
      i.lifetime_cpc_all,
      i.lifetime_cpm,
      i.lifetime_ctr_all,
      i.lifetime_frequency,
      i.lifetime_spend,
      CAST(NULL AS INT64) AS bid_amount, -- Ad-level specific column, set to NULL for campaign level
      CAST(NULL AS STRING) AS bid_type, -- Ad-level specific column, set to NULL for campaign level
      CAST(NULL AS STRING) AS preview_shareable_link, -- Ad-level specific column, set to NULL for campaign level
      CAST(NULL AS STRING) AS bid_strategy, -- Ad set-level specific column, set to NULL for campaign level
      CAST(NULL AS STRING) AS billing_event, -- Ad set-level specific column, set to NULL for campaign level
      CAST(NULL AS STRING) AS optimization_goal, -- Ad set-level specific column, set to NULL for campaign level
      e.objective
    FROM
      facebook_ads__campaign_report AS a
      LEFT JOIN basic_campaign AS b
      ON a.account_id = b.account_id
      AND a.campaign_id = CAST(b.campaign_id AS INT64)
      AND a.date_day = b.date
      LEFT JOIN campaign_history_dedupe AS e
      ON a.account_id = e.account_id
      AND a.campaign_id = e.campaign_id
      LEFT JOIN (
        SELECT
            campaign_id,
            date,
            SUM(value) AS value
          FROM
            video_view_3sec
          GROUP BY
            campaign_id,
            date
      ) AS f
      ON a.campaign_id = CAST(f.campaign_id AS INT64)
      AND a.date_day = f.date
      LEFT JOIN (
        SELECT
            date,
            account_id,
            campaign_id,
            SUM(video_view) AS video_view,
            --SUM(comment) AS comment,
            --SUM(_like) AS _like,
            --SUM(post) AS post
          FROM
            pivots_ad_actions
          GROUP BY
            date,
            account_id,
            campaign_id
      ) AS g
      ON a.account_id = g.account_id
      AND a.campaign_id = g.campaign_id
      AND a.date_day = g.date
      LEFT JOIN (
        SELECT
            date,
            account_id,
            campaign_id,
            SUM(video_play) AS video_play,
            SUM(video_p25) AS video_p25,
            SUM(video_p50) AS video_p50,
            SUM(video_p75) AS video_p75,
            SUM(video_p95) AS video_p95,
            SUM(video_p100) AS video_p100
          FROM
            video_metrics
          GROUP BY
            date,
            account_id,
            campaign_id
      ) AS h
      ON a.account_id = h.account_id
      AND a.campaign_id = h.campaign_id
      AND a.date_day = h.date
      LEFT JOIN campaign_lifetime AS i
      ON a.account_id = i.account_id
      AND a.campaign_id = i.campaign_id
  )
-- Main query: Union all the joined data sets (ad, ad set, and campaign)
-- and calculate lifetime reach metrics using window functions.
SELECT
    z.*,
    CASE
      WHEN campaign_name IS NULL THEN NULL
      ELSE MAX(lifetime_reach) OVER(PARTITION BY account_id, campaign_id ORDER BY date_day DESC)
    END AS reach_clft,
    CASE
      WHEN ad_set_name IS NULL THEN NULL
      ELSE MAX(lifetime_reach) OVER(PARTITION BY account_id, campaign_id, ad_set_id ORDER BY date_day DESC)
    END AS reach_aglft,
    CASE
      WHEN ad_name IS NULL THEN NULL
      ELSE MAX(lifetime_reach) OVER(PARTITION BY account_id, campaign_id, ad_set_id, ad_id ORDER BY date_day DESC)
    END AS reach_alft
  FROM
    (
      SELECT
          MIN(date_day) OVER(PARTITION BY account_id, campaign_id, ad_set_id, ad_id) AS start_date_reporting,
          MAX(date_day) OVER(PARTITION BY account_id, campaign_id, ad_set_id, ad_id) AS end_date_reporting,
          a.*,
          'joined_ads_final' AS table_level, -- Renamed 'table' to 'table_level' to avoid reserved keyword
          FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP(), 'America/Los_Angeles') AS run_time
        FROM
          joined_ads_final AS a
      UNION ALL
      SELECT
          MIN(date_day) OVER(PARTITION BY account_id, campaign_id, ad_set_id) AS start_date_reporting,
          MAX(date_day) OVER(PARTITION BY account_id, campaign_id, ad_set_id) AS end_date_reporting,
          b.*,
          'joined_ad_set_final' AS table_level,
          FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP(), 'America/Los_Angeles') AS run_time
        FROM
          joined_ad_set_final AS b
      UNION ALL
      SELECT
          MIN(date_day) OVER(PARTITION BY account_id, campaign_id) AS start_date_reporting,
          MAX(date_day) OVER(PARTITION BY account_id, campaign_id) AS end_date_reporting,
          c.*,
          'joined_campaign_final' AS table_level,
          FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP(), 'America/Los_Angeles') AS run_time
        FROM
          joined_campaign_final AS c
    ) AS z