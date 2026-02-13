--v2 
    CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_mart.crossplatform_pacing_daily__view` AS
  -- ────────────────────────────────────────────────────────────────────────────────
  -- @title:        Ad-Day Spend with Cross-Platform Pacing
  -- @description:  Aggregates ad-level DAILY spend and joins pacing metadata
  --                (ad/adgroup/campaign, dates, budgets) from crossplatform_pacing.
  -- @grain:        ad_id × date (one row per ad per day)
  -- @owner:        [Your Name]
  -- @last_updated: [YYYY-MM-DD]
  -- @target:       repo_int.int__ad_day_with_pacing
  -- @notes:
  --   - Avoids SELECT * to prevent column collisions.
  --   - Uses SAFE_CAST on date_day; rows with invalid dates will be NULL and retained.
  --   - Ensure ad_ids are consistent across sources (same ID system).
  -- ────────────────────────────────────────────────────────────────────────────────

  -- ============================================================================
  -- Daily Spend Aggregation (ad_id × date)
  -- ============================================================================
  WITH data AS (
    SELECT
      ad_id,
      ad_group_id,
      campaign_id,
      --SAFE_CAST(date_day AS DATE) AS date,
      platform as d_platform,
      SUM(spend)                  AS spend,
      max(date_day) as data_updated_thru
    FROM `giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report`
    where date_day < current_date()
    GROUP BY 1, 2, 3,4
  )

  -- ============================================================================
  -- Final Join: Add pacing metadata to daily spend
  -- ============================================================================
  SELECT
    -- Spend (left side)
    d.ad_id,
    d.ad_group_id,
    d.campaign_id,
    --d.platform as platform, 
    --d.date as date,
    d.spend as spend,
    d.data_updated_thru,

    -- Pacing metadata (right side; prefixed to avoid collisions)
    --p.a_id                      AS p_a_id,
    --p.ag_id                     AS p_ag_id,
    --p.c_id                      AS p_c_id,
    p.ad_name                   AS ad_name,
    p.adgroup_name              AS adgroup_name,
    p.campaign_name             AS campaign_name,
    p.ag_start_date             AS ag_start_date,
    p.ag_end_date               AS ag_end_date,
    p.c_start_date              AS c_start_date,
    p.c_end_date                AS c_end_date,
    p.start_date                AS entity_start_date,
    p.end_date                  AS entity_end_date,
    p.ag_budget                 AS ag_budget,
    p.c_budget                  AS c_budget,
    p.final_budget              AS final_budget,      
    p.platform as platform
  FROM data AS d
  LEFT JOIN `looker-studio-pro-452620.repo_int.crossplatform_pacing` AS p
    ON d.ad_id = p.a_id;

--
-- v1 broken
    -- -- ────────────────────────────────────────────────────────────────────────────────
    -- -- @title:        FCT – Cross-Platform Pacing (Daily)  [single-query version]
    -- -- @description:  Aggregates daily ad spend and joins crossplatform pacing metadata,
    -- --                then computes pacing KPIs (linear pacing).
    -- --
    -- -- @grain:        ad_id × date (per platform)
    -- -- @owner:        [Your Name]
    -- -- @last_updated: [YYYY-MM-DD]
    -- -- @target:       looker-studio-pro-452620.repo_mart.fct_crossplatform_pacing_daily
    -- -- @depends_on:   giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report,
    -- --                looker-studio-pro-452620.repo_int.crossplatform_pacing
    -- -- @notes:
    -- --   - Requires `platform` in crossplatform_pacing; set it per source upstream
    -- --     (e.g., 'tiktok','facebook','google_ads').
    -- -- ────────────────────────────────────────────────────────────────────────────────

    CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_int.crossplatform_pacing_and_delivery__view` AS
    WITH
    -- ============================================================================
    -- Daily spend at ad grain
    -- ============================================================================
    data AS (
      SELECT
        --ad_id,
        ad_group_id,
        campaign_id,
        platform,
        SAFE_CAST(date_day AS DATE) AS date,
        (spend)                  AS spend
      FROM `giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report`
      --GROUP BY 1,2,3,4
    ),

    -- ============================================================================
    -- Join pacing metadata (platform-aware join)
    -- ============================================================================
    joined AS (
      SELECT
        --d.ad_id,
        d.ad_group_id,
        d.campaign_id,
        d.platform,
        d.date,
        d.spend,

        --p.a_id                 AS a_id,
        --p.ag_id                AS ag_id,
        --p.c_id                 AS c_id,
        --p.platform             AS platform,
        --p.ad_name              AS ad_name,
        p.adgroup_name         AS adgroup_name,
        p.campaign_name        AS campaign_name,
        p.ag_start_date        AS ag_start_date,
        p.ag_end_date          AS ag_end_date,
        p.c_start_date         AS c_start_date,
        p.c_end_date           AS c_end_date,
        p.start_date           AS entity_start_date,
        p.end_date             AS entity_end_date,
        p.ag_budget            AS ag_budget,
        p.c_budget             AS c_budget,
        p.final_budget         AS final_budget
      FROM data d
      LEFT JOIN `looker-studio-pro-452620.repo_int.crossplatform_pacing` p
        ON d.ad_group_id    = p.ag_id
       AND d.campaign_id = p.c_id
    ),

    -- ============================================================================
    -- Active window & progress
    -- ============================================================================
    calc AS (
      SELECT
        *,
        entity_start_date AS start_date,
        entity_end_date   AS end_date,

        CASE
          WHEN (entity_start_date IS NULL OR date >= entity_start_date)
           AND (entity_end_date   IS NULL OR date <= entity_end_date)
          THEN TRUE ELSE FALSE
        END AS is_active_day,

        CASE
          WHEN entity_start_date IS NOT NULL AND entity_end_date IS NOT NULL
            THEN DATE_DIFF(entity_end_date, entity_start_date, DAY) + 1
          ELSE NULL
        END AS total_days,

        CASE
          WHEN entity_start_date IS NULL OR entity_end_date IS NULL THEN NULL
          WHEN date <  entity_start_date THEN 0
          WHEN date >  entity_end_date   THEN DATE_DIFF(entity_end_date, entity_start_date, DAY) + 1
          ELSE DATE_DIFF(date, entity_start_date, DAY) + 1
        END AS progress_days
      FROM joined
      where contains_substr(campaign_name, "_gs")
    ),

    -- ============================================================================
    -- KPIs
    -- ============================================================================
    kpis AS (
      SELECT
        *,
        SAFE_DIVIDE(progress_days, total_days)                               AS progress_ratio,
        (SAFE_DIVIDE(progress_days, total_days) * final_budget)            AS expected_spend_to_date,
        SAFE_DIVIDE(spend,
          NULLIF(SAFE_DIVIDE(progress_days, total_days) * final_budget, 0)
        )                                                                    AS pace_ratio,
        spend - (SAFE_DIVIDE(progress_days, total_days) * final_budget)    AS variance_amount,
        SAFE_DIVIDE(spend, final_budget)                                   AS spend_to_budget_pct,
        CASE
          WHEN final_budget IS NULL OR progress_days IS NULL THEN 'unknown'
          WHEN SAFE_DIVIDE(spend,
                 NULLIF(SAFE_DIVIDE(progress_days, total_days) * final_budget, 0)
               ) < 0.90 THEN 'under'
          WHEN SAFE_DIVIDE(spend,
                 NULLIF(SAFE_DIVIDE(progress_days, total_days) * final_budget, 0)
               ) > 1.10 THEN 'over'
          ELSE 'on_target'
        END                                                                  AS status_flag
      FROM calc
    )

    SELECT
      -- Keys & date
      --ad_id, 
      ad_group_id, campaign_id, platform, date,

      -- Descriptives
      --ad_name, 
      adgroup_name, campaign_name,

      -- Windows
      start_date, end_date, is_active_day,

      -- Financials and KPIs
      spend, ag_budget, c_budget, final_budget,
      total_days, progress_days, progress_ratio,
      expected_spend_to_date, spend_to_budget_pct, pace_ratio, variance_amount, status_flag
    FROM kpis;