

--CREATE OR REPLACE TABLE repo_tables.cost_model AS -- UPDATED 7/6/25
CREATE OR REPLACE TABLE looker-studio-pro-452620.DCM.20250505_costModel_v5 AS

--------------- version current UPDATED 05/13/25 ------------

WITH

-- 1. [  bd. ] JOIN RAW DELIVERY + METADATA (one source of truth)
base_delivery_metadata AS (
  /*
    Joins raw delivery to Prisma metadata.
    Applies 60-day email flight extension.
    Filters to only rows with impressions > 0.
    Aliases Prisma fields with `p_` prefix.
    Adds static flags: flight_date_flag, flight_status_flag, rate_raw
  */
  SELECT
    -- Delivery fields
    a.date,
    a.campaign,
    a.package_roadblock,
    a.package_id,
    a.placement_id,
    a.impressions,
    a.placement,
    a.`KEY`,
    a.`ad`,
    a.`click_rate`,
    a.`clicks`,
    a.`creative`,
    a.`media_cost`,
    a.`rich_media_video_completions`,
    a.`rich_media_video_plays`,
    a.`total_conversions`,

    -- Prisma metadata fields with aliases
    b.cost_method AS p_cost_method,
    b.p_package_friendly,
    b.start_date AS p_start_date,

    -- [p_end_date] Extended flight window for email packages (+60 days)
    CASE
      WHEN a.package_roadblock LIKE '%email%' THEN DATE_ADD(b.end_date, INTERVAL 60 DAY)
      ELSE b.end_date
    END AS p_end_date,

    b.total_days AS p_total_days,
    b.planned_daily_spend_pk AS p_pkg_daily_planned_cost,
    b.planned_cost_pk AS p_pkg_total_planned_cost,
    b.planned_daily_impressions_pk AS p_pkg_daily_planned_imps,
    b.planned_imps_pk AS p_pkg_total_planned_imps,
    b.channel_group AS p_channel_group,
    b.advertiser_name AS p_advertiser_name,

    -- [flight_date_flag] 0 = in-flight, 1 = out-of-flight (based on extended window)
    CASE
      WHEN a.date BETWEEN b.start_date AND
           CASE WHEN a.package_roadblock LIKE '%email%' THEN DATE_ADD(b.end_date, INTERVAL 60 DAY)
                ELSE b.end_date END
      THEN 0 ELSE 1
    END AS flight_date_flag,

    -- [flight_status_flag] 'live' if today is within the flight window, else 'ended'
    CASE
      WHEN CURRENT_DATE() BETWEEN b.start_date AND
           CASE WHEN a.package_roadblock LIKE '%email%' THEN DATE_ADD(b.end_date, INTERVAL 60 DAY)
                ELSE b.end_date END
      THEN 'live' ELSE 'ended'
    END AS flight_status_flag,

    -- [rate_raw] Extracted numeric rate value from end of package_roadblock string
    REGEXP_EXTRACT(a.package_roadblock, r'(\d+\.?\d*)\D*$') AS rate_raw

  FROM `giant-spoon-299605.data_model_2025.new_md` a
  LEFT JOIN `looker-studio-pro-452620.20250327_data_model.prisma_porcessed` b
    ON a.package_id = b.package_id

  WHERE a.impressions > 0 
  
),

-- 2. [ dr. ] delivery rollup |  PRECOMPUTED PACKAGE-LEVEL METRICS
delivery_rollups AS (
  /*
    Computes package-level aggregates used in multiple downstream CTEs:
    - Total impressions
    - Distinct placements
    - Min/max dates inside and outside flight window
  */
  SELECT
    package_id,

    -- Actual min/max delivery dates
    MIN(date) AS d_min_date,
    MAX(date) AS d_max_date,

    -- Min/max dates within the official flight window
    MIN(CASE WHEN date BETWEEN p_start_date AND p_end_date THEN date ELSE NULL END) AS min_flight_date,
    MAX(CASE WHEN date BETWEEN p_start_date AND p_end_date THEN date ELSE NULL END) AS max_flight_date,

    SUM(impressions) AS total_impressions,
    COUNT(DISTINCT placement_id) AS n_of_placements,

    SUM(CASE
      WHEN date BETWEEN p_start_date AND p_end_date THEN impressions
      ELSE 0
    END) AS total_inflight_impressions

  FROM base_delivery_metadata
  GROUP BY package_id
),

-- 3. [ cf. ] CALCULATED FLAGS + PRORATED FIELDS
calculated_fields AS (
  /*
    Adds:
      - Package-level impression %s
      - Flags: flight, overdelivery
      - Prorated cost & imps
      - days_live, flight status
      - rate extraction
  */
  SELECT
    bd.*,

    -- ───────────────────────────────────────────────
    -- From delivery_rollups
    dr.n_of_placements,
    dr.d_min_date,
    dr.d_max_date,
    dr.min_flight_date,
    dr.max_flight_date,
    dr.total_impressions AS pkg_total_imps,
    dr.total_inflight_impressions,

    -- ───────────────────────────────────────────────
    -- Windowed daily aggregates
    SUM(bd.impressions) OVER (PARTITION BY bd.package_id, bd.date) AS pkg_daily_imps,

    -- Impression share of daily package total
    SAFE_DIVIDE(
      bd.impressions,
      SUM(bd.impressions) OVER (PARTITION BY bd.package_id, bd.date)
    ) AS pkg_daily_imps_perc,

    -- Impression share of full package total
    SAFE_DIVIDE(bd.impressions, dr.total_impressions) AS pkg_total_imps_perc,
  -- Impression share of full package total
      SAFE_DIVIDE(bd.impressions, 
        
        dr.total_inflight_impressions) AS pkg_inflight_imps_perc,

    -- ───────────────────────────────────────────────
    -- [days_live]: days with in-flight delivery
    DATE_DIFF(dr.max_flight_date, dr.min_flight_date, DAY) + 1 AS days_live,

    -- ───────────────────────────────────────────────
    -- [prorated_planned_cost_pk]: based on actual active days
    CASE
      WHEN bd.p_total_days IS NULL OR bd.p_total_days = 0 THEN NULL
      WHEN flight_status_flag = 'live' 
        THEN (bd.p_pkg_total_planned_cost/bd.p_total_days) *
        (DATE_DIFF(dr.d_max_date, dr.d_min_date, DAY) + 1) -- active days 
      ELSE  
        LEAST(
          DATE_DIFF(dr.d_max_date, dr.d_min_date, DAY) + 1, -- active days
          bd.p_total_days
          )     
        *                              -- planned days 
        (bd.p_pkg_total_planned_cost / 
        LEAST(DATE_DIFF(dr.d_max_date, dr.d_min_date, DAY) + 1,bd.p_total_days)
          )
    END AS prorated_planned_cost_pk,

    -- [prorated_planned_imps_pk]: same logic
    CASE
      WHEN bd.p_total_days IS NULL OR bd.p_total_days = 0 THEN NULL
      ELSE
        LEAST(
          DATE_DIFF(dr.d_max_date, dr.d_min_date, DAY) + 1,
          bd.p_total_days
        ) * bd.p_pkg_total_planned_imps / bd.p_total_days
    END AS prorated_planned_imps_pk,

    -- ───────────────────────────────────────────────
    -- Flags

    -- -- Is the delivery date within the planned flight window?
    -- CASE
    --   WHEN bd.date BETWEEN bd.p_start_date AND bd.p_end_date THEN 0
    --   ELSE 1
    -- END AS flight_date_flag,

    -- Is the package currently live?
    -- CASE
    --   WHEN CURRENT_DATE() BETWEEN bd.p_start_date AND bd.p_end_date THEN 'live'
    --   ELSE 'ended'
    -- END AS flight_status_flag,

    -- CPM Overdelivery Flag
    CASE
      WHEN dr.total_impressions > bd.p_pkg_total_planned_imps
           AND bd.p_cost_method = 'CPM' THEN 1
      ELSE 0
    END AS cpm_overdelivery_flag,

    -- Rate pulled from package_roadblock
    --REGEXP_EXTRACT(bd.package_roadblock, r'(\d+\.?\d*)\D*$') AS rate_raw,

    -- Estimated CPM (optional QA metric)
    SAFE_DIVIDE(bd.p_pkg_daily_planned_cost, bd.impressions) * 1000 AS daily_cpm

  FROM base_delivery_metadata bd
  LEFT JOIN delivery_rollups dr
    ON bd.package_id = dr.package_id
),

-- 4. [ cm. ]  COST LOGIC (PRICING ENGINE)
cost_model AS (
  /*
    Inputs: calculated_fields
    Outputs: daily cost per row + logic path flag
    Handles: CPM, CPC, CPA, Flat
  */
  SELECT
    package_id,
    placement_id,
    date,

    -- For output/debugging
    pkg_total_imps,
    total_inflight_impressions,
    impressions,
    clicks,
    total_conversions,
    rate_raw,
    p_cost_method,
    p_pkg_total_planned_cost,
    p_pkg_total_planned_imps,
    p_total_days,
    prorated_planned_cost_pk,
    flight_date_flag,

    -- ───────────────────────────────────────────────
    -- Pricing logic
    CASE
      WHEN flight_date_flag = 1 THEN 0

      -- CPM models
      WHEN p_cost_method = 'CPM' THEN
        CASE
          WHEN total_inflight_impressions > SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC)
            THEN SAFE_DIVIDE(p_pkg_total_planned_cost, total_inflight_impressions) * impressions
          ELSE SAFE_CAST(rate_raw AS NUMERIC) * impressions / 1000
        END

      -- CPC
      WHEN p_cost_method = 'CPC'
        THEN SAFE_CAST(rate_raw AS NUMERIC) * clicks

      -- CPA
      WHEN p_cost_method = 'CPA'
        THEN SAFE_CAST(rate_raw AS NUMERIC) * total_conversions

      -- Flat logic
      WHEN p_cost_method = 'Flat' THEN
        CASE
          WHEN SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0
            THEN (prorated_planned_cost_pk * SAFE_CAST(pkg_inflight_imps_perc AS NUMERIC))
          ELSE SAFE_CAST(prorated_planned_cost_pk AS NUMERIC) *
               SAFE_DIVIDE(impressions, NULLIF(total_inflight_impressions, 0))
        END

      ELSE 0
    END AS daily_recalculated_cost,

    -- ───────────────────────────────────────────────
    -- Debug path
    CASE
      WHEN flight_date_flag = 1 THEN 'OUT_OF_FLIGHT'
      WHEN p_cost_method = 'CPM' AND pkg_total_imps > SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) THEN 'CPM_OVERDELIVERY'
      WHEN p_cost_method = 'CPM' THEN 'CPM_STANDARD'
      WHEN p_cost_method = 'CPC' THEN 'CPC'
      WHEN p_cost_method = 'CPA' THEN 'CPA'
      WHEN p_cost_method = 'Flat' AND SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0 THEN 'FLAT_DAILY'
      WHEN p_cost_method = 'Flat' THEN 'FLAT_RATIO'
      ELSE 'UNKNOWN'
    END AS daily_recalculated_cost_flag,

    case
      when flight_date_flag = 1 then 0
      else impressions
    end as daily_recalculated_imps

  FROM calculated_fields
)

-- 5. FINAL OUTPUT: APPEND COSTS BACK TO ROWS
SELECT
  cf.*,
  cm.daily_recalculated_cost,
  cm.daily_recalculated_cost_flag,
  cm.daily_recalculated_imps
FROM calculated_fields cf
LEFT JOIN cost_model cm
  ON cf.package_id = cm.package_id
  AND cf.placement_id = cm.placement_id
  AND cf.date = cm.date
ORDER BY cf.impressions DESC

--------------- V1 out-of-date ------------
-- WITH

-- -- 1. JOIN RAW DELIVERY + METADATA (one source of truth)
-- base_delivery_metadata AS (
--   /*
--     Joins raw delivery to Prisma metadata.
--     Applies 60-day email flight extension.
--     Filters to only rows with impressions > 0.
--     Aliases Prisma fields with `p_` prefix.
--   */
--   SELECT
--     -- Delivery fields
--     a.date,
--     a.campaign,
--     a.package_roadblock,
--     a.package_id,
--     a.placement_id,
--     a.impressions,
--     a.`KEY`,
--     a.`ad`,
--     a.`click_rate`,
--     a.`clicks`,
--     a.`creative`,
--     a.`media_cost`,
--     a.`rich_media_video_completions`,
--     a.`rich_media_video_plays`,
--     a.`total_conversions`,

--     -- Prisma metadata fields with aliases
--     b.cost_method AS p_cost_method,
--     b.p_package_friendly,
--     b.start_date AS p_start_date,
--     CASE
--       WHEN a.package_roadblock LIKE '%email%' THEN DATE_ADD(b.end_date, INTERVAL 60 DAY)
--       ELSE b.end_date
--     END AS p_end_date,
--     b.total_days AS p_total_days,
--     b.planned_daily_spend_pk AS p_pkg_daily_planned_cost,
--     b.planned_cost_pk AS p_pkg_total_planned_cost,
--     b.planned_daily_impressions_pk AS p_pkg_daily_planned_imps,
--     b.planned_imps_pk AS p_pkg_total_planned_imps,
--     b.channel_group AS p_channel_group,
--     b.advertiser_name AS p_advertiser_name

--   FROM `giant-spoon-299605.data_model_2025.new_md` a
--   LEFT JOIN `looker-studio-pro-452620.20250327_data_model.prisma_porcessed` b
--     ON a.package_id = b.package_id
--   WHERE a.impressions > 0
-- ),

-- -- 2. PRECOMPUTED PACKAGE-LEVEL METRICS
-- delivery_rollups AS (
--   /*
--     Computes package-level aggregates used in multiple downstream CTEs:
--     - Total impressions
--     - Distinct placements
--     - Min/max dates inside and outside flight window
--   */
--   SELECT
--     package_id,

--     -- Actual min/max delivery dates
--     MIN(date) AS d_min_date,
--     MAX(date) AS d_max_date,

--     -- Min/max dates within the official flight window
--     MIN(CASE WHEN date BETWEEN p_start_date AND p_end_date THEN date ELSE NULL END) AS min_flight_date,
--     MAX(CASE WHEN date BETWEEN p_start_date AND p_end_date THEN date ELSE NULL END) AS max_flight_date,

--     SUM(impressions) AS total_impressions,
--     COUNT(DISTINCT placement_id) AS n_of_placements

--   FROM base_delivery_metadata
--   GROUP BY package_id
-- ),

-- -- 3. CALCULATED FLAGS + PRORATED FIELDS
-- calculated_fields AS (
--   /*
--     Adds:
--       - Package-level impression %s
--       - Flags: flight, overdelivery
--       - Prorated cost & imps
--       - days_live, flight status
--       - rate extraction
--   */
--   SELECT
--     bd.*,

--     -- ───────────────────────────────────────────────
--     -- From delivery_rollups
--     dr.n_of_placements,
--     dr.d_min_date,
--     dr.d_max_date,
--     dr.min_flight_date,
--     dr.max_flight_date,
--     dr.total_impressions AS pkg_total_imps,

--     -- ───────────────────────────────────────────────
--     -- Windowed daily aggregates
--     SUM(bd.impressions) OVER (PARTITION BY bd.package_id, bd.date) AS pkg_daily_imps,

--     -- Impression share of daily package total
--     SAFE_DIVIDE(
--       bd.impressions,
--       SUM(bd.impressions) OVER (PARTITION BY bd.package_id, bd.date)
--     ) AS pkg_daily_imps_perc,

--     -- Impression share of full package total
--     SAFE_DIVIDE(bd.impressions, dr.total_impressions) AS pkg_total_imps_perc,

--     -- ───────────────────────────────────────────────
--     -- [days_live]: days with in-flight delivery
--     DATE_DIFF(dr.max_flight_date, dr.min_flight_date, DAY) + 1 AS days_live,

--     -- ───────────────────────────────────────────────
--     -- [prorated_planned_cost_pk]: based on actual active days
--     CASE
--       WHEN bd.p_total_days IS NULL OR bd.p_total_days = 0 THEN NULL
--       ELSE
--         LEAST(
--           DATE_DIFF(dr.d_max_date, dr.d_min_date, DAY) + 1,
--           bd.p_total_days
--         ) * bd.p_pkg_total_planned_cost / bd.p_total_days
--     END AS prorated_planned_cost_pk,

--     -- [prorated_planned_imps_pk]: same logic
--     CASE
--       WHEN bd.p_total_days IS NULL OR bd.p_total_days = 0 THEN NULL
--       ELSE
--         LEAST(
--           DATE_DIFF(dr.d_max_date, dr.d_min_date, DAY) + 1,
--           bd.p_total_days
--         ) * bd.p_pkg_total_planned_imps / bd.p_total_days
--     END AS prorated_planned_imps_pk,

--     -- ───────────────────────────────────────────────
--     -- Flags

--     -- Is the delivery date within the planned flight window?
--     CASE
--       WHEN bd.date BETWEEN bd.p_start_date AND bd.p_end_date THEN 0
--       ELSE 1
--     END AS flight_date_flag,

--     -- Is the package currently live?
--     CASE
--       WHEN CURRENT_DATE() BETWEEN bd.p_start_date AND bd.p_end_date THEN 'live'
--       ELSE 'ended'
--     END AS flight_status_flag,

--     -- CPM Overdelivery Flag
--     CASE
--       WHEN dr.total_impressions > bd.p_pkg_total_planned_imps
--            AND bd.p_cost_method = 'CPM' THEN 1
--       ELSE 0
--     END AS cpm_overdelivery_flag,

--     -- Rate pulled from package_roadblock
--     REGEXP_EXTRACT(bd.package_roadblock, r'(\d+\.?\d*)\D*$') AS rate_raw,

--     -- Estimated CPM (optional QA metric)
--     SAFE_DIVIDE(bd.p_pkg_daily_planned_cost, bd.impressions) * 1000 AS daily_cpm

--   FROM base_delivery_metadata bd
--   LEFT JOIN delivery_rollups dr
--     ON bd.package_id = dr.package_id
-- ),

-- -- 4. COST LOGIC (PRICING ENGINE)
-- cost_model AS (
--   /*
--     Inputs: calculated_fields
--     Outputs: daily cost per row + logic path flag
--     Handles: CPM, CPC, CPA, Flat
--   */
--   SELECT
--     package_id,
--     placement_id,
--     date,

--     -- For output/debugging
--     pkg_total_imps,
--     impressions,
--     clicks,
--     total_conversions,
--     rate_raw,
--     p_cost_method,
--     p_pkg_total_planned_cost,
--     p_pkg_total_planned_imps,
--     p_total_days,
--     prorated_planned_cost_pk,
--     flight_date_flag,

--     -- ───────────────────────────────────────────────
--     -- Pricing logic
--     CASE
--       WHEN flight_date_flag = 1 THEN 0

--       -- CPM models
--       WHEN p_cost_method = 'CPM' THEN
--         CASE
--           WHEN pkg_total_imps > SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC)
--             THEN SAFE_DIVIDE(p_pkg_total_planned_cost, pkg_total_imps) * impressions
--           ELSE SAFE_CAST(rate_raw AS NUMERIC) * impressions / 1000
--         END

--       -- CPC
--       WHEN p_cost_method = 'CPC'
--         THEN SAFE_CAST(rate_raw AS NUMERIC) * clicks

--       -- CPA
--       WHEN p_cost_method = 'CPA'
--         THEN SAFE_CAST(rate_raw AS NUMERIC) * total_conversions

--       -- Flat logic
--       WHEN p_cost_method = 'Flat' THEN
--         CASE
--           WHEN SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0
--             THEN SAFE_DIVIDE(p_pkg_total_planned_cost, SAFE_CAST(p_total_days AS NUMERIC))
--           ELSE SAFE_CAST(prorated_planned_cost_pk AS NUMERIC) *
--                SAFE_DIVIDE(impressions, pkg_total_imps)
--         END

--       ELSE 0
--     END AS daily_recalculated_cost,

--     -- ───────────────────────────────────────────────
--     -- Debug path
--     CASE
--       WHEN flight_date_flag = 1 THEN 'OUT_OF_FLIGHT'
--       WHEN p_cost_method = 'CPM' AND pkg_total_imps > SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) THEN 'CPM_OVERDELIVERY'
--       WHEN p_cost_method = 'CPM' THEN 'CPM_STANDARD'
--       WHEN p_cost_method = 'CPC' THEN 'CPC'
--       WHEN p_cost_method = 'CPA' THEN 'CPA'
--       WHEN p_cost_method = 'Flat' AND SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0 THEN 'FLAT_DAILY'
--       WHEN p_cost_method = 'Flat' THEN 'FLAT_RATIO'
--       ELSE 'UNKNOWN'
--     END AS daily_recalculated_cost_flag,

--     case
--       when flight_date_flag = 1 then 0
--       else impressions
--     end as daily_recalculated_imps

--   FROM calculated_fields
-- )

-- -- 5. FINAL OUTPUT: APPEND COSTS BACK TO ROWS
-- SELECT
--   cf.*,
--   cm.daily_recalculated_cost,
--   cm.daily_recalculated_cost_flag
-- FROM calculated_fields cf
-- LEFT JOIN cost_model cm
--   ON cf.package_id = cm.package_id
--   AND cf.placement_id = cm.placement_id
--   AND cf.date = cm.date
-- ORDER BY cf.impressions DESC




-- WITH

-- --! 1. [  bd. ] JOIN RAW DELIVERY + METADATA (one source of truth)
-- base_delivery_metadata AS (
--   /*
--     Joins raw delivery to Prisma metadata.
--     Applies 60-day email flight extension.
--     Filters to only rows with impressions > 0.
--     Aliases Prisma fields with `p_` prefix.
--     Adds static flags: flight_date_flag, flight_status_flag, rate_raw
--   */
--   SELECT 
--   -- Delivery fields
--     a.date,
--     a.campaign,
--     a.package_roadblock,
--     a.package_id,
--     a.placement_id,
--     a.impressions,
--     a.`KEY`,
--     a.`ad`,
--     a.`click_rate`,
--     a.`clicks`,
--     a.`creative`,
--     a.`media_cost`,
--     a.`rich_media_video_completions`,
--     a.`rich_media_video_plays`,
--     a.`total_conversions`,

--   -- Prisma metadata fields with aliases
--     b.cost_method AS p_cost_method,
--     b.p_package_friendly,
--     b.start_date AS p_start_date,

--   -- [p_end_date] Extended flight window for email packages (+60 days)
--     CASE
--       WHEN a.package_roadblock LIKE '%email%' THEN DATE_ADD(b.end_date, INTERVAL 60 DAY)
--       ELSE b.end_date
--     END AS p_end_date,

--     b.total_days AS p_total_days,
--     b.planned_daily_spend_pk AS p_pkg_daily_planned_cost,
--     b.planned_cost_pk AS p_pkg_total_planned_cost,
--     b.planned_daily_impressions_pk AS p_pkg_daily_planned_imps,
--     b.planned_imps_pk AS p_pkg_total_planned_imps,
--     b.channel_group AS p_channel_group,
--     b.advertiser_name AS p_advertiser_name,

--   -- [flight_date_flag] 0 = in-flight, 1 = out-of-flight (based on extended window)
--     CASE
--       WHEN a.date BETWEEN b.start_date AND
--            CASE WHEN a.package_roadblock LIKE '%email%' THEN DATE_ADD(b.end_date, INTERVAL 60 DAY)
--                 ELSE b.end_date END
--       THEN 0 ELSE 1
--     END AS flight_date_flag,

--   -- [flight_status_flag] 'live' if today is within the flight window, else 'ended'
--     CASE
--       WHEN CURRENT_DATE() BETWEEN b.start_date AND
--            CASE WHEN a.package_roadblock LIKE '%email%' THEN DATE_ADD(b.end_date, INTERVAL 60 DAY)
--                 ELSE b.end_date END
--       THEN 'live' ELSE 'ended'
--     END AS flight_status_flag,

--   -- [rate_raw] Extracted numeric rate value from end of package_roadblock string
--     REGEXP_EXTRACT(a.package_roadblock, r'(\d+\.?\d*)\D*$') AS rate_raw

--   FROM `giant-spoon-299605.data_model_2025.new_md` a
--   LEFT JOIN `looker-studio-pro-452620.20250327_data_model.prisma_porcessed` b
--     ON a.package_id = b.package_id
--   WHERE a.impressions > 0
-- ),

-- --! 2. [ dr. ] delivery rollup |  PRECOMPUTED PACKAGE-LEVEL METRICS
-- delivery_rollups AS (
--   /*
--     Computes package-level aggregates used in multiple downstream CTEs:
--     - Total impressions
--     - Distinct placements
--     - Min/max dates inside and outside flight window
--   */
--   SELECT
--     package_id,

--     -- Actual min/max delivery dates
--     MIN(date) AS d_min_date,
--     MAX(date) AS d_max_date,

--     -- Min/max dates within the official flight window
--     MIN(CASE WHEN date BETWEEN p_start_date AND p_end_date THEN date ELSE NULL END) AS min_flight_date,
--     MAX(CASE WHEN date BETWEEN p_start_date AND p_end_date THEN date ELSE NULL END) AS max_flight_date,

--     SUM(impressions) AS total_impressions,
--     COUNT(DISTINCT placement_id) AS n_of_placements,

--     SUM(CASE
--       WHEN date BETWEEN p_start_date AND p_end_date THEN impressions
--       ELSE 0
--     END) AS total_inflight_impressions

--   FROM base_delivery_metadata
--   GROUP BY package_id
-- ),

-- -- 3. [ cf. ] CALCULATED FLAGS + PRORATED FIELDS
-- calculated_fields AS (
--   /*
--     Adds:
--       - Package-level impression %s
--       - Flags: flight, overdelivery
--       - Prorated cost & imps
--       - days_live, flight status
--       - rate extraction
--   */
--   SELECT
--     bd.*,

--     -- ───────────────────────────────────────────────
--     -- From delivery_rollups
--     dr.n_of_placements,
--     dr.d_min_date,
--     dr.d_max_date,
--     dr.min_flight_date,
--     dr.max_flight_date,
--     dr.total_impressions AS pkg_total_imps,
--     dr.total_inflight_impressions,

--     -- ───────────────────────────────────────────────
--     -- Windowed daily aggregates
--     SUM(bd.impressions) OVER (PARTITION BY bd.package_id, bd.date) AS pkg_daily_imps,

--     -- Impression share of daily package total
--     SAFE_DIVIDE(
--       bd.impressions,
--       SUM(bd.impressions) OVER (PARTITION BY bd.package_id, bd.date)
--     ) AS pkg_daily_imps_perc,

--     -- Impression share of full package total
--     SAFE_DIVIDE(bd.impressions, dr.total_impressions) AS pkg_total_imps_perc,

--     -- ───────────────────────────────────────────────
--     -- [days_live]: days with in-flight delivery
--     DATE_DIFF(dr.max_flight_date, dr.min_flight_date, DAY) + 1 AS days_live,

--     -- ───────────────────────────────────────────────
--     -- [prorated_planned_cost_pk]: based on actual active days
--     CASE
--       WHEN bd.p_total_days IS NULL OR bd.p_total_days = 0 THEN NULL
--       WHEN flight_status_flag = 'live' 
--         THEN (bd.p_pkg_total_planned_cost/bd.p_total_days) *
--         (DATE_DIFF(dr.d_max_date, dr.d_min_date, DAY) + 1) -- active days 
--       ELSE  
--         LEAST(
--           DATE_DIFF(dr.d_max_date, dr.d_min_date, DAY) + 1, -- active days
--           bd.p_total_days
--           )     
--         *                              -- planned days 
--         (bd.p_pkg_total_planned_cost / 
--         LEAST(DATE_DIFF(dr.d_max_date, dr.d_min_date, DAY) + 1,bd.p_total_days)
--           )
--     END AS prorated_planned_cost_pk,

--     -- [prorated_planned_imps_pk]: same logic
--     CASE
--       WHEN bd.p_total_days IS NULL OR bd.p_total_days = 0 THEN NULL
--       ELSE
--         LEAST(
--           DATE_DIFF(dr.d_max_date, dr.d_min_date, DAY) + 1,
--           bd.p_total_days
--         ) * bd.p_pkg_total_planned_imps / bd.p_total_days
--     END AS prorated_planned_imps_pk,

--     -- ───────────────────────────────────────────────
--     -- Flags

--     -- -- Is the delivery date within the planned flight window?
--     -- CASE
--     --   WHEN bd.date BETWEEN bd.p_start_date AND bd.p_end_date THEN 0
--     --   ELSE 1
--     -- END AS flight_date_flag,

--     -- Is the package currently live?
--     -- CASE
--     --   WHEN CURRENT_DATE() BETWEEN bd.p_start_date AND bd.p_end_date THEN 'live'
--     --   ELSE 'ended'
--     -- END AS flight_status_flag,

--     -- CPM Overdelivery Flag
--     CASE
--       WHEN dr.total_impressions > bd.p_pkg_total_planned_imps
--            AND bd.p_cost_method = 'CPM' THEN 1
--       ELSE 0
--     END AS cpm_overdelivery_flag,

--     -- Rate pulled from package_roadblock
--     --REGEXP_EXTRACT(bd.package_roadblock, r'(\d+\.?\d*)\D*$') AS rate_raw,

--     -- Estimated CPM (optional QA metric)
--     SAFE_DIVIDE(bd.p_pkg_daily_planned_cost, bd.impressions) * 1000 AS daily_cpm

--   FROM base_delivery_metadata bd
--   LEFT JOIN delivery_rollups dr
--     ON bd.package_id = dr.package_id
-- ),

-- -- 4. [ cm. ]  COST LOGIC (PRICING ENGINE)
-- cost_model AS (
--   /*
--     Inputs: calculated_fields
--     Outputs: daily cost per row + logic path flag
--     Handles: CPM, CPC, CPA, Flat
--   */
--     SELECT
--     -- Inputs 
--       package_id,
--       placement_id,
--       ad,
--       date,

--       -- For output/debugging
--       pkg_total_imps,
--       total_inflight_impressions,
--       impressions,
--       clicks,
--       total_conversions,
--       rate_raw,
--       p_cost_method,
--       p_pkg_total_planned_cost,
--       p_pkg_total_planned_imps,
--       p_total_days,
--       prorated_planned_cost_pk,
--       flight_date_flag,

--     -- ───────────────────────────────────────────────
--     -- Pricing logic
--     CASE
--       WHEN flight_date_flag = 1 THEN 0

--       -- CPM models
--       WHEN p_cost_method = 'CPM' THEN
--         CASE
--           WHEN total_inflight_impressions > SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC)
--             THEN SAFE_DIVIDE(p_pkg_total_planned_cost, total_inflight_impressions) * impressions
--           ELSE SAFE_CAST(rate_raw AS NUMERIC) * impressions / 1000
--         END

--       -- CPC
--       WHEN p_cost_method = 'CPC'
--         THEN SAFE_CAST(rate_raw AS NUMERIC) * clicks

--       -- CPA
--       WHEN p_cost_method = 'CPA'
--         THEN SAFE_CAST(rate_raw AS NUMERIC) * total_conversions

--       -- FLAT FEE MODEL - Distribution approaches for fixed-cost campaigns
--       -- BUSINESS CONTEXT: Flat fee campaigns have a fixed total cost that must be 
--       -- distributed across placements based on some allocation method
--       WHEN p_cost_method = 'Flat' THEN
--         CASE
--           -- SPECIAL CASE: When no planned impressions exist (brand awareness campaigns, etc.)
--           -- Historical context: V1 used a pure daily distribution approach:
--           -- SAFE_DIVIDE(p_pkg_total_planned_cost, SAFE_CAST(p_total_days AS NUMERIC))
--           WHEN SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0
--             -- Current approach (V2) uses pkg_total_imps_perc which is:
--             -- SAFE_DIVIDE(bd.impressions, dr.total_impressions)
--             -- This distributes costs based on placement's share of ALL campaign impressions
--             -- PROS: More stable, less day-to-day fluctuation
--             -- CONS: May not reflect daily delivery patterns
--             --
--             -- ALTERNATIVE OPTION: Could use pkg_daily_imps_perc instead:
--             -- SAFE_DIVIDE(bd.impressions, SUM(bd.impressions) OVER (PARTITION BY bd.package_id, bd.date))
--             -- This would distribute costs based on placement's share of DAILY impressions
--             -- PROS: Better reflects day-by-day value delivered, matches "FLAT_DAILY" flag
--             -- CONS: More volatile, can produce uneven cost distribution across days
            
            
--             THEN (prorated_planned_cost_pk * SAFE_CAST(pkg_total_imps_perc AS NUMERIC))
--             --THEN (prorated_planned_cost_pk * SAFE_CAST(pkg_daily_imps_perc AS NUMERIC))
            
--           -- STANDARD CASE: When planned impressions exist (normal flat fee campaigns)
--           ELSE 
--             -- V2 ENHANCEMENT: Uses total_inflight_impressions (within flight window)
--             -- instead of V1's pkg_total_imps (all impressions including out-of-flight)
--             -- This ensures only impressions within the intended campaign period
--             -- are used for cost attribution
--             SAFE_CAST(prorated_planned_cost_pk AS NUMERIC) *
--             -- Protect against division by zero with NULLIF()
--             SAFE_DIVIDE(impressions, NULLIF(total_inflight_impressions, 0))
--         END
        

--       ELSE 0
--     END AS daily_recalculated_cost,

--     -- ───────────────────────────────────────────────
--     -- Debug path
--     CASE
--       WHEN flight_date_flag = 1 THEN 'OUT_OF_FLIGHT'
--       WHEN p_cost_method = 'CPM' AND pkg_total_imps > SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) THEN 'CPM_OVERDELIVERY'
--       WHEN p_cost_method = 'CPM' THEN 'CPM_STANDARD'
--       WHEN p_cost_method = 'CPC' THEN 'CPC'
--       WHEN p_cost_method = 'CPA' THEN 'CPA'
--       WHEN p_cost_method = 'Flat' AND SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0 THEN 'FLAT_DAILY'
--       WHEN p_cost_method = 'Flat' THEN 'FLAT_RATIO'
--       ELSE 'UNKNOWN'
--     END AS daily_recalculated_cost_flag,

--     case
--       when flight_date_flag = 1 then 0
--       else impressions
--     end as daily_recalculated_imps

--   FROM calculated_fields
-- )

-- -- 5. FINAL OUTPUT: APPEND COSTS BACK TO ROWS
-- SELECT
--   cf.*,
--   cm.daily_recalculated_cost,
--   cm.daily_recalculated_cost_flag
-- FROM calculated_fields cf
-- LEFT JOIN cost_model cm
--   ON cf.package_id = cm.package_id
--   AND cf.placement_id = cm.placement_id
--   and cf.ad = cm.ad
--   AND cf.date = cm.date
-- ORDER BY cf.impressions DESC