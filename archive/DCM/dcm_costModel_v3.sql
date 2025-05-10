-- /* v2 - 250429
/* COST_MODEL CTE ******************************************************
 * 
 * Calculates a daily, placement-level cost for digital ad campaigns.
 *   • Zeros cost outside the flight window (no spend before/after campaign)
 *   • Caps CPM spend if the package over-delivers impressions
 *   • Handles multiple pricing models: CPM (cost per mille), CPC (cost per click), CPA (cost per acquisition), and Flat
 *   • Emits a debug flag for QA to trace which logic path was used
 * 
 * CTE (Common Table Expression) is used here to modularize the cost calculation logic,
 * making the query more readable and maintainable.
 *************************************************************/

WITH base_flags AS (

  /* BASE_FLAGS CTE ******************************************************
   *
   * Calculates flags needed for downstream cost model calculations:
   *   • flight_date_flag = Whether the date is inside the flight window
   *   • daily_recalculated_cost_flag = Which cost logic branch was taken
   *
   * This isolates flag calculations so they can be referenced cleanly later.
   *************************************************************/

  SELECT
  -- [based fields]
    package_id,
    placement_id,
    date,
  -- Flag Calculations
    -- [flight_date_flag] 1 = out-of-flight, 0 = in-flight
      -- Flag to indicate if the current row's date is outside the campaign's flight window.
      CASE
      WHEN date < p_start_date OR date > p_end_date THEN 1
      ELSE 0
      END AS flight_date_flag,

    --

    -- [daily_recalculated_cost_flag] 
      -- ───────── COST LOGIC FLAG (for QA) ─────────
      -- Emits a flag indicating which cost calculation path was used for this row, for debugging and QA.
      -- The flag helps trace which branch of the cost logic was applied.
      CASE
        WHEN date < p_start_date OR date > p_end_date -- No cost: date is outside the campaign window
          THEN 'OUT_OF_FLIGHT'   
        WHEN p_cost_method = 'CPM' -- CPM: Over-delivery, cost capped at planned package cost
          AND SUM(SUM(impressions)) OVER (PARTITION BY package_id) > SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC)
          THEN 'CPM_OVERDELIVERY' 
        WHEN p_cost_method = 'CPM' -- CPM: Standard calculation (no over-delivery)
          THEN 'CPM_STANDARD'     
        WHEN p_cost_method = 'CPC'
          THEN 'CPC'              -- CPC: Cost per click calculation
        WHEN p_cost_method = 'CPA'
          THEN 'CPA'              -- CPA: Cost per acquisition calculation
        WHEN p_cost_method = 'Flat' -- Flat: Even daily distribution (no planned impressions)
          AND SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0
          THEN 'FLAT_DAILY'       
        WHEN p_cost_method = 'Flat' -- Flat: Prorated by delivered impressions
          THEN 'FLAT_RATIO'       
        ELSE 'UNKNOWN'            -- Fallback: Unexpected or unhandled case
      END AS daily_recalculated_cost_flag
      --

    --
    
  
  FROM `looker-studio-pro-452620.DCM.dcm_linkedView2`

  GROUP BY
    package_id,
    placement_id,
    date,
    p_cost_method,
    p_pkg_total_planned_imps,
    p_start_date,
    p_end_date

),

cost_model AS (

  /* COST_MODEL CTE (Main) ******************************************************
   *
   * Pulls in raw delivery + metadata, applies flags, and calculates daily recalculated cost.
   * References base_flags for modularity and readability.
   *************************************************************/

  SELECT
    -- ─────────────────── GROUPED FIELDS (dimensions) ───────────────────
    a.package_id,                    
    a.placement_id,                  
    a.date,                          
    a.p_cost_method,                 
    a.rate_raw,                      
    a.p_pkg_total_planned_cost,      
    a.p_pkg_total_planned_imps,      
    a.p_total_days,                  
    a.p_start_date,                  
    a.p_end_date,                    
    a.prorated_planned_imps_pk,      
    a.prorated_planned_cost_pk,      

    -- ─────────────────── AGGREGATED DELIVERY METRICS ───────────────────
    SUM(a.impressions) AS impressions,            
    SUM(a.clicks) AS clicks,                      
    SUM(a.total_conversions) AS total_conversions,

    -- [pkg_total_delivered_imps] Package-level delivered impressions (window over aggregate)
      -- Double aggregation: SUM(impressions) is daily, then SUM(SUM(impressions)) OVER (PARTITION BY package_id)
      -- computes the total delivered impressions for the entire package (across all placements and dates).
    SUM(SUM(a.impressions)) OVER (PARTITION BY a.package_id) AS pkg_total_delivered_imps,

    -- Bring in flight date flag from base_flags
    f.flight_date_flag,

    -- [daily_recalculated_cost] ───────── DAILY COST CALCULATION (per pricing model) [ !!! FLAT RATE IS WIP !!! ] ─────────
      -- Calculates the daily cost for this placement/date, based on the pricing model and business rules.
    CASE
      -- Out-of-flight rows incur no cost
      WHEN f.flight_date_flag = 1 THEN 0

      -- CPM: cap cost if package over-delivers
      -- If the package delivers more impressions than planned, cap total spend at planned cost.
      -- Otherwise, use the standard CPM calculation.
      WHEN a.p_cost_method = 'CPM' THEN
        CASE
          -- If total delivered impressions for the package exceed planned, cap total spend at planned cost.
          WHEN SUM(SUM(a.impressions)) OVER (PARTITION BY a.package_id) > SAFE_CAST(a.p_pkg_total_planned_imps AS NUMERIC)
            /* Adjusted CPM = planned_cost / delivered_imps. 
               Each placement's cost is proportional to its share of delivered impressions. */
            THEN SAFE_DIVIDE(
                   SAFE_CAST(a.p_pkg_total_planned_cost AS NUMERIC),   
                   SUM(SUM(a.impressions)) OVER (PARTITION BY a.package_id) 
                 ) * SUM(a.impressions)                                
          /* Standard CPM = rate * imps / 1000 */
          ELSE SAFE_CAST(a.rate_raw AS NUMERIC) * SUM(a.impressions) / 1000
        END

      -- CPC: rate * clicks
      WHEN a.p_cost_method = 'CPC'
        THEN SAFE_CAST(a.rate_raw AS NUMERIC) * SUM(a.clicks)

      -- CPA: rate * conversions
      WHEN a.p_cost_method = 'CPA'
        THEN SAFE_CAST(a.rate_raw AS NUMERIC) * SUM(a.total_conversions)

      -- Flat-rate logic [ !!! FLAT RATE IS WIP !!! ]
      -- Two scenarios:
      --   1. If there are no planned impressions, distribute the planned cost evenly across all days in the campaign.
      --   2. If planned impressions exist, allocate cost to each placement based on its share of delivered impressions.
      WHEN a.p_cost_method = 'Flat' THEN
        CASE
          -- Even daily distribution when no planned impressions
          WHEN SAFE_CAST(a.p_pkg_total_planned_imps AS NUMERIC) = 0
            THEN SAFE_DIVIDE(
                   SAFE_CAST(a.p_pkg_total_planned_cost AS NUMERIC),  
                   SAFE_CAST(a.p_total_days AS NUMERIC)               
                 )
          -- Otherwise prorated by share of impressions
          ELSE SAFE_CAST(a.prorated_planned_cost_pk AS NUMERIC) *     
               SAFE_DIVIDE(
                SUM(a.impressions),                                   
                SUM(SUM(a.impressions)) OVER (PARTITION BY a.package_id) 
               )
        END

      -- Fallback for unknown cost models
      ELSE 0   
    END AS daily_recalculated_cost,

    -- Bring in cost logic flag from base_flags
    f.daily_recalculated_cost_flag

  FROM `looker-studio-pro-452620.DCM.dcm_linkedView2` a

  LEFT JOIN base_flags f
    ON a.package_id = f.package_id
    AND a.placement_id = f.placement_id
    AND a.date = f.date

  GROUP BY
    a.package_id,
    a.placement_id,
    a.date,
    a.p_cost_method,
    a.rate_raw,
    a.p_pkg_total_planned_cost,
    a.p_pkg_total_planned_imps,
    a.p_total_days,
    a.p_start_date,
    a.p_end_date,
    a.prorated_planned_imps_pk,
    a.prorated_planned_cost_pk,
    f.flight_date_flag,
    f.daily_recalculated_cost_flag
)

-- ─────────────────── FINAL SELECT (join back to raw rows) ───────────────────
-- Join the calculated cost_model CTE back to the raw data to append cost and QA fields to each row.
SELECT
  a.* EXCEPT (flight_status_flag_dcm),                                    
  c.pkg_total_delivered_imps,             
  c.daily_recalculated_cost,              
  c.daily_recalculated_cost_flag,         
  c.flight_date_flag AS flight_date_flag2  

FROM `looker-studio-pro-452620.DCM.dcm_linkedView2` a
LEFT JOIN cost_model c
  ON a.package_id = c.package_id
  AND a.placement_id = c.placement_id
  AND a.date = c.date
-- WHERE a.p_cost_method = 'Flat'
-- Filter: Only include rows for the specified package (business logic filter)

----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------

-- /* v1 - 250428
-- /* COST_MODEL CTE  ******************************************************
--  * 
--  * Calculates a daily, placement-level cost for digital ad campaigns.
--  *   • Zeros cost outside the flight window (no spend before/after campaign)
--  *   • Caps CPM spend if the package over-delivers impressions
--  *   • Handles multiple pricing models: CPM (cost per mille), CPC (cost per click), CPA (cost per acquisition), and Flat
--  *   • Emits a debug flag for QA to trace which logic path was used
--  * 
--  * CTE (Common Table Expression) is used here to modularize the cost calculation logic,
--  * making the query more readable and maintainable.
--  *************************************************************/

-- WITH cost_model AS (

--   SELECT
--     -- ─────────────────── GROUPED FIELDS (dimensions) ───────────────────
--     package_id,                    -- Unique identifier for the ad package (group of placements)
--     placement_id,                  -- Unique identifier for the ad placement (individual ad slot)
--     date,                          -- Date of delivery (granularity: daily)
--     p_cost_method,                 -- Pricing model for the placement ('CPM', 'CPC', 'CPA', 'Flat')
--     rate_raw,                      -- Raw rate for the placement (e.g., CPM rate, CPC rate, etc.)
--     p_pkg_total_planned_cost,      -- Total planned cost for the package (used for capping/cost allocation)
--     p_pkg_total_planned_imps,      -- Total planned impressions for the package (used for capping/ratios)
--     p_total_days,                  -- Total number of days in the campaign flight window
--     p_start_date,                  -- Campaign start date
--     p_end_date,                    -- Campaign end date
--     prorated_planned_imps_pk,      -- Placement's share of planned impressions (prorated)
--     prorated_planned_cost_pk,      -- Placement's share of planned cost (prorated)
    

--     -- ─────────────────── AGGREGATED DELIVERY METRICS ───────────────────
--     SUM(impressions) AS impressions,            -- Total impressions delivered for this placement on this date
--     SUM(clicks) AS clicks,                      -- Total clicks delivered for this placement on this date
--     SUM(total_conversions) AS total_conversions,-- Total conversions delivered for this placement on this date

--     -- [pkg_total_delivered_imps] Package-level delivered impressions (window over aggregate)
--       -- Double aggregation: SUM(impressions) is daily, then SUM(SUM(impressions)) OVER (PARTITION BY package_id)
--       -- computes the total delivered impressions for the entire package (across all placements and dates).
--     SUM(SUM(impressions)) OVER (PARTITION BY package_id) AS pkg_total_delivered_imps,

--     -- [flight_date_flag] 1 = out-of-flight, 0 = in-flight
--       -- Flag to indicate if the current row's date is outside the campaign's flight window.
--     CASE
--       WHEN date < p_start_date OR date > p_end_date THEN 1  -- Out-of-flight: before start or after end
--       ELSE 0                                               -- In-flight: within campaign window
--     END AS flight_date_flag,

--     -- [daily_recalculated_cost] ───────── DAILY COST CALCULATION (per pricing model) [ !!! FLAT RATE IS WIP !!! ] ─────────
--       -- Calculates the daily cost for this placement/date, based on the pricing model and business rules.
--     CASE
--       -- Out-of-flight rows incur no cost
--       WHEN date < p_start_date OR date > p_end_date THEN 0

--       -- CPM: cap cost if package over-delivers
--       -- If the package delivers more impressions than planned, cap total spend at planned cost.
--       -- Otherwise, use the standard CPM calculation.
--       WHEN p_cost_method = 'CPM' THEN
--         CASE
--           -- If total delivered impressions for the package exceed planned, cap total spend at planned cost.
--           WHEN SUM(SUM(impressions)) OVER (PARTITION BY package_id) > SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC)
--             /* Adjusted CPM = planned_cost / delivered_imps. 
--                Each placement's cost is proportional to its share of delivered impressions. */
--             THEN SAFE_DIVIDE(
--                    SAFE_CAST(p_pkg_total_planned_cost AS NUMERIC),   -- Planned package cost (cast to NUMERIC for safety)
--                    SUM(SUM(impressions)) OVER (PARTITION BY package_id) -- Total delivered impressions for package
--                  ) * SUM(impressions)                                -- Multiply by this placement's delivered imps
--           /* Standard CPM = rate * imps / 1000 */
--           ELSE SAFE_CAST(rate_raw AS NUMERIC) * SUM(impressions) / 1000
--         END

--       -- CPC: rate * clicks
--       WHEN p_cost_method = 'CPC'
--         THEN SAFE_CAST(rate_raw AS NUMERIC) * SUM(clicks)   -- Cost = rate per click * total clicks

--       -- CPA: rate * conversions
--       WHEN p_cost_method = 'CPA'
--         THEN SAFE_CAST(rate_raw AS NUMERIC) * SUM(total_conversions) -- Cost = rate per conversion * total conversions

--       -- Flat-rate logic [ !!! FLAT RATE IS WIP !!! ]
--       -- Two scenarios:
--       --   1. If there are no planned impressions, distribute the planned cost evenly across all days in the campaign.
--       --   2. If planned impressions exist, allocate cost to each placement based on its share of delivered impressions.
--       WHEN p_cost_method = 'Flat' THEN
--         CASE
--           -- Even daily distribution when no planned impressions
--           WHEN SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0
--             -- If no planned impressions, distribute planned cost evenly across all days in the flight window.
--             THEN SAFE_DIVIDE(
--                    SAFE_CAST(p_pkg_total_planned_cost AS NUMERIC),  -- Planned package cost
--                    SAFE_CAST(p_total_days AS NUMERIC)               -- Number of days in campaign
--                  )
--           -- Otherwise prorated by share of impressions
--           ELSE SAFE_CAST(prorated_planned_cost_pk AS NUMERIC) *     -- Placement's share of planned cost
--                safe_divide(
--                 sum(impressions),                                   -- Delivered impressions for this placement/date
--                 sum(sum(impressions)) over ( partition by package_id) -- Total delivered impressions for package
--                ) 
               
--               --  (SUM(impressions) / SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC))
--         END

--       -- Fallback for unknown cost models
--       ELSE 0   -- If pricing model is unrecognized, assign zero cost
--     END AS daily_recalculated_cost,

--     -- [daily_recalculated_cost_flag] ───────── COST LOGIC FLAG (for QA) ─────────
--       -- Emits a flag indicating which cost calculation path was used for this row, for debugging and QA.
--       -- The flag helps trace which branch of the cost logic was applied.
--     CASE
--       WHEN date < p_start_date OR date > p_end_date -- No cost: date is outside the campaign window
--         THEN 'OUT_OF_FLIGHT'   
--       WHEN p_cost_method = 'CPM' -- CPM: Over-delivery, cost capped at planned package cost
--         AND SUM(SUM(impressions)) OVER (PARTITION BY package_id) > SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC)
--         THEN 'CPM_OVERDELIVERY' 
--       WHEN p_cost_method = 'CPM' -- CPM: Standard calculation (no over-delivery)
--         THEN 'CPM_STANDARD'     
--       WHEN p_cost_method = 'CPC'
--         THEN 'CPC'              -- CPC: Cost per click calculation
--       WHEN p_cost_method = 'CPA'
--         THEN 'CPA'              -- CPA: Cost per acquisition calculation
--       WHEN p_cost_method = 'Flat' -- Flat: Even daily distribution (no planned impressions)
--         AND SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0
--         THEN 'FLAT_DAILY'       
--       WHEN p_cost_method = 'Flat' -- Flat: Prorated by delivered impressions
--         THEN 'FLAT_RATIO'       
--       ELSE 'UNKNOWN'            -- Fallback: Unexpected or unhandled case
--     END AS daily_recalculated_cost_flag

--   FROM `looker-studio-pro-452620.DCM.dcm_linkedView2`  -- Source table: contains raw delivery and plan data

--   -- Every non-aggregated field must appear here
--   -- GROUP BY is required for all non-aggregated fields in SELECT when using aggregation functions.
--   GROUP BY
    
--     package_id,
--     placement_id,
--     date,
--     p_cost_method,
--     rate_raw,
--     p_pkg_total_planned_cost,
--     p_pkg_total_planned_imps,
--     p_total_days,
--     p_start_date,
--     p_end_date,
--     prorated_planned_imps_pk,
--     prorated_planned_cost_pk
-- )

-- -- ─────────────────── FINAL SELECT (join back to raw rows) ───────────────────
-- -- Join the calculated cost_model CTE back to the raw data to append cost and QA fields to each row.
-- SELECT
--   a.* except (flight_status_flag_dcm),                                    -- All original fields from the raw data
--   c.pkg_total_delivered_imps,             -- Total delivered impressions for the package (from CTE)
--   c.daily_recalculated_cost,              -- Calculated daily cost (from CTE)
--   c.daily_recalculated_cost_flag,         -- QA/debug flag (from CTE)
--   c.flight_date_flag as flight_date_flag2  -- Duplicate/renamed flight flag for clarity

-- FROM `looker-studio-pro-452620.DCM.dcm_linkedView2` a
-- LEFT JOIN cost_model c
--   ON a.package_id    = c.package_id       -- Join on package_id, placement_id, and date to align cost calculations
--   AND a.placement_id = c.placement_id
--   AND a.date         = c.date
-- -- WHERE a.p_cost_method = 'Flat'
-- -- Filter: Only include rows for the specified package (business logic filter)