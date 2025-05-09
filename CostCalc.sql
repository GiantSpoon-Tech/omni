-- derived_table: {
--   persist_for: "24 hours"
--   sql: 
    /*******************************************************
     * BASE COSTS CALCULATION
     * Purpose: Calculate daily costs for each placement based on its pricing model
     * Includes data validation and safe type casting
     *******************************************************/
    WITH base_costs AS (
      SELECT
        -- Dimensional fields for grouping and analysis
        package_id,
        placement_id,
        p_cost_method,
        rate_raw,
        p_pkg_total_planned_cost,
        p_pkg_total_planned_imps,
        p_total_days,
        date,
        

        -- Safe type casting to prevent query failures from malformed data
        -- Returns NULL instead of failing if conversion is impossible
        SAFE_CAST(rate_raw AS NUMERIC) AS rate_numeric,
        SAFE_CAST(p_pkg_total_planned_cost AS NUMERIC) AS planned_cost_numeric,
        SAFE_CAST(p_total_days AS INT64) AS total_days_int,

        -- Daily metric aggregations
        -- These provide the base values for cost calculations
        SUM(impressions) AS impressions,          -- Total daily impressions
        SUM(clicks) AS clicks,                    -- Total daily clicks
        SUM(total_conversions) AS total_conversions,  -- Total daily conversions
        SUM(media_cost) AS cost,                  -- Actual media cost

        /*******************************************************
         * DAILY COST CALCULATION
         * Handles different pricing models with specific logic for each:
         * - CPM: Cost per thousand impressions
         * - CPC: Cost per click
         * - CPA: Cost per acquisition/conversion
         * - Flat: Either even daily distribution or impression-based
         *******************************************************/
        CASE
          -- CPM: Rate per 1000 impressions
          -- Formula: (rate * impressions) / 1000
          WHEN p_cost_method = 'CPM' 
            THEN SAFE_CAST(rate_raw AS NUMERIC) * SUM(impressions) / 1000

          -- CPC: Rate per click
          -- Formula: rate * number of clicks
          WHEN p_cost_method = 'CPC' 
            THEN SAFE_CAST(rate_raw AS NUMERIC) * SUM(clicks)

          -- CPA: Rate per conversion/acquisition
          -- Formula: rate * number of conversions
          WHEN p_cost_method = 'CPA' 
            THEN SAFE_CAST(rate_raw AS NUMERIC) * SUM(total_conversions)

          -- Flat Rate: Two possible distribution methods
          WHEN p_cost_method = 'Flat' THEN
            CASE
              -- If no planned impressions, distribute cost evenly across days
              -- Formula: planned_cost / total_days
              WHEN SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0 
                THEN SAFE_CAST(p_pkg_total_planned_cost AS NUMERIC) / 
                     SAFE_CAST(p_total_days AS NUMERIC)

              -- With planned impressions, distribute cost based on delivery ratio
              -- Formula: planned_cost * (actual_impressions / planned_impressions)
              ELSE SAFE_CAST(p_pkg_total_planned_cost AS NUMERIC) * 
                   (SUM(impressions) / SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC))
            END
          -- Default to 0 for unknown cost methods
          ELSE 0
        END AS daily_recalculated_cost
      FROM `looker-studio-pro-452620.DCM.dcm_linkedView2`
      GROUP BY
        package_id,
        placement_id,
        p_cost_method,
        rate_raw,
        p_pkg_total_planned_cost,
        p_pkg_total_planned_imps,
        p_total_days,
        date
    )

    /*******************************************************
     * FINAL COST CALCULATION AND BUDGET ENFORCEMENT
     * Purpose: Ensure costs don't exceed planned package budget
     * Uses window functions to track cumulative spend
     *******************************************************/
    SELECT *,
      -- Calculate running total of costs for each package
      -- Window: All previous days up to and including current day
      SUM(daily_recalculated_cost) OVER (
        PARTITION BY package_id 
        ORDER BY date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS cumulative_recalculated_cost,

      -- Determine actual daily cost while respecting budget limits
      CASE
        -- Scenario 1: Full daily cost if cumulative is still under budget
        WHEN SUM(daily_recalculated_cost) OVER (
          PARTITION BY package_id 
          ORDER BY date 
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) <= planned_cost_numeric THEN daily_recalculated_cost

        -- Scenario 2: Partial cost if some budget remains from previous days
        WHEN SUM(daily_recalculated_cost) OVER (
          PARTITION BY package_id 
          ORDER BY date 
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) < planned_cost_numeric 
        -- Use only the remaining budget
        THEN planned_cost_numeric - SUM(daily_recalculated_cost) OVER (
          PARTITION BY package_id 
          ORDER BY date 
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )

        -- Scenario 3: No cost if budget is already exceeded
        ELSE 0
      END AS placement_actualized_cost_by_day
    FROM base_costs
-- ;;}