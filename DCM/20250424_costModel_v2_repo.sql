/*************************************************************
 * COST_MODEL CTE
 * Calculates a daily, placement-level cost that…
 *   • Zeros cost outside the flight window
 *   • Caps CPM spend when the entire package over-delivers
 *   • Handles CPC, CPA, and Flat pricing rules
 *   • Emits a debug flag for QA
 *************************************************************/

WITH cost_model AS (

  SELECT
    -- ─────────────────── GROUPED FIELDS (dimensions) ───────────────────
    package_id,
    placement_id,
    date,
    p_cost_method,
    rate_raw,
    p_pkg_total_planned_cost,
    p_pkg_total_planned_imps,
    p_total_days,
    p_start_date,
    p_end_date,
    prorated_planned_imps_pk,
    prorated_planned_cost_pk,
    

    -- ─────────────────── AGGREGATED DELIVERY METRICS ───────────────────
    SUM(impressions) AS impressions,            -- Daily impressions
    SUM(clicks) AS clicks,                      -- Daily clicks
    SUM(total_conversions) AS total_conversions,-- Daily conversions

    -- Package-level delivered impressions (window over aggregate)
    SUM(SUM(impressions)) OVER (PARTITION BY package_id) AS pkg_total_delivered_imps,

    -- 1 = out-of-flight, 0 = in-flight
    CASE
      WHEN date < p_start_date OR date > p_end_date THEN 1
      ELSE 0
    END AS flight_date_flag,

    -- ───────── DAILY COST CALCULATION (per pricing model) [ !!! FLAT RATE IS BROKEN !!! ] ─────────
    CASE
      -- Out-of-flight rows incur no cost
      WHEN date < p_start_date OR date > p_end_date THEN 0

      -- CPM: cap cost if package over-delivers
      WHEN p_cost_method = 'CPM' THEN
        CASE
          WHEN SUM(SUM(impressions)) OVER (PARTITION BY package_id) > SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC)
            /* Adjusted CPM = planned_cost / delivered_imps  */
            THEN SAFE_DIVIDE(
              SAFE_CAST(p_pkg_total_planned_cost AS NUMERIC),               -- Convert planned cost to a numeric value (just in case it's stored as a string)
              SUM(SUM(impressions)) OVER (PARTITION BY package_id)          -- Total impressions delivered for the entire package (across all dates), using nested SUM to aggregate then window it
            ) * SUM(impressions)                                            -- Multiply the per-impression cost by number of impressions delivered on this row's date
          /* Standard CPM = rate * imps / 1000 */
          ELSE SAFE_CAST(rate_raw AS NUMERIC) * SUM(impressions) / 1000
        END

      -- CPC: rate * clicks
      WHEN p_cost_method = 'CPC'
        THEN SAFE_CAST(rate_raw AS NUMERIC) * SUM(clicks)

      -- CPA: rate * conversions
      WHEN p_cost_method = 'CPA'
        THEN SAFE_CAST(rate_raw AS NUMERIC) * SUM(total_conversions)

      -- Flat-rate logic [ !!! FLAT RATE IS BROKEN !!! ]
      WHEN p_cost_method = 'Flat' THEN
        CASE
          -- Even daily distribution when no planned impressions
          WHEN SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0
            THEN SAFE_DIVIDE(
                   SAFE_CAST(p_pkg_total_planned_cost AS NUMERIC),
                   SAFE_CAST(p_total_days AS NUMERIC)
                 )
          -- Otherwise prorated by share of impressions
          ELSE SAFE_CAST(prorated_planned_cost_pk AS NUMERIC) *
               safe_divide(
                sum(impressions),
                sum(sum(impressions)) over ( partition by package_id)
               ) 
               
              --  (SUM(impressions) / SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC))
        END

      -- Fallback for unknown cost models
      ELSE 0
    END AS daily_recalculated_cost,

    -- ───────── COST LOGIC FLAG (for QA) ─────────
    CASE
      WHEN date < p_start_date OR date > p_end_date
        THEN 'OUT_OF_FLIGHT'
      WHEN p_cost_method = 'CPM'
        AND SUM(SUM(impressions)) OVER (PARTITION BY package_id) > SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC)
        THEN 'CPM_OVERDELIVERY'
      WHEN p_cost_method = 'CPM'
        THEN 'CPM_STANDARD'
      WHEN p_cost_method = 'CPC'
        THEN 'CPC'
      WHEN p_cost_method = 'CPA'
        THEN 'CPA'
      WHEN p_cost_method = 'Flat'
        AND SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0
        THEN 'FLAT_DAILY'
      WHEN p_cost_method = 'Flat'
        THEN 'FLAT_RATIO'
      ELSE 'UNKNOWN'
    END AS daily_recalculated_cost_flag

  FROM `looker-studio-pro-452620.DCM.dcm_linkedView2`

  -- Every non-aggregated field must appear here
  GROUP BY
    
    package_id,
    placement_id,
    date,
    p_cost_method,
    rate_raw,
    p_pkg_total_planned_cost,
    p_pkg_total_planned_imps,
    p_total_days,
    p_start_date,
    p_end_date,
    prorated_planned_imps_pk,
    prorated_planned_cost_pk
)

-- ─────────────────── FINAL SELECT (join back to raw rows) ───────────────────
SELECT
  a.*,
  c.daily_recalculated_cost,
  c.daily_recalculated_cost_flag, 
  c.flight_date_flag as flight_date_flag2   -- keep original plus renamed copy
FROM `looker-studio-pro-452620.DCM.dcm_linkedView2` a
LEFT JOIN cost_model c
  ON a.package_id    = c.package_id
  AND a.placement_id = c.placement_id
  AND a.date         = c.date;