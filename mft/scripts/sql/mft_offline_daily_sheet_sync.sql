
# * SECTION [1]: OFFLINE SHEET SNAPSHOT SELECT
  # Description: Select filtered offline fields from connected sheet staging using actual mapped column names.
  SELECT
    date,
    channel,
    business_unit,
    campaign,
    partner,
    placement,
    spend,
    impressions,
    cpm,
    data_type,
    month,
    quarter,
    year,
    key_simp,
    total_act_cost_key,
    total_est_cost_key,
    full_key,
    year_quarter
  FROM `__FULL_STAGING_TABLE__`
  WHERE COALESCE(spend, 0) + COALESCE(impressions, 0) > 0;
