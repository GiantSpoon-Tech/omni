

# * SECTION [1]: OUTPUT TABLE REFRESH
  # Description: Rebuild native output table from staging, keep lowercase names, and remove zero-activity rows.
  CREATE OR REPLACE TABLE `looker-studio-pro-452620.mass_mutual_mft_ext.mft_offline` AS
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
  FROM `looker-studio-pro-452620.repo_stg.stg__mm__mft_offline_connected_gsheet`
  WHERE COALESCE(spend, 0) + COALESCE(impressions, 0) > 0;
