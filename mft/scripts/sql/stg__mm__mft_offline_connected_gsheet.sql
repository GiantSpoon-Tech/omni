

# * SECTION [1]: STAGING EXTERNAL TABLE
  # Description: Recreate connected-sheet staging table in repo_stg using actual mapped column names through column U.
  CREATE OR REPLACE EXTERNAL TABLE `looker-studio-pro-452620.repo_stg.stg__mm__mft_offline_connected_gsheet` (
    date DATE,
    channel STRING,
    business_unit STRING,
    campaign STRING,
    partner STRING,
    placement STRING,
    spend INT64,
    impressions INT64,
    cpm STRING,
    data_type STRING,
    month STRING,
    quarter STRING,
    year STRING,
    key_simp STRING,
    _blank_o STRING,
    _blank_p STRING,
    _blank_q STRING,
    total_act_cost_key STRING,
    total_est_cost_key STRING,
    full_key STRING,
    year_quarter STRING
  )
  OPTIONS (
    format = 'GOOGLE_SHEETS',
    uris = ['https://docs.google.com/spreadsheets/d/15DddW291w_O7WWv8F0AcOcumEMYJSdm9hWKU9vE5WPQ/edit?gid=1999096908#gid=1999096908'],
    skip_leading_rows = 1,
    sheet_range = "'[NEW] INTERNAL | COMBINED DATA'!A:U"
  );
