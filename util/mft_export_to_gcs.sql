EXPORT DATA OPTIONS(
  uri = 'gs://mft_from_bq/delivery_mft.csv',
  format = 'CSV',
  overwrite = true,
  header = true,
  field_delimiter = ','
) AS
SELECT * FROM `looker-studio-pro-452620.repo_mart.mft_clean_view`;
