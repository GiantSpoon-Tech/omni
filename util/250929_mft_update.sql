-- download  copy of table to gcs
-- bq extract --destination_format=CSV --compression=GZIP 'looker-studio-pro-452620.repo_mart.mft_clean_view' gs://eugene_tsenter_temp/mft_clean_view.csv.gz

SELECT * FROM `looker-studio-pro-452620.repo_mart.mft_clean_view`