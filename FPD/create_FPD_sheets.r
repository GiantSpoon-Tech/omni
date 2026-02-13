# create a df for partners with untracked media
 

# run sql
library(bigrquery)
library(dplyr)
library(stringr)
library(lubridate)
library(purrr)
library(tidyr)
library(readr)
library(janitor)
library(googlesheets4)
library(googledrive)

sql <- "
SELECT DISTINCT
  advertiser_name,
  advertiser_short_name,
  campaign_name,
  supplier_code,
  supplier_name,
  script_run_date,
  tracking_status,
  start_date,
  end_date,
FROM
  `looker-studio-pro-452620.Prisma.prisma__stg__digital_plus_linear_view`
WHERE
  EXTRACT(YEAR FROM end_date) = EXTRACT(YEAR FROM CURRENT_DATE())
  AND tracking_status != 'Tracking Delivery'
  AND tracking_status != 'Pre-flight'
"

# Run the query, download the rows into a tibble, and keep column names clean.
billing_project <- "looker-studio-pro-452620"
job <- bq_project_query(billing_project, sql)

fpd_untracked_df_raw <- bq_table_download(job) %>%
  clean_names()

glimpse(fpd_untracked_df_raw)

fpd_untracked_df <- fpd_untracked_df_raw %>%
  select(advertiser_name, advertiser_short_name, supplier_code, supplier_name, start_date, end_date,script_run_date,campaign_name,) %>%
  group_by(advertiser_name,advertiser_short_name,supplier_code,supplier_name) %>%
  summarise(start_date = min(start_date),end_date = max(end_date),script_run_date = max(script_run_date)) %>%
  mutate(sheet_name = paste0(advertiser_short_name, " | ", supplier_code, " | Partner Data")) %>%
  
  filter(end_date >= today())

unique(fpd_untracked_df$sheet_name) %>% sort()

#write to google sheets
sheet_url <- "https://docs.google.com/spreadsheets/d/15zQ_IZx0kFpAffCFpp2d8ddDfjRf-kjnoSQXHxSu5eA/edit?gid=1365364759#gid=1365364759"
sheet_name <- "Untracked_Media"   

write_sheet(fpd_untracked_df, sheet_url, sheet = sheet_name)


