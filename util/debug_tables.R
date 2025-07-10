# Debug script to check available tables
library(bigrquery)
library(dplyr)
library(glue)

bq_project  <- "looker-studio-pro-452620"
bq_dataset  <- "20250327_data_model"

# Check what tables exist in the dataset
check_tables_query <- glue("
  SELECT table_name
  FROM `{bq_project}.{bq_dataset}.INFORMATION_SCHEMA.TABLES`
  ORDER BY table_name
")

print("Available tables in dataset:")
available_tables <- bq_project_query(bq_project, check_tables_query) %>% bq_table_download()
print(available_tables$table_name)

# Check specifically for tables with 'basis' and 'utm' in the name
print("\nTables containing 'basis' and 'utm':")
basis_utm_tables <- available_tables$table_name[grepl("basis.*utm|utm.*basis", available_tables$table_name, ignore.case = TRUE)]
print(basis_utm_tables)
