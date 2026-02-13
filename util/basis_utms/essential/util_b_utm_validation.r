library(readxl)    # For reading Excel files
library(dplyr)     # For data manipulation
library(tidyr)     # For data reshaping (pivot_longer)
library(janitor)   # For cleaning column names
library(stringr)   # For string manipulation
library(DBI)       # For database connections
library(bigrquery) # For BigQuery operations

# PULL repo_stg.basis_delivery DATA FROM BIGQUERY TABLE
# =============================================================================



# CONFIGURATION VARIABLES =====================================================
cat("\n", rep("=", 80), "\n")
cat("PULLING DATA FROM BIGQUERY TABLE")
cat("\n", rep("=", 80), "\n")

# Connect to BigQuery
tryCatch({
  bq_connection <- dbConnect(
    bigrquery::bigquery(),
    project = "looker-studio-pro-452620",
    dataset = "repo_stg",
    billing = "looker-studio-pro-452620"
  )
  
  cat("Connected to BigQuery successfully\n")
  
  # Query to get creative names data
  query <- "
  SELECT DISTINCT
    creative_name,
    cleaned_creative_name,
    del_key,
    placement,
    sum(impressions) AS total_impressions,
  FROM `looker-studio-pro-452620.repo_stg.basis_delivery`
  WHERE creative_name IS NOT NULL 
    AND creative_name != ''
    AND date >= '2025-01-01'  -- Adjust date filter as needed
  GROUP BY creative_name, cleaned_creative_name, del_key, placement
  ORDER BY creative_name
  "
  
  cat("Executing query to fetch creative names data...\n")
  
  # Execute query
  bq_b_del_data <- dbGetQuery(bq_connection, query)
  
  cat("Query completed successfully\n")
  cat("Records retrieved:", nrow(bq_b_del_data), "\n\n")

  # Display BigQuery data
  cat("BIGQUERY DATA - CREATIVE NAMES:\n")
  cat(rep("-", 120), "\n")
  cat(sprintf("%-40s | %-40s | %-30s\n", "creative_name", "CLEANED_CREATIVE_NAME", "DEL_KEY"))
  cat(rep("-", 120), "\n")

  for (i in 1:min(nrow(bq_b_del_data), 50)) {  # Show first 50 records
    cat(sprintf("%-40s | %-40s | %-30s\n", 
                substr(bq_b_del_data$creative_name[i], 1, 40),
                substr(bq_b_del_data$cleaned_creative_name[i], 1, 40),
                substr(bq_b_del_data$del_key[i], 1, 30)))
  }

  if (nrow(bq_b_del_data) > 50) {
    cat("... (showing first 50 of", nrow(bq_b_del_data), "records)\n")
  }

# PULL repo_stg.utm DATA FROM BIGQUERY TABLE
# =============================================================================
