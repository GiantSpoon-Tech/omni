################################################################################
#### PROCESS UPDATED FPD DATA WITH DATE SPREADING
################################################################################
# Purpose: Read updated FPD data from Google Sheet (package-level), join with
# Prisma to get date ranges, spread metrics evenly across days, upload to BQ
################################################################################

library(googlesheets4)
library(bigrquery)
library(dplyr)
library(tidyr)
library(lubridate)


cat("\n========================================\n")
cat("UPDATED FPD PROCESSING PIPELINE\n")
cat("========================================\n\n")
cat("Started at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Authenticate
gs4_auth(email = "gene.tsenter@giantspoon.com")
bq_auth(email = "gene.tsenter@giantspoon.com")

# Configuration
sheet_id <- "1818fclbE_pxNLlCz4dW5zX6ulRRqnnedolbSYVUWnjk"
output_dir <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/data"
bq_project <- "looker-studio-pro-452620"
bq_dataset <- "landing"
bq_table <- "adif_updated_fpd_daily"

################################################################################
#### STEP 1: READ UPDATED FPD FROM GOOGLE SHEET ####
################################################################################

cat("--- STEP 1: Read Updated FPD from Google Sheet ---\n")

# Get sheet info to find the correct tab
sheet_info <- gs4_get(sheet_id)
target_gid <- "1894007924"
tab_info <- sheet_info$sheets %>%
  filter(as.character(id) == target_gid)

if (nrow(tab_info) > 0) {
  tab_name <- tab_info$name[1]
  cat("Reading from tab:", tab_name, "\n")
} else {
  cat("Could not find gid", target_gid, "- using first tab\n")
  tab_name <- sheet_info$sheets$name[1]
}

# Read the data
fpd_raw <- read_sheet(
  ss = sheet_id,
  sheet = tab_name,
  col_names = TRUE
)

cat("✓ Read", nrow(fpd_raw), "package-level records\n")

# Clean and select relevant columns
fpd_packages <- fpd_raw %>%
  select(
    package_id,
    supplier_name,
    initiative,
    package_name,
    updated_FPD_IMPRESSIONS,
    updated_FPD_SPEND
  ) %>%
  filter(!is.na(package_id)) %>%
  mutate(
    # Ensure numeric types
    updated_FPD_IMPRESSIONS = as.numeric(updated_FPD_IMPRESSIONS),
    updated_FPD_SPEND = as.numeric(updated_FPD_SPEND)
  ) %>%
  # Filter to only packages with actual FPD data
  filter(!is.na(updated_FPD_IMPRESSIONS) | !is.na(updated_FPD_SPEND))

cat("✓ After filtering: ", nrow(fpd_packages), "packages with FPD data\n")
cat("  Total impressions:", format(sum(fpd_packages$updated_FPD_IMPRESSIONS, na.rm = TRUE), big.mark = ","), "\n")
cat("  Total spend: $", format(sum(fpd_packages$updated_FPD_SPEND, na.rm = TRUE), big.mark = ","), "\n\n")

# Save checkpoint
fpd_checkpoint <- file.path(output_dir, "updated_fpd_packages.csv")
write.csv(fpd_packages, fpd_checkpoint, row.names = FALSE)
cat("✓ Checkpoint saved:", fpd_checkpoint, "\n\n")

################################################################################
#### STEP 2: GET DATE RANGES FROM PRISMA ####
################################################################################

cat("--- STEP 2: Get Date Ranges from Prisma ---\n")

# Query Prisma to get package-level date ranges
# Use the most recent report_date for each package
prisma_query <- "
WITH latest_prisma AS (
  SELECT
    package_id,
    MIN(date) as prisma_start_date,
    MAX(date) as prisma_end_date,
    COUNT(DISTINCT date) as days_in_plan,
    MAX(report_date) as latest_report_date
  FROM `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
  WHERE advertiser_name = 'Forevermark US'
    AND package_type != 'Child'
  GROUP BY package_id
)
SELECT *
FROM latest_prisma
ORDER BY package_id
"

cat("Querying Prisma for package date ranges...\n")
prisma_dates <- bq_table_download(
  bq_project_query(bq_project, prisma_query)
)

cat("✓ Retrieved date ranges for", nrow(prisma_dates), "packages from Prisma\n")
cat("  Date range: ", min(prisma_dates$prisma_start_date), "to", max(prisma_dates$prisma_end_date), "\n\n")

# Save checkpoint
prisma_checkpoint <- file.path(output_dir, "prisma_package_dates.csv")
write.csv(prisma_dates, prisma_checkpoint, row.names = FALSE)
cat("✓ Checkpoint saved:", prisma_checkpoint, "\n\n")

################################################################################
#### STEP 3: JOIN FPD WITH PRISMA DATES ####
################################################################################

cat("--- STEP 3: Join FPD with Prisma Dates ---\n")

fpd_with_dates <- fpd_packages %>%
  left_join(prisma_dates, by = "package_id") %>%
  mutate(
    # Ensure dates are Date type
    prisma_start_date = as.Date(prisma_start_date),
    prisma_end_date = as.Date(prisma_end_date)
  )

# Check for packages without date ranges
missing_dates <- fpd_with_dates %>%
  filter(is.na(prisma_start_date) | is.na(prisma_end_date))

if (nrow(missing_dates) > 0) {
  cat("⚠ Warning:", nrow(missing_dates), "packages have no Prisma date ranges:\n")
  print(missing_dates %>% select(package_id, supplier_name, initiative, updated_FPD_IMPRESSIONS, updated_FPD_SPEND))
  cat("\nThese packages will be skipped in daily spreading.\n\n")
}

# Filter to only packages with valid date ranges
fpd_ready <- fpd_with_dates %>%
  filter(!is.na(prisma_start_date), !is.na(prisma_end_date)) %>%
  filter(prisma_end_date >= prisma_start_date)

cat("✓", nrow(fpd_ready), "packages ready for daily spreading\n")
cat("  Total impressions (with dates):", format(sum(fpd_ready$updated_FPD_IMPRESSIONS, na.rm = TRUE), big.mark = ","), "\n")
cat("  Total spend (with dates): $", format(sum(fpd_ready$updated_FPD_SPEND, na.rm = TRUE), big.mark = ","), "\n\n")

# Save checkpoint
joined_checkpoint <- file.path(output_dir, "updated_fpd_with_dates.csv")
write.csv(fpd_ready, joined_checkpoint, row.names = FALSE)
cat("✓ Checkpoint saved:", joined_checkpoint, "\n\n")

################################################################################
#### STEP 4: SPREAD METRICS ACROSS DATE RANGES ####
################################################################################

cat("--- STEP 4: Spread Metrics Across Daily Rows ---\n")
cat("This may take a few minutes for large date ranges...\n\n")

# Expand each package to daily rows
daily_rows <- list()
row_idx <- 1

for (i in seq_len(nrow(fpd_ready))) {
  pkg <- fpd_ready[i, ]

  start_date <- pkg$prisma_start_date
  end_date <- pkg$prisma_end_date
  n_days <- as.integer(end_date - start_date) + 1

  if (n_days <= 0) {
    cat("  ⚠ Skipping", pkg$package_id, "- invalid date range\n")
    next
  }

  # Calculate daily values (spread evenly)
  daily_impressions <- ifelse(
    is.na(pkg$updated_FPD_IMPRESSIONS),
    NA_real_,
    pkg$updated_FPD_IMPRESSIONS / n_days
  )

  daily_spend <- ifelse(
    is.na(pkg$updated_FPD_SPEND),
    NA_real_,
    pkg$updated_FPD_SPEND / n_days
  )

  # Create daily rows
  for (day_offset in 0:(n_days - 1)) {
    date <- start_date + day_offset

    daily_rows[[row_idx]] <- data.frame(
      package_id = pkg$package_id,
      date = date,
      supplier_name = pkg$supplier_name,
      initiative = pkg$initiative,
      package_name = pkg$package_name,
      prisma_start_date = start_date,
      prisma_end_date = end_date,
      days_in_package = n_days,
      daily_fpd_impressions = daily_impressions,
      daily_fpd_spend = daily_spend,
      # Original package totals for reference
      total_package_impressions = pkg$updated_FPD_IMPRESSIONS,
      total_package_spend = pkg$updated_FPD_SPEND,
      data_source = "updated_fpd_sheet",
      data_update_datetime = Sys.time(),
      stringsAsFactors = FALSE
    )

    row_idx <- row_idx + 1
  }

  # Progress indicator
  if (i %% 10 == 0) {
    cat("  Processed", i, "of", nrow(fpd_ready), "packages...\n")
  }
}

# Combine all daily rows
fpd_daily <- bind_rows(daily_rows)

cat("\n✓ Created", nrow(fpd_daily), "daily rows from", nrow(fpd_ready), "packages\n")
cat("  Date range:", min(fpd_daily$date), "to", max(fpd_daily$date), "\n")
cat("  Total impressions (check):", format(sum(fpd_daily$daily_fpd_impressions, na.rm = TRUE), big.mark = ","), "\n")
cat("  Total spend (check): $", format(sum(fpd_daily$daily_fpd_spend, na.rm = TRUE), big.mark = ","), "\n\n")

# Validation: Check that totals match
original_imps <- sum(fpd_ready$updated_FPD_IMPRESSIONS, na.rm = TRUE)
expanded_imps <- sum(fpd_daily$daily_fpd_impressions, na.rm = TRUE)
imps_diff <- abs(expanded_imps - original_imps)

original_spend <- sum(fpd_ready$updated_FPD_SPEND, na.rm = TRUE)
expanded_spend <- sum(fpd_daily$daily_fpd_spend, na.rm = TRUE)
spend_diff <- abs(expanded_spend - original_spend)

if (imps_diff < 1 && spend_diff < 0.01) {
  cat("✓ Validation passed: Daily totals match package totals\n\n")
} else {
  cat("⚠ Warning: Validation mismatch detected:\n")
  cat("  Impressions diff:", format(imps_diff, big.mark = ","), "\n")
  cat("  Spend diff: $", format(spend_diff, big.mark = ","), "\n\n")
}

# Save checkpoint
daily_checkpoint <- file.path(output_dir, "updated_fpd_daily.csv")
write.csv(fpd_daily, daily_checkpoint, row.names = FALSE)
cat("✓ Checkpoint saved:", daily_checkpoint, "\n\n")

################################################################################
#### STEP 5: UPLOAD TO BIGQUERY ####
################################################################################

cat("--- STEP 5: Upload to BigQuery ---\n")

# Replace NA with 0 for numeric columns to avoid NULL issues
fpd_daily <- fpd_daily %>%
  mutate(
    daily_fpd_impressions = ifelse(is.na(daily_fpd_impressions), 0, daily_fpd_impressions),
    daily_fpd_spend = ifelse(is.na(daily_fpd_spend), 0, daily_fpd_spend)
  )

# Delete old table if exists
bq_table_ref <- bq_table(project = bq_project, dataset = bq_dataset, table = bq_table)
tryCatch({
  bq_table_delete(bq_table_ref)
  cat("✓ Deleted old BigQuery table\n")
}, error = function(e) {
  cat("  (No existing table to delete)\n")
})

# Upload new data
cat("Uploading", nrow(fpd_daily), "rows to BigQuery...\n")
bq_table_upload(
  bq_table_ref,
  fpd_daily,
  write_disposition = "WRITE_TRUNCATE"
)

cat("✓ Data uploaded to BigQuery:", paste0(bq_dataset, ".", bq_table), "\n\n")

################################################################################
#### STEP 6: SUMMARY REPORT ####
################################################################################

cat("========================================\n")
cat("PIPELINE SUMMARY\n")
cat("========================================\n\n")

cat("Packages processed:\n")
cat("  Input (from Google Sheet):", nrow(fpd_packages), "\n")
cat("  With Prisma dates:", nrow(fpd_ready), "\n")
cat("  Without dates (skipped):", nrow(missing_dates), "\n\n")

cat("Metrics (package-level totals):\n")
cat("  Impressions:", format(sum(fpd_ready$updated_FPD_IMPRESSIONS, na.rm = TRUE), big.mark = ","), "\n")
cat("  Spend: $", format(sum(fpd_ready$updated_FPD_SPEND, na.rm = TRUE), big.mark = ","), "\n\n")

cat("Daily output:\n")
cat("  Total daily rows:", format(nrow(fpd_daily), big.mark = ","), "\n")
cat("  Date range:", min(fpd_daily$date), "to", max(fpd_daily$date), "\n")
cat("  Unique packages:", n_distinct(fpd_daily$package_id), "\n\n")

cat("BigQuery table:", paste0(bq_project, ".", bq_dataset, ".", bq_table), "\n\n")

# Summary by package
pkg_summary <- fpd_daily %>%
  group_by(package_id, supplier_name, initiative) %>%
  summarise(
    start_date = min(date),
    end_date = max(date),
    days = n(),
    total_daily_impressions = sum(daily_fpd_impressions, na.rm = TRUE),
    total_daily_spend = sum(daily_fpd_spend, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(total_daily_spend))

cat("Top 10 packages by spend:\n")
print(head(pkg_summary, 10), n = 10)

# Save summary
summary_file <- file.path(output_dir, "updated_fpd_package_summary.csv")
write.csv(pkg_summary, summary_file, row.names = FALSE)
cat("\n✓ Package summary saved:", summary_file, "\n\n")

cat("========================================\n")
cat("Pipeline completed at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

cat("Next steps:\n")
cat("1. Verify data in BigQuery:", paste0(bq_dataset, ".", bq_table), "\n")
cat("2. Update downstream views/marts to include this new FPD source\n")
cat("3. Schedule this script for regular execution if needed\n\n")
