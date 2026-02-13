################################################################################
#### VALIDATION: Compare Existing Staging View vs Updated FPD Impact
################################################################################
# Purpose: Analyze the impact of adding updated FPD data by comparing:
# 1. Packages common to both sources (overlap analysis)
# 2. Packages only in existing staging view
# 3. Packages only in new updated FPD data
################################################################################

library(bigrquery)
library(dplyr)
library(tidyr)
library(ggplot2)

cat("\n========================================\n")
cat("UPDATED FPD IMPACT VALIDATION\n")
cat("========================================\n\n")
cat("Started at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Authenticate
bq_auth(email = "gene.tsenter@giantspoon.com")

# Configuration
bq_project <- "looker-studio-pro-452620"
output_dir <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/data"

################################################################################
#### STEP 1: QUERY EXISTING STAGING VIEW ####
################################################################################

cat("--- STEP 1: Query Existing Staging View ---\n")
cat("Retrieving data from: repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test\n")

existing_query <- "
SELECT
  package_name,
  package_id,
  supplier_name,
  date,
  -- FPD metrics from existing source
  fpd_impressions AS existing_fpd_impressions,
  fpd_spend AS existing_fpd_spend,
  fpd_clicks AS existing_fpd_clicks,
  -- DCM metrics
  d_daily_recalculated_imps AS dcm_impressions,
  d_daily_recalculated_cost AS dcm_spend,
  d_clicks AS dcm_clicks,
  -- Final metrics (coalesced)
  final_impressions AS existing_final_impressions,
  final_spend AS existing_final_spend,
  final_clicks AS existing_final_clicks,
  -- Planned metrics
  planned_daily_impressions_pk AS planned_impressions,
  planned_daily_spend_pk AS planned_spend
FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
WHERE package_id_joined IS NOT NULL
"

cat("Running query... (this may take a minute)\n")
existing_data <- bq_table_download(
  bq_project_query(bq_project, existing_query)
)

cat("✓ Retrieved", nrow(existing_data), "rows from existing staging view\n")
cat("  Unique packages:", n_distinct(existing_data$package_id), "\n")
cat("  Date range:", min(existing_data$date, na.rm = TRUE), "to", max(existing_data$date, na.rm = TRUE), "\n\n")

################################################################################
#### STEP 2: QUERY NEW UPDATED FPD DATA ####
################################################################################

cat("--- STEP 2: Query New Updated FPD Data ---\n")
cat("Retrieving data from: landing.adif_updated_fpd_daily\n")

updated_fpd_query <- "
SELECT
  package_name,
  supplier_name,
  
  package_id,
  date,
  daily_fpd_impressions AS updated_fpd_impressions,
  daily_fpd_spend AS updated_fpd_spend,
  supplier_name,
  initiative,
  days_in_package,
  total_package_impressions,
  total_package_spend
FROM `looker-studio-pro-452620.landing.adif_updated_fpd_daily`
WHERE package_id IS NOT NULL
"

cat("Running query...\n")
updated_fpd_data <- bq_table_download(
  bq_project_query(bq_project, updated_fpd_query)
)

cat("✓ Retrieved", nrow(updated_fpd_data), "rows from updated FPD\n")
cat("  Unique packages:", n_distinct(updated_fpd_data$package_id), "\n")
cat("  Date range:", min(updated_fpd_data$date, na.rm = TRUE), "to", max(updated_fpd_data$date, na.rm = TRUE), "\n\n")

################################################################################
#### STEP 3: PACKAGE-LEVEL AGGREGATION ####
################################################################################

cat("--- STEP 3: Aggregate to Package Level ---\n")

# Aggregate existing data by package
existing_pkg <- existing_data %>%
  group_by(package_id) %>%
  summarise(
    package_name = first(package_name),
    supplier_name = first(supplier_name),
    existing_days = n(),
    existing_fpd_impressions = sum(existing_fpd_impressions, na.rm = TRUE),
    existing_fpd_spend = sum(existing_fpd_spend, na.rm = TRUE),
    existing_fpd_clicks = sum(existing_fpd_clicks, na.rm = TRUE),
    existing_dcm_impressions = sum(dcm_impressions, na.rm = TRUE),
    existing_dcm_spend = sum(dcm_spend, na.rm = TRUE),
    existing_final_impressions = sum(existing_final_impressions, na.rm = TRUE),
    existing_final_spend = sum(existing_final_spend, na.rm = TRUE),
    existing_min_date = min(date, na.rm = TRUE),
    existing_max_date = max(date, na.rm = TRUE),
    .groups = "drop"
  )

# Aggregate updated FPD by package
updated_pkg <- updated_fpd_data %>%
  group_by(package_id) %>%
  summarise(
    package_name = first(package_name),
    supplier_name = first(supplier_name),
    updated_days = n(),
    updated_fpd_impressions = sum(updated_fpd_impressions, na.rm = TRUE),
    updated_fpd_spend = sum(updated_fpd_spend, na.rm = TRUE),
    initiative = first(initiative),
    updated_min_date = min(date, na.rm = TRUE),
    updated_max_date = max(date, na.rm = TRUE),
    .groups = "drop"
  )

cat("✓ Aggregated existing data:", nrow(existing_pkg), "packages\n")
cat("✓ Aggregated updated FPD:", nrow(updated_pkg), "packages\n\n")

################################################################################
#### STEP 4: IDENTIFY PACKAGE OVERLAP ####
################################################################################

cat("--- STEP 4: Identify Package Overlap ---\n")

# Get package sets
existing_package_ids <- unique(existing_pkg$package_id)
updated_package_ids <- unique(updated_pkg$package_id)

# Common packages (in both)
common_packages <- intersect(existing_package_ids, updated_package_ids)

# Packages only in existing
only_existing <- setdiff(existing_package_ids, updated_package_ids)

# Packages only in updated FPD
only_updated <- setdiff(updated_package_ids, existing_package_ids)

cat("Package overlap analysis:\n")
cat("  Common packages (in both):", length(common_packages), "\n")
cat("  Only in existing staging view:", length(only_existing), "\n")
cat("  Only in updated FPD:", length(only_updated), "\n\n")

# Create overlap indicator
existing_pkg <- existing_pkg %>%
  mutate(
    overlap_category = case_when(
      package_id %in% common_packages ~ "Common (in both)",
      package_id %in% only_existing ~ "Only in existing",
      TRUE ~ "Other"
    )
  )

updated_pkg <- updated_pkg %>%
  mutate(
    overlap_category = case_when(
      package_id %in% common_packages ~ "Common (in both)",
      package_id %in% only_updated ~ "Only in updated FPD",
      TRUE ~ "Other"
    )
  )

################################################################################
#### STEP 5: COMPARISON ANALYSIS - COMMON PACKAGES ####
################################################################################

cat("--- STEP 5: Comparison Analysis - Common Packages ---\n")

if (length(common_packages) > 0) {
  # Join common packages for side-by-side comparison
  common_comparison <- existing_pkg %>%
    filter(package_id %in% common_packages) %>%
    inner_join(
      updated_pkg %>% filter(package_id %in% common_packages),
      by = "package_id",
      suffix = c("_existing", "_updated")
    ) %>%
    mutate(
      # Calculate differences
      fpd_impressions_diff = updated_fpd_impressions - existing_fpd_impressions,
      fpd_spend_diff = updated_fpd_spend - existing_fpd_spend,
      # Calculate percent change
      fpd_impressions_pct_change = ifelse(
        existing_fpd_impressions > 0,
        100 * (updated_fpd_impressions - existing_fpd_impressions) / existing_fpd_impressions,
        NA_real_
      ),
      fpd_spend_pct_change = ifelse(
        existing_fpd_spend > 0,
        100 * (updated_fpd_spend - existing_fpd_spend) / existing_fpd_spend,
        NA_real_
      )
    )

  cat("✓ Common packages detailed comparison:\n")
  cat("  Total packages:", nrow(common_comparison), "\n")
  cat("\n  Existing FPD totals (common packages):\n")
  cat("    Impressions:", format(sum(common_comparison$existing_fpd_impressions, na.rm = TRUE), big.mark = ","), "\n")
  cat("    Spend: $", format(sum(common_comparison$existing_fpd_spend, na.rm = TRUE), big.mark = ","), "\n")
  cat("\n  Updated FPD totals (common packages):\n")
  cat("    Impressions:", format(sum(common_comparison$updated_fpd_impressions, na.rm = TRUE), big.mark = ","), "\n")
  cat("    Spend: $", format(sum(common_comparison$updated_fpd_spend, na.rm = TRUE), big.mark = ","), "\n")
  cat("\n  Net change (common packages):\n")
  cat("    Impressions:", format(sum(common_comparison$fpd_impressions_diff, na.rm = TRUE), big.mark = ","), "\n")
  cat("    Spend: $", format(sum(common_comparison$fpd_spend_diff, na.rm = TRUE), big.mark = ","), "\n\n")

  # Save common packages comparison
  common_file <- file.path(output_dir, "validation_common_packages.csv")
  write.csv(common_comparison, common_file, row.names = FALSE)
  cat("✓ Common packages comparison saved:", common_file, "\n\n")
} else {
  cat("⚠ No common packages found between sources\n\n")
  common_comparison <- data.frame()
}

################################################################################
#### STEP 6: ANALYSIS - PACKAGES ONLY IN EXISTING ####
################################################################################

cat("--- STEP 6: Analysis - Packages Only in Existing ---\n")

only_existing_data <- existing_pkg %>%
  filter(package_id %in% only_existing)

cat("Packages only in existing staging view:\n")
cat("  Count:", nrow(only_existing_data), "\n")
cat("  Total existing FPD impressions:", format(sum(only_existing_data$existing_fpd_impressions, na.rm = TRUE), big.mark = ","), "\n")
cat("  Total existing FPD spend: $", format(sum(only_existing_data$existing_fpd_spend, na.rm = TRUE), big.mark = ","), "\n")
cat("  Total DCM impressions:", format(sum(only_existing_data$existing_dcm_impressions, na.rm = TRUE), big.mark = ","), "\n")
cat("  Total DCM spend: $", format(sum(only_existing_data$existing_dcm_spend, na.rm = TRUE), big.mark = ","), "\n\n")

# Top 10 packages by spend
cat("  Top 10 packages (by existing final spend):\n")
top_existing <- only_existing_data %>%
  arrange(desc(existing_final_spend)) %>%
  head(10) %>%
  select(package_id, package_name, supplier_name, existing_final_impressions, existing_final_spend)
print(top_existing, n = 10)
cat("\n")

# Save
only_existing_file <- file.path(output_dir, "validation_only_existing_packages.csv")
write.csv(only_existing_data, only_existing_file, row.names = FALSE)
cat("✓ Packages only in existing saved:", only_existing_file, "\n\n")

################################################################################
#### STEP 7: ANALYSIS - PACKAGES ONLY IN UPDATED FPD ####
################################################################################

cat("--- STEP 7: Analysis - Packages Only in Updated FPD ---\n")

only_updated_data <- updated_pkg %>%
  filter(package_id %in% only_updated)

cat("Packages only in updated FPD:\n")
cat("  Count:", nrow(only_updated_data), "\n")
cat("  Total impressions:", format(sum(only_updated_data$updated_fpd_impressions, na.rm = TRUE), big.mark = ","), "\n")
cat("  Total spend: $", format(sum(only_updated_data$updated_fpd_spend, na.rm = TRUE), big.mark = ","), "\n\n")

if (nrow(only_updated_data) > 0) {
  cat("  Top 10 packages (by spend):\n")
  top_updated <- only_updated_data %>%
    arrange(desc(updated_fpd_spend)) %>%
    head(10) %>%
    select(package_id, package_name, supplier_name, initiative, updated_fpd_impressions, updated_fpd_spend)
  print(top_updated, n = 10)
  cat("\n")
}

# Save
only_updated_file <- file.path(output_dir, "validation_only_updated_packages.csv")
write.csv(only_updated_data, only_updated_file, row.names = FALSE)
cat("✓ Packages only in updated FPD saved:", only_updated_file, "\n\n")

################################################################################
#### STEP 8: DAILY-LEVEL COMPARISON FOR COMMON PACKAGES ####
################################################################################

cat("--- STEP 8: Daily-Level Comparison for Common Packages ---\n")

if (length(common_packages) > 0) {
  # Sample: compare daily data for top 5 common packages by updated spend
  top_common_pkgs <- common_comparison %>%
    arrange(desc(updated_fpd_spend)) %>%
    head(5) %>%
    pull(package_id)

  cat("Analyzing daily data for top 5 common packages by spend:\n")
  cat(" ", paste(top_common_pkgs, collapse = ", "), "\n\n")

  # Get daily data for these packages
  existing_daily_sample <- existing_data %>%
    filter(package_id %in% top_common_pkgs) %>%
    select(package_id, package_name, supplier_name, date, existing_fpd_impressions, existing_fpd_spend, existing_final_impressions, existing_final_spend)

  updated_daily_sample <- updated_fpd_data %>%
    filter(package_id %in% top_common_pkgs) %>%
    select(package_id, package_name, supplier_name, date, updated_fpd_impressions, updated_fpd_spend)

  # Join on package_id and date
  daily_comparison <- existing_daily_sample %>%
    full_join(updated_daily_sample, by = c("package_id", "package_name", "supplier_name", "date"), suffix = c("_existing", "_updated"))

  cat("✓ Daily comparison for", length(top_common_pkgs), "sample packages:\n")
  cat("  Total daily rows:", nrow(daily_comparison), "\n")
  cat("  Date range:", min(daily_comparison$date, na.rm = TRUE), "to", max(daily_comparison$date, na.rm = TRUE), "\n\n")

  # Save daily comparison
  daily_file <- file.path(output_dir, "validation_daily_comparison_sample.csv")
  write.csv(daily_comparison, daily_file, row.names = FALSE)
  cat("✓ Daily comparison sample saved:", daily_file, "\n\n")
} else {
  cat("⚠ No common packages for daily comparison\n\n")
}

################################################################################
#### STEP 9: SUMMARY REPORT ####
################################################################################

cat("========================================\n")
cat("VALIDATION SUMMARY REPORT\n")
cat("========================================\n\n")

# Create summary table
summary_table <- data.frame(
  Category = c(
    "Common packages (in both)",
    "Only in existing staging view",
    "Only in updated FPD",
    "TOTAL (existing)",
    "TOTAL (updated FPD)"
  ),
  Package_Count = c(
    length(common_packages),
    length(only_existing),
    length(only_updated),
    length(existing_package_ids),
    length(updated_package_ids)
  ),
  Impressions = c(
    ifelse(length(common_packages) > 0, sum(common_comparison$updated_fpd_impressions, na.rm = TRUE), 0),
    sum(only_existing_data$existing_final_impressions, na.rm = TRUE),
    sum(only_updated_data$updated_fpd_impressions, na.rm = TRUE),
    sum(existing_pkg$existing_final_impressions, na.rm = TRUE),
    sum(updated_pkg$updated_fpd_impressions, na.rm = TRUE)
  ),
  Spend = c(
    ifelse(length(common_packages) > 0, sum(common_comparison$updated_fpd_spend, na.rm = TRUE), 0),
    sum(only_existing_data$existing_final_spend, na.rm = TRUE),
    sum(only_updated_data$updated_fpd_spend, na.rm = TRUE),
    sum(existing_pkg$existing_final_spend, na.rm = TRUE),
    sum(updated_pkg$updated_fpd_spend, na.rm = TRUE)
  )
)

cat("Overall Summary:\n")
print(summary_table, row.names = FALSE)
cat("\n")

# Impact analysis
if (length(common_packages) > 0) {
  cat("Impact on Common Packages:\n")
  cat("  Impressions change: ", format(sum(common_comparison$fpd_impressions_diff, na.rm = TRUE), big.mark = ","), "\n")
  cat("  Spend change: $", format(sum(common_comparison$fpd_spend_diff, na.rm = TRUE), big.mark = ","), "\n")

  # Count packages with increases/decreases
  imps_increase <- sum(common_comparison$fpd_impressions_diff > 0, na.rm = TRUE)
  imps_decrease <- sum(common_comparison$fpd_impressions_diff < 0, na.rm = TRUE)
  imps_same <- sum(common_comparison$fpd_impressions_diff == 0, na.rm = TRUE)

  cat("\n  Packages with impression changes:\n")
  cat("    Increased:", imps_increase, "\n")
  cat("    Decreased:", imps_decrease, "\n")
  cat("    Unchanged:", imps_same, "\n")
}

cat("\n")

# Save summary
summary_file <- file.path(output_dir, "validation_summary.csv")
write.csv(summary_table, summary_file, row.names = FALSE)
cat("✓ Summary report saved:", summary_file, "\n\n")

################################################################################
#### STEP 10: RECOMMENDATIONS ####
################################################################################

cat("========================================\n")
cat("RECOMMENDATIONS\n")
cat("========================================\n\n")

if (length(only_updated) > 0) {
  cat("⚠ ATTENTION: ", length(only_updated), " packages in updated FPD are NOT in existing staging view\n")
  cat("   These are net NEW packages that will be added when integrating updated FPD.\n")
  cat("   Review 'validation_only_updated_packages.csv' for details.\n\n")
}

if (length(common_packages) > 0 && nrow(common_comparison) > 0) {
  # Check for significant changes
  big_changes <- common_comparison %>%
    filter(
      abs(fpd_impressions_pct_change) > 50 |
      abs(fpd_spend_pct_change) > 50
    )

  if (nrow(big_changes) > 0) {
    cat("⚠ ATTENTION: ", nrow(big_changes), " common packages have >50% change in metrics\n")
    cat("   Review 'validation_common_packages.csv' to investigate these discrepancies.\n\n")
  } else {
    cat("✓ No extreme metric changes detected in common packages.\n\n")
  }
}

cat("Next steps:\n")
cat("1. Review the validation CSV files in:", output_dir, "\n")
cat("2. Investigate any unexpected packages in 'only updated FPD' category\n")
cat("3. Verify metric changes for common packages are expected\n")
cat("4. Deploy integration SQL if validation looks good\n\n")

cat("========================================\n")
cat("Validation completed at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")
