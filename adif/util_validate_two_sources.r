################################################################################
#### GENERALIZED TWO-SOURCE VALIDATION FRAMEWORK
################################################################################
# Purpose: Compare any two BigQuery tables/views to identify:
#   - Common records (overlap)
#   - Records only in Source A
#   - Records only in Source B
#   - Metric differences for common records
#
# Configuration: Uses YAML config file to define data sources and comparison logic
#
# Usage:
#   Rscript util_validate_two_sources.r config_validation_fpd.yaml
#   Rscript util_validate_two_sources.r config_validation_custom.yaml
################################################################################

library(bigrquery)
library(dplyr)
library(tidyr)
library(yaml)

cat("\n========================================\n")
cat("GENERALIZED TWO-SOURCE VALIDATION\n")
cat("========================================\n\n")
cat("Started at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

################################################################################
#### STEP 1: LOAD CONFIGURATION ####
################################################################################

cat("--- STEP 1: Load Configuration ---\n")

# Get config file from command line argument
args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  stop("Usage: Rscript util_validate_two_sources.r <config_file.yaml>")
}

config_file <- args[1]
if (!file.exists(config_file)) {
  stop("Config file not found: ", config_file)
}

cat("Loading configuration from:", config_file, "\n")
config <- yaml::read_yaml(config_file)

# Validate required config sections
required_sections <- c("project", "output_dir", "source_a", "source_b", "comparison")
missing_sections <- setdiff(required_sections, names(config))
if (length(missing_sections) > 0) {
  stop("Missing required config sections: ", paste(missing_sections, collapse = ", "))
}

cat("✓ Configuration loaded successfully\n")
cat("  Project:", config$project, "\n")
cat("  Source A:", config$source_a$label, "\n")
cat("  Source B:", config$source_b$label, "\n")
cat("  Join keys:", paste(config$comparison$join_keys, collapse = ", "), "\n\n")

# Authenticate
bq_auth(email = config$auth_email)

################################################################################
#### STEP 2: QUERY SOURCE A ####
################################################################################

cat("--- STEP 2: Query Source A (", config$source_a$label, ") ---\n", sep = "")
cat("Table/Query:", config$source_a$table, "\n")

# Build query for Source A
if (!is.null(config$source_a$query) && config$source_a$query != "") {
  # Use custom query if provided
  query_a <- config$source_a$query
} else {
  # Build query from column mappings
  select_cols_a <- c()

  # Add join key columns
  for (i in seq_along(config$comparison$join_keys)) {
    key <- config$comparison$join_keys[[i]]
    col_a <- config$source_a$join_key_columns[[i]]
    select_cols_a <- c(select_cols_a, paste0(col_a, " AS ", key))
  }

  # Add metric columns
  for (metric in names(config$source_a$metrics)) {
    col_name <- config$source_a$metrics[[metric]]
    select_cols_a <- c(select_cols_a, paste0(col_name, " AS a_", metric))
  }

  # Add dimension columns if specified
  if (!is.null(config$source_a$dimensions)) {
    for (dim in names(config$source_a$dimensions)) {
      col_name <- config$source_a$dimensions[[dim]]
      select_cols_a <- c(select_cols_a, paste0(col_name, " AS a_", dim))
    }
  }

  # Build SELECT statement
  query_a <- paste0(
    "SELECT\n  ",
    paste(select_cols_a, collapse = ",\n  "),
    "\nFROM `", config$project, ".", config$source_a$table, "`"
  )

  # Add WHERE clause if specified
  if (!is.null(config$source_a$where) && config$source_a$where != "") {
    query_a <- paste0(query_a, "\nWHERE ", config$source_a$where)
  }
}

cat("Running query...\n")
data_a <- bq_table_download(
  bq_project_query(config$project, query_a)
)

cat("✓ Retrieved", nrow(data_a), "rows from Source A\n")
if (length(config$comparison$join_keys) > 0) {
  for (key in config$comparison$join_keys) {
    cat("  Unique", key, ":", n_distinct(data_a[[key]]), "\n")
  }
}
cat("\n")

################################################################################
#### STEP 3: QUERY SOURCE B ####
################################################################################

cat("--- STEP 3: Query Source B (", config$source_b$label, ") ---\n", sep = "")
cat("Table/Query:", config$source_b$table, "\n")

# Build query for Source B (same logic as Source A)
if (!is.null(config$source_b$query) && config$source_b$query != "") {
  query_b <- config$source_b$query
} else {
  select_cols_b <- c()

  # Add join key columns
  for (i in seq_along(config$comparison$join_keys)) {
    key <- config$comparison$join_keys[[i]]
    col_b <- config$source_b$join_key_columns[[i]]
    select_cols_b <- c(select_cols_b, paste0(col_b, " AS ", key))
  }

  # Add metric columns
  for (metric in names(config$source_b$metrics)) {
    col_name <- config$source_b$metrics[[metric]]
    select_cols_b <- c(select_cols_b, paste0(col_name, " AS b_", metric))
  }

  # Add dimension columns if specified
  if (!is.null(config$source_b$dimensions)) {
    for (dim in names(config$source_b$dimensions)) {
      col_name <- config$source_b$dimensions[[dim]]
      select_cols_b <- c(select_cols_b, paste0(col_name, " AS b_", dim))
    }
  }

  query_b <- paste0(
    "SELECT\n  ",
    paste(select_cols_b, collapse = ",\n  "),
    "\nFROM `", config$project, ".", config$source_b$table, "`"
  )

  if (!is.null(config$source_b$where) && config$source_b$where != "") {
    query_b <- paste0(query_b, "\nWHERE ", config$source_b$where)
  }
}

cat("Running query...\n")
data_b <- bq_table_download(
  bq_project_query(config$project, query_b)
)

cat("✓ Retrieved", nrow(data_b), "rows from Source B\n")
if (length(config$comparison$join_keys) > 0) {
  for (key in config$comparison$join_keys) {
    cat("  Unique", key, ":", n_distinct(data_b[[key]]), "\n")
  }
}
cat("\n")

################################################################################
#### STEP 4: AGGREGATE TO COMPARISON LEVEL ####
################################################################################

cat("--- STEP 4: Aggregate to Comparison Level ---\n")

# Determine aggregation level from config
agg_keys <- config$comparison$aggregation_keys
if (is.null(agg_keys)) {
  agg_keys <- config$comparison$join_keys[1]  # Default to first join key
  cat("Using default aggregation key:", agg_keys, "\n")
} else {
  cat("Aggregation keys:", paste(agg_keys, collapse = ", "), "\n")
}

# Aggregate Source A
metric_cols_a <- grep("^a_", names(data_a), value = TRUE)
agg_a <- data_a %>%
  group_by(across(all_of(agg_keys))) %>%
  summarise(
    across(all_of(metric_cols_a), ~ sum(.x, na.rm = TRUE), .names = "{.col}_sum"),
    row_count_a = n(),
    .groups = "drop"
  )

# Aggregate Source B
metric_cols_b <- grep("^b_", names(data_b), value = TRUE)
agg_b <- data_b %>%
  group_by(across(all_of(agg_keys))) %>%
  summarise(
    across(all_of(metric_cols_b), ~ sum(.x, na.rm = TRUE), .names = "{.col}_sum"),
    row_count_b = n(),
    .groups = "drop"
  )

cat("✓ Aggregated Source A:", nrow(agg_a), "groups\n")
cat("✓ Aggregated Source B:", nrow(agg_b), "groups\n\n")

################################################################################
#### STEP 5: IDENTIFY OVERLAP ####
################################################################################

cat("--- STEP 5: Identify Overlap ---\n")

# Create composite key for matching
create_composite_key <- function(df, key_cols) {
  df %>%
    unite("__composite_key__", all_of(key_cols), sep = "|||", remove = FALSE) %>%
    pull(__composite_key__)
}

keys_a <- create_composite_key(agg_a, agg_keys)
keys_b <- create_composite_key(agg_b, agg_keys)

# Find common and unique keys
common_keys <- intersect(keys_a, keys_b)
only_a_keys <- setdiff(keys_a, keys_b)
only_b_keys <- setdiff(keys_b, keys_a)

cat("Overlap analysis:\n")
cat("  Common (in both sources):", length(common_keys), "\n")
cat("  Only in Source A (", config$source_a$label, "):", length(only_a_keys), "\n", sep = "")
cat("  Only in Source B (", config$source_b$label, "):", length(only_b_keys), "\n\n", sep = "")

# Add composite key to aggregated data
agg_a$__composite_key__ <- keys_a
agg_b$__composite_key__ <- keys_b

# Add overlap category
agg_a <- agg_a %>%
  mutate(overlap_category = case_when(
    __composite_key__ %in% common_keys ~ "Common (in both)",
    __composite_key__ %in% only_a_keys ~ paste0("Only in ", config$source_a$label),
    TRUE ~ "Other"
  ))

agg_b <- agg_b %>%
  mutate(overlap_category = case_when(
    __composite_key__ %in% common_keys ~ "Common (in both)",
    __composite_key__ %in% only_b_keys ~ paste0("Only in ", config$source_b$label),
    TRUE ~ "Other"
  ))

################################################################################
#### STEP 6: COMPARISON ANALYSIS - COMMON RECORDS ####
################################################################################

cat("--- STEP 6: Comparison Analysis - Common Records ---\n")

if (length(common_keys) > 0) {
  # Join common records
  common_comparison <- agg_a %>%
    filter(__composite_key__ %in% common_keys) %>%
    inner_join(
      agg_b %>% filter(__composite_key__ %in% common_keys),
      by = "__composite_key__",
      suffix = c("_a", "_b")
    )

  # Calculate differences for each metric
  metric_names <- names(config$source_a$metrics)

  for (metric in metric_names) {
    col_a <- paste0("a_", metric, "_sum")
    col_b <- paste0("b_", metric, "_sum")
    diff_col <- paste0(metric, "_diff")
    pct_col <- paste0(metric, "_pct_change")

    if (col_a %in% names(common_comparison) && col_b %in% names(common_comparison)) {
      common_comparison[[diff_col]] <- common_comparison[[col_b]] - common_comparison[[col_a]]

      common_comparison[[pct_col]] <- ifelse(
        common_comparison[[col_a]] > 0,
        100 * common_comparison[[diff_col]] / common_comparison[[col_a]],
        NA_real_
      )
    }
  }

  cat("✓ Common records detailed comparison:\n")
  cat("  Total records:", nrow(common_comparison), "\n\n")

  # Print metric totals
  for (metric in metric_names) {
    col_a <- paste0("a_", metric, "_sum")
    col_b <- paste0("b_", metric, "_sum")
    diff_col <- paste0(metric, "_diff")

    if (col_a %in% names(common_comparison) && col_b %in% names(common_comparison)) {
      total_a <- sum(common_comparison[[col_a]], na.rm = TRUE)
      total_b <- sum(common_comparison[[col_b]], na.rm = TRUE)
      total_diff <- sum(common_comparison[[diff_col]], na.rm = TRUE)

      cat("  Metric:", metric, "\n")
      cat("    Source A total:", format(total_a, big.mark = ","), "\n")
      cat("    Source B total:", format(total_b, big.mark = ","), "\n")
      cat("    Net change:", format(total_diff, big.mark = ","), "\n\n")
    }
  }

  # Save common comparison
  common_file <- file.path(config$output_dir, paste0(config$comparison$output_prefix, "_common.csv"))
  write.csv(common_comparison, common_file, row.names = FALSE)
  cat("✓ Common records comparison saved:", common_file, "\n\n")
} else {
  cat("⚠ No common records found between sources\n\n")
  common_comparison <- data.frame()
}

################################################################################
#### STEP 7: ANALYSIS - RECORDS ONLY IN SOURCE A ####
################################################################################

cat("--- STEP 7: Analysis - Records Only in Source A ---\n")

only_a_data <- agg_a %>%
  filter(__composite_key__ %in% only_a_keys)

cat("Records only in Source A (", config$source_a$label, "):\n", sep = "")
cat("  Count:", nrow(only_a_data), "\n")

for (metric in names(config$source_a$metrics)) {
  col_name <- paste0("a_", metric, "_sum")
  if (col_name %in% names(only_a_data)) {
    total <- sum(only_a_data[[col_name]], na.rm = TRUE)
    cat("  Total", metric, ":", format(total, big.mark = ","), "\n")
  }
}
cat("\n")

# Save
only_a_file <- file.path(config$output_dir, paste0(config$comparison$output_prefix, "_only_source_a.csv"))
write.csv(only_a_data, only_a_file, row.names = FALSE)
cat("✓ Records only in Source A saved:", only_a_file, "\n\n")

################################################################################
#### STEP 8: ANALYSIS - RECORDS ONLY IN SOURCE B ####
################################################################################

cat("--- STEP 8: Analysis - Records Only in Source B ---\n")

only_b_data <- agg_b %>%
  filter(__composite_key__ %in% only_b_keys)

cat("Records only in Source B (", config$source_b$label, "):\n", sep = "")
cat("  Count:", nrow(only_b_data), "\n")

for (metric in names(config$source_b$metrics)) {
  col_name <- paste0("b_", metric, "_sum")
  if (col_name %in% names(only_b_data)) {
    total <- sum(only_b_data[[col_name]], na.rm = TRUE)
    cat("  Total", metric, ":", format(total, big.mark = ","), "\n")
  }
}
cat("\n")

# Save
only_b_file <- file.path(config$output_dir, paste0(config$comparison$output_prefix, "_only_source_b.csv"))
write.csv(only_b_data, only_b_file, row.names = FALSE)
cat("✓ Records only in Source B saved:", only_b_file, "\n\n")

################################################################################
#### STEP 9: SUMMARY REPORT ####
################################################################################

cat("========================================\n")
cat("VALIDATION SUMMARY REPORT\n")
cat("========================================\n\n")

# Create summary table
summary_rows <- list()

# Common records row
if (length(common_keys) > 0) {
  common_row <- list(Category = "Common (in both)", Record_Count = length(common_keys))
  for (metric in names(config$source_a$metrics)) {
    col_b <- paste0("b_", metric, "_sum")
    if (col_b %in% names(common_comparison)) {
      common_row[[metric]] <- sum(common_comparison[[col_b]], na.rm = TRUE)
    }
  }
  summary_rows <- c(summary_rows, list(common_row))
}

# Only in A row
only_a_row <- list(
  Category = paste0("Only in ", config$source_a$label),
  Record_Count = length(only_a_keys)
)
for (metric in names(config$source_a$metrics)) {
  col_a <- paste0("a_", metric, "_sum")
  if (col_a %in% names(only_a_data)) {
    only_a_row[[metric]] <- sum(only_a_data[[col_a]], na.rm = TRUE)
  }
}
summary_rows <- c(summary_rows, list(only_a_row))

# Only in B row
only_b_row <- list(
  Category = paste0("Only in ", config$source_b$label),
  Record_Count = length(only_b_keys)
)
for (metric in names(config$source_b$metrics)) {
  col_b <- paste0("b_", metric, "_sum")
  if (col_b %in% names(only_b_data)) {
    only_b_row[[metric]] <- sum(only_b_data[[col_b]], na.rm = TRUE)
  }
}
summary_rows <- c(summary_rows, list(only_b_row))

# Total rows
total_a_row <- list(
  Category = paste0("TOTAL (", config$source_a$label, ")"),
  Record_Count = nrow(agg_a)
)
for (metric in names(config$source_a$metrics)) {
  col_a <- paste0("a_", metric, "_sum")
  if (col_a %in% names(agg_a)) {
    total_a_row[[metric]] <- sum(agg_a[[col_a]], na.rm = TRUE)
  }
}
summary_rows <- c(summary_rows, list(total_a_row))

total_b_row <- list(
  Category = paste0("TOTAL (", config$source_b$label, ")"),
  Record_Count = nrow(agg_b)
)
for (metric in names(config$source_b$metrics)) {
  col_b <- paste0("b_", metric, "_sum")
  if (col_b %in% names(agg_b)) {
    total_b_row[[metric]] <- sum(agg_b[[col_b]], na.rm = TRUE)
  }
}
summary_rows <- c(summary_rows, list(total_b_row))

# Convert to data frame
summary_table <- bind_rows(summary_rows)

cat("Overall Summary:\n")
print(summary_table, row.names = FALSE)
cat("\n")

# Save summary
summary_file <- file.path(config$output_dir, paste0(config$comparison$output_prefix, "_summary.csv"))
write.csv(summary_table, summary_file, row.names = FALSE)
cat("✓ Summary report saved:", summary_file, "\n\n")

################################################################################
#### STEP 10: RECOMMENDATIONS ####
################################################################################

cat("========================================\n")
cat("RECOMMENDATIONS\n")
cat("========================================\n\n")

if (length(only_b_keys) > 0) {
  cat("⚠ ATTENTION:", length(only_b_keys), "records in Source B not found in Source A\n")
  cat("   Review '", basename(only_b_file), "' for details.\n\n", sep = "")
}

if (length(common_keys) > 0 && nrow(common_comparison) > 0) {
  # Check for significant changes
  big_changes <- 0
  for (metric in names(config$source_a$metrics)) {
    pct_col <- paste0(metric, "_pct_change")
    if (pct_col %in% names(common_comparison)) {
      big_changes <- big_changes + sum(abs(common_comparison[[pct_col]]) > 50, na.rm = TRUE)
    }
  }

  if (big_changes > 0) {
    cat("⚠ ATTENTION:", big_changes, "common records have >50% change in metrics\n")
    cat("   Review '", basename(common_file), "' to investigate.\n\n", sep = "")
  } else {
    cat("✓ No extreme metric changes detected in common records.\n\n")
  }
}

cat("Next steps:\n")
cat("1. Review validation files in:", config$output_dir, "\n")
cat("2. Verify overlap patterns match expectations\n")
cat("3. Investigate any unexpected records in 'only in B' category\n")
cat("4. Proceed with data integration if validation looks good\n\n")

cat("========================================\n")
cat("Validation completed at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")
