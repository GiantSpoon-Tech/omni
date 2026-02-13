################################################################################
#### EXPLORE NEW FPD GOOGLE SHEET
################################################################################
# Purpose: Read and examine the structure of the new FPD sheet to understand
# its schema and prepare for integration with date ranges
################################################################################

library(googlesheets4)
library(googledrive)
library(dplyr)
library(tidyr)

# Authenticate
gs4_auth(email = "gene.tsenter@giantspoon.com")

# Sheet details
sheet_id <- "1818fclbE_pxNLlCz4dW5zX6ulRRqnnedolbSYVUWnjk"
sheet_url <- paste0("https://docs.google.com/spreadsheets/d/", sheet_id)

cat("========================================\n")
cat("EXPLORING NEW FPD SHEET\n")
cat("========================================\n\n")

cat("Sheet URL:", sheet_url, "\n\n")

# Get sheet metadata
cat("--- STEP 1: Get Sheet Metadata ---\n")
sheet_info <- gs4_get(sheet_id)
cat("Sheet title:", sheet_info$name, "\n")
cat("Available tabs:\n")
print(sheet_info$sheets$name)
cat("\n")

# Try to read from the specific gid (1894007924)
# First, let's see what tab name corresponds to this gid
target_gid <- "1894007924"
tab_info <- sheet_info$sheets %>%
  filter(as.character(id) == target_gid)

if (nrow(tab_info) > 0) {
  tab_name <- tab_info$name[1]
  cat("Target gid", target_gid, "corresponds to tab:", tab_name, "\n\n")
} else {
  cat("Could not find gid", target_gid, "- using first tab\n")
  tab_name <- sheet_info$sheets$name[1]
}

# Read the sheet to examine structure
cat("--- STEP 2: Read Sheet Data ---\n")
cat("Reading from tab:", tab_name, "\n")

# Read first 20 rows to detect header and examine structure
scan_data <- read_sheet(
  ss = sheet_id,
  sheet = tab_name,
  range = "A1:Z50",
  col_names = FALSE
)

cat("First 10 rows (to identify header):\n")
print(head(scan_data, 10))
cat("\n")

# Try to detect header row
cat("--- STEP 3: Detect Header Row ---\n")
header_row <- NA_integer_
for (i in 1:min(20, nrow(scan_data))) {
  row_vals <- as.character(scan_data[i, ])
  non_empty <- sum(!is.na(row_vals) & row_vals != "" & trimws(row_vals) != "")
  if (non_empty >= 3) {
    cat("Row", i, "has", non_empty, "non-empty cells:", paste(row_vals[1:min(5, length(row_vals))], collapse = " | "), "\n")
    if (is.na(header_row)) {
      header_row <- i
    }
  }
}

if (is.na(header_row)) {
  cat("Could not detect header row automatically. Assuming row 1.\n")
  header_row <- 1
} else {
  cat("\nDetected header row:", header_row, "\n")
}

cat("\n--- STEP 4: Read Full Dataset ---\n")
# Read from detected header row
fpd_data <- read_sheet(
  ss = sheet_id,
  sheet = tab_name,
  range = paste0("A", header_row, ":Z"),
  col_names = TRUE
)

cat("Dataset dimensions:", nrow(fpd_data), "rows x", ncol(fpd_data), "columns\n\n")

cat("Column names:\n")
print(names(fpd_data))
cat("\n")

cat("Column types:\n")
print(sapply(fpd_data, class))
cat("\n")

cat("First 5 rows of data:\n")
print(head(fpd_data, 5))
cat("\n")

# Check for package identifiers
cat("--- STEP 5: Identify Package Identifiers ---\n")
possible_pkg_cols <- grep("package|pkg|id", names(fpd_data), ignore.case = TRUE, value = TRUE)
cat("Possible package identifier columns:", paste(possible_pkg_cols, collapse = ", "), "\n")

if (length(possible_pkg_cols) > 0) {
  for (col in possible_pkg_cols) {
    cat("\n", col, "- unique values:", n_distinct(fpd_data[[col]], na.rm = TRUE), "\n")
    cat("  Sample values:", paste(head(unique(fpd_data[[col]]), 5), collapse = ", "), "\n")
  }
}
cat("\n")

# Check for numeric/metric columns
cat("--- STEP 6: Identify Metric Columns ---\n")
numeric_cols <- names(fpd_data)[sapply(fpd_data, function(x) is.numeric(x) || all(grepl("^[0-9,.$%]+$", na.omit(x))))]
cat("Numeric/metric columns:", paste(numeric_cols, collapse = ", "), "\n")

if (length(numeric_cols) > 0) {
  cat("\nMetric summaries:\n")
  for (col in numeric_cols) {
    val <- as.numeric(gsub("[^0-9.-]", "", as.character(fpd_data[[col]])))
    cat("  ", col, ": sum =", sum(val, na.rm = TRUE), ", non-NA count =", sum(!is.na(val)), "\n")
  }
}
cat("\n")

# Check for any date columns (even though user says there aren't any)
cat("--- STEP 7: Check for Date Columns ---\n")
date_cols <- grep("date|start|end|week|month", names(fpd_data), ignore.case = TRUE, value = TRUE)
cat("Possible date columns:", paste(date_cols, collapse = ", "), "\n")

if (length(date_cols) > 0) {
  for (col in date_cols) {
    cat("\n", col, "- sample values:\n")
    print(head(unique(fpd_data[[col]]), 5))
  }
} else {
  cat("(No date columns found - will need to join with Prisma or other source)\n")
}
cat("\n")

# Summary
cat("========================================\n")
cat("SUMMARY\n")
cat("========================================\n")
cat("Sheet has", nrow(fpd_data), "package-level records\n")
cat("Next steps:\n")
cat("1. Identify the package ID column to use for joining\n")
cat("2. Join with Prisma data to get start/end dates for each package\n")
cat("3. Spread metrics evenly across the date range\n")
cat("4. Upload to BigQuery\n")
cat("\n")

# Save exploration results
output_file <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/data/new_fpd_exploration.csv"
write.csv(fpd_data, output_file, row.names = FALSE)
cat("Data saved to:", output_file, "\n")
