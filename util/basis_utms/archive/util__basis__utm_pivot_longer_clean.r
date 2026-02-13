# =============================================================================
# BASIS UTM DATA PROCESSING SCRIPT - CLEANED VERSION # =============================================================================
  # Purpose: Load UTM data from Excel, clean column names, pivot to long format,
  #          and upload to BigQuery
  # Author: [Your Name]
  # Date: [Current Date]

# LOAD REQUIRED LIBRARIES ====================================================
library(readxl)    # For reading Excel files
library(dplyr)     # For data manipulation
library(tidyr)     # For data reshaping (pivot_longer)
library(janitor)   # For cleaning column names
library(stringr)   # For string manipulation
library(DBI)       # For database connections
library(bigrquery) # For BigQuery operations

# CONFIGURATION VARIABLES =====================================================
# File paths and sheet names - modify these as needed
excel_file_path <- '/Users/eugenetsenter/Downloads/Flight 3 Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx'
sheet_name <- "Flight 3"

# BigQuery configuration
bq_project_id <- "looker-studio-pro-452620"
bq_dataset <- "landing"
bq_table_name <- "basis_utms_pivoted_flight3_v2"

# DEBUGGING CONFIGURATION =====================================================
# Set to TRUE to enable detailed output for debugging
debug_mode <- TRUE

# Helper function for conditional debugging output
debug_print <- function(message, data = NULL, type = "info") {
  if (debug_mode) {
    # Create structured output with clear formatting
    if (type == "header") {
      cat("\n", rep("=", 80), "\n", sep = "")
      cat("  ", toupper(message), "\n")
      cat(rep("=", 80), "\n\n", sep = "")
    } else if (type == "section") {
      cat("\n", rep("-", 60), "\n", sep = "")
      cat("  ", message, "\n")
      cat(rep("-", 60), "\n", sep = "")
    } else if (type == "step") {
      cat("\n>>> ", message, "\n")
    } else {
      cat("    ", message, "\n")
    }
    
    # Handle data output with better formatting
    if (!is.null(data)) {
      if (is.vector(data) && length(data) > 10) {
        cat("    First 10 items: ", paste(head(data, 10), collapse = ", "), "\n")
        cat("    Total items: ", length(data), "\n")
      } else if (is.vector(data)) {
        cat("    Values: ", paste(data, collapse = ", "), "\n")
      } else if (is.data.frame(data)) {
        cat("    Dimensions: ", nrow(data), " rows x ", ncol(data), " columns\n")
      } else {
        print(data)
      }
    }
  }
}

# =============================================================================
# SECTION 1: LOAD UTM DATA FROM EXCEL FILE
# =============================================================================

debug_print("STARTING DATA LOAD PROCESS", type = "header")

# Step 1.1: Auto-detect header row position
# Read first 20 rows to find the header row containing "Property" in first column
temp_data <- read_excel(
  excel_file_path,
  sheet = sheet_name,
  n_max = 20,
  col_names = FALSE
)

# Find the row that contains "Property" in the first column
header_row_position <- which(temp_data[[1]] == "Property")
if(length(header_row_position) == 0) {
  stop("Could not find header row with 'Property' in first column")
}

# Calculate how many rows to skip (header row position minus 1)
rows_to_skip <- header_row_position[1] - 1

debug_print(paste("Found header row at position", header_row_position[1], "- skipping", rows_to_skip, "rows"), type = "step")

# Step 1.2: Load the actual data with correct skip value
utm_data_raw <- read_excel(
  excel_file_path,
  sheet = sheet_name,
  skip = rows_to_skip
) %>%
  clean_names() %>%                                    # Clean column names using janitor
  slice(-1) %>%                                        # Remove first row (often empty or duplicate header)
  mutate(across(everything(), as.character))           # Convert all columns to character type

debug_print("Raw data loaded successfully", type = "step")
debug_print("Column names", colnames(utm_data_raw))

# =============================================================================
# SECTION 2: CLEAN UP COLUMN NAMES
# =============================================================================

debug_print("STARTING COLUMN NAME CLEANUP", type = "header")

# Step 2.1: Create dataframe to track original and cleaned column names
column_cleanup_tracker <- data.frame(original_name = colnames(utm_data_raw))

# Step 2.2: Extract creative numbers and clean field names
column_cleanup_tracker <- column_cleanup_tracker %>%
  mutate(
    # Extract the creative number from column names (e.g., "_5" from "creative_name_5")
    creative_number_extracted = paste0("creative", str_extract(original_name, "_(\\d+)(?=_|$)")),
    
    # Remove the "creative_number_" pattern to get clean field names
    field_name_cleaned = gsub("creative_", "", gsub("_number_\\d+", "", original_name)),
    
    # Remove additional number patterns and clean up underscores
    field_name_cleaned = gsub("number_||\\d+_", "", field_name_cleaned),
    field_name_cleaned = gsub("_number", "", field_name_cleaned)
  )

# Step 2.3: Create final cleaned column names
column_cleanup_tracker$final_column_name <- paste(
  column_cleanup_tracker$creative_number_extracted,
  column_cleanup_tracker$field_name_cleaned,
  sep = "_"
)

# Keep the first 7 columns unchanged (these are typically metadata columns)
column_cleanup_tracker$final_column_name[1:7] <- column_cleanup_tracker$original_name[1:7]

# Apply the cleaned names to the dataframe
colnames(utm_data_raw) <- column_cleanup_tracker$final_column_name

debug_print("Column names cleaned and applied", type = "step")

# =============================================================================
# SECTION 3: STANDARDIZE CREATIVE COLUMN NAMING
# =============================================================================

debug_print("STANDARDIZING CREATIVE COLUMN NAMES", type = "header")

# Step 3.1: Process column names to follow consistent creative_<number>_<attribute> pattern
utm_data_processed <- utm_data_raw

# Extract and clean the main part of column names (removing creative prefix and numbers)
column_names_main <- str_replace(                     # Step 4: Remove leading underscore
  str_remove(                                         # Step 3: Remove leading/trailing numbers and underscores
    str_replace(                                      # Step 2: Remove "number_" prefix
      str_replace(                                    # Step 1: Remove "creative_" prefix
        names(utm_data_processed),
        "creative_",
        ""
      ),
      "number_",
      ""
    ),
    "^[\\d_]+|[\\d_]+$"                              # Remove leading and trailing numbers/underscores
  ), 
  "^_", 
  ""
)

debug_print("Main column names extracted", unique(column_names_main))

# Step 3.2: Create new standardized column names with creative prefix
standardized_column_names <- paste0(
  "creative_",
  str_extract(names(utm_data_processed), "-?\\d*\\.?\\d+"),  # Extract the number from original names
  "_", 
  column_names_main                                           # Add the cleaned main name
)

# Apply the standardized names
names(utm_data_processed) <- standardized_column_names

debug_print("Standardized column names applied", type = "step")
debug_print("New column names", names(utm_data_processed))

# Step 3.3: Final cleanup of column names
# Remove trailing numbers and fix common patterns
names(utm_data_processed) <- names(utm_data_processed) |>
  # Remove trailing numbers from specific field types
  str_replace("(_name|_url|_asset_link|_edo_tag|_disqo_tag|_video_amp_tag|_3p_or_1p_tag)_\\d+$", "\\1") |>
  # Replace multiple underscores with single underscore
  str_replace_all("_+", "_") |>
  # Fix specific known issues
  str_replace("_url_3_number", "_url")

debug_print("Final column name cleanup completed", type = "step")

# =============================================================================
# SECTION 4: PIVOT DATA FROM WIDE TO LONG FORMAT
# =============================================================================

debug_print("STARTING PIVOT OPERATION", type = "header")

# Step 4.1: Identify all creative columns for pivoting
creative_columns <- grep(
  "(name|url|asset_link|edo_tag|disqo_tag|samba_tag|video_amp_tag|3p_or_1p_tag)",
  names(utm_data_processed), 
  value = TRUE
)

debug_print("Creative columns identified", length(creative_columns))

# Step 4.2: Pivot from wide to long format
utm_data_long <- utm_data_processed %>%
  pivot_longer(
    cols = matches("^creative_\\d+_"),                # Select columns starting with 'creative_' + digits + '_'
    names_to = c("creative_num", ".value"),           # Split names into creative number and attribute
    names_pattern = "^creative_(\\d+)_(.*)$",         # Regex pattern to extract number and attribute
    values_drop_na = TRUE                             # Remove rows where the value is NA
  ) %>%
  mutate(creative_num = as.integer(creative_num))     # Convert creative number to integer

debug_print("Pivot operation completed", type = "step")
debug_print("Long format data dimensions", dim(utm_data_long))

# =============================================================================
# SECTION 5: FINAL DATA CLEANUP AND PREPARATION
# =============================================================================

debug_print("FINAL DATA CLEANUP", type = "header")

# Step 5.1: Clean up any remaining column name issues
names(utm_data_long) <- str_replace_all(names(utm_data_long), "creative_NA_", "")

debug_print("Column names after pivot", names(utm_data_long))

# Step 5.2: Select relevant columns for final output
utm_data_final <- utm_data_long %>% 
  select(2:16)  # Select columns 2 through 16 (adjust as needed)

debug_print("Final data columns", colnames(utm_data_final))

# Step 5.3: Rename 'tag' column to 'tag_placement' if it exists
if ("tag" %in% colnames(utm_data_final)) {
  utm_data_final <- utm_data_final %>%
    rename(tag_placement = tag)
  debug_print("Renamed 'tag' column to 'tag_placement'", type = "step")
}

# =============================================================================
# SECTION 6: CREATIVE NAME STANDARDIZATION
# =============================================================================

debug_print("STANDARDIZING CREATIVE NAMES", type = "header")

# Step 6.1: Clean up creative names while preserving originals
utm_data_with_clean_names <- utm_data_final %>%
  mutate(
    name_original = name,                                    # Preserve original values
    name = str_replace_all(name, "1x1", ""),               # Remove "1x1" size indicators
    name = str_replace_all(name, "0x0", ""),               # Remove "0x0" size indicators  
    name = gsub("(?:_?\\d+x\\d+.*)?$", "", name),          # Remove "NxN" size patterns
    name = str_to_lower(name),                             # Convert to lower case
    name = str_replace_all(name, "[^A-Za-z0-9]", ""),      # Remove special characters
    name = str_replace_all(name, "\\s+", "_")              # Replace spaces with underscores
  )

debug_print("Creative name standardization completed", type = "step")
debug_print("Unique cleaned names", unique(utm_data_with_clean_names$name))
debug_print("Sample original names", head(unique(utm_data_final$name)))

# =============================================================================
# SECTION 7: WRITE DATA TO BIGQUERY
# =============================================================================

debug_print("WRITING DATA TO BIGQUERY", type = "header")

# Step 7.1: Create BigQuery connection
bq_connection <- dbConnect(
  bigrquery::bigquery(),
  project = bq_project_id,
  dataset = bq_dataset,
  billing = bq_project_id
)

debug_print(paste("Connected to BigQuery project:", bq_project_id), type = "step")

# Step 7.2: Write data to BigQuery table
DBI::dbWriteTable(
  bq_connection,
  name = bq_table_name,
  value = utm_data_final,  # Using utm_data_final (change to utm_data_with_clean_names if you want cleaned names)
  overwrite = TRUE         # Set to FALSE and use append=TRUE to append instead of overwrite
)

debug_print(paste("Data successfully written to table:", bq_table_name), type = "step")
debug_print(paste("Final data dimensions:", nrow(utm_data_final), "rows,", ncol(utm_data_final), "columns"))

# =============================================================================
# SCRIPT COMPLETION
# =============================================================================

debug_print("SCRIPT EXECUTION COMPLETED SUCCESSFULLY", type = "header")
debug_print(paste("Processed", nrow(utm_data_final), "records"))
debug_print(paste("Data available in BigQuery table:", paste(bq_project_id, bq_dataset, bq_table_name, sep = ".")))

# Optional: Display summary statistics
if (debug_mode) {
  cat("\n=== SUMMARY STATISTICS ===\n")
  cat("Total records processed:", nrow(utm_data_final), "\n")
  cat("Total columns in final dataset:", ncol(utm_data_final), "\n")
  cat("Unique creative numbers:", length(unique(utm_data_final$creative_num)), "\n")
  if ("name" %in% colnames(utm_data_final)) {
    cat("Unique creative names:", length(unique(utm_data_final$name)), "\n")
  }
}
