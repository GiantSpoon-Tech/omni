# BASIS UTM DATA PROCESSING SCRIPT - LOOP VERSION
# =============================================================================
# Purpose: Loop through multiple data sources to load UTM data from Excel files,
#          clean column names, pivot to long format, and upload to BigQuery

# ! use util/extract_creative_names.r to test creative name extraction


# LOAD REQUIRED LIBRARIES ====================================================
library(readxl)    # For reading Excel files
library(dplyr)     # For data manipulation
library(tidyr)     # For data reshaping (pivot_longer)
library(janitor)   # For cleaning column names
library(stringr)   # For string manipulation
library(DBI)       # For database connections
library(bigrquery) # For BigQuery operations

# DATA SOURCES CONFIGURATION =================================================
# Define all data sources to process - modify these as needed
data_sources <- data.frame(
  source_id = c("flight1", "flight2", "flight3", "flight4_1", "flight4_2", "fy26_q1"),
  excel_file_path = c(
    '/Users/eugenetsenter/Downloads/Flight 1_Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx',
    '/Users/eugenetsenter/Downloads/Flight 2_Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx',
    '/Users/eugenetsenter/Downloads/Flight 3 Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx',
    '/Users/eugenetsenter/Downloads/Flight 4 Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx_Flight 9.10-9.30.xlsx',
    '/Users/eugenetsenter/Downloads/Flight 4 Trafficking Sheet_MASSMUTUAL003CP_DSP_10.1 Launch_Flight 10.1-12.31.xlsx',
    '/Users/eugenetsenter/Downloads/MassMutual_FY26_Q1_Traffic Sheet.xlsx'
    
  ),
  sheet_name = c(
    "Q1 Tsheet",
    "Flight 2_Updated", 
    "Flight 3",
    "MASSMUTUAL003CP",
    "10.1_Updated",
    "MASSMUTUAL004_updated 1.14.26"

  ),
  bq_table_name = c(
    "basis_utms_pivoted_flight1_2",
    "basis_utms_pivoted_flight2_2",
    "basis_utms_pivoted_flight3_2",
    "basis_utms_pivoted_flight4_1",
    "basis_utms_pivoted_flight4_2",
    "basis_utms_pivoted_fy26_q1"
  ),
  stringsAsFactors = FALSE
)


# GLOBAL CONFIGURATION =======================================================
# BigQuery configuration (same for all data sources)
bq_project_id <- "looker-studio-pro-452620"
bq_dataset <- "landing"

# Processing configuration
debug_mode <- TRUE              # Set to TRUE to enable detailed output
continue_on_error <- TRUE       # Set to TRUE to continue processing other sources if one fails
sources_to_process <- c(
  # "flight1", "flight2", "flight3", "flight4_1", "flight4_2", 
  "fy26_q1")  # Specify which sources to process


# DEBUGGING CONFIGURATION =====================================================
# Helper function for conditional debugging output
debug_print <- function(message, data = NULL, type = "info", source_id = "") {
  if (debug_mode) {
    # Add source identifier to messages
    prefix <- if (source_id != "") paste0("[", toupper(source_id), "] ") else ""
    
    # Create structured output with clear formatting
    if (type == "header") {
      cat("\n", rep("=", 80), "\n", sep = "")
      cat("  ", prefix, toupper(message), "\n")
      cat(rep("=", 80), "\n\n", sep = "")
    } else if (type == "section") {
      cat("\n", rep("-", 60), "\n", sep = "")
      cat("  ", prefix, message, "\n")
      cat(rep("-", 60), "\n", sep = "")
    } else if (type == "step") {
      cat("\n>>> ", prefix, message, "\n")
    } else if (type == "error") {
      cat("\n!!! ERROR - ", prefix, message, "\n")
    } else if (type == "success") {
      cat("\n✓ SUCCESS - ", prefix, message, "\n")
    } else {
      cat("    ", prefix, message, "\n")
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


# MAIN PROCESSING FUNCTION ===================================================
process_utm_data <- function(source_config) {
  
  source_id <- source_config$source_id
  excel_file_path <- source_config$excel_file_path
  sheet_name <- source_config$sheet_name
  bq_table_name <- source_config$bq_table_name
  
  tryCatch({
    
    debug_print("STARTING DATA LOAD PROCESS", type = "header", source_id = source_id)
    
    
# SECTION 1: LOAD UTM DATA FROM EXCEL FILE
# ==========================================================================

# Step 1.1: Check if file exists
if (!file.exists(excel_file_path)) {
  stop(paste("Excel file not found:", excel_file_path))
}

# Step 1.2: Auto-detect header row position
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

debug_print(paste("Found header row at position", header_row_position[1], "- skipping", rows_to_skip, "rows"), 
            type = "step", source_id = source_id)

# Step 1.3: Load the actual data with correct skip value
utm_data_raw <- read_excel(
  excel_file_path,
  sheet = sheet_name,
  skip = rows_to_skip
) %>%
  clean_names() %>%                                    # Clean column names using janitor
  slice(-1) %>%                                        # Remove first row (often empty or duplicate header)
  mutate(across(everything(), as.character))           # Convert all columns to character type

debug_print("Raw data loaded successfully", type = "step", source_id = source_id)
debug_print("Column names", colnames(utm_data_raw), source_id = source_id)


# SECTION 2: CLEAN UP COLUMN NAMES
# ==========================================================================

debug_print("STARTING COLUMN NAME CLEANUP", type = "header", source_id = source_id)

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

debug_print("Column names cleaned and applied", type = "step", source_id = source_id)


# SECTION 3: STANDARDIZE CREATIVE COLUMN NAMING
# ==========================================================================

debug_print("STANDARDIZING CREATIVE COLUMN NAMES", type = "header", source_id = source_id)

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

debug_print("Main column names extracted", unique(column_names_main), source_id = source_id)

# Step 3.2: Create new standardized column names with creative prefix
standardized_column_names <- paste0(
  "creative_",
  str_extract(names(utm_data_processed), "-?\\d*\\.?\\d+"),  # Extract the number from original names
  "_", 
  column_names_main                                           # Add the cleaned main name
)

# Apply the standardized names
names(utm_data_processed) <- standardized_column_names

debug_print("Standardized column names applied", type = "step", source_id = source_id)
debug_print("New column names", names(utm_data_processed), source_id = source_id)

# Step 3.3: Final cleanup of column names
names(utm_data_processed) <- names(utm_data_processed) |>
  # Remove trailing numbers from specific field types
  str_replace("(_name|_url|_asset_link|_edo_tag|_disqo_tag|_video_amp_tag|_3p_or_1p_tag)_\\d+$", "\\1") |>
  # Replace multiple underscores with single underscore
  str_replace_all("_+", "_") |>
  # Fix specific known issues
  str_replace("_url_3_number", "_url")

debug_print("Final column name cleanup completed", type = "step", source_id = source_id)


# SECTION 4: PIVOT DATA FROM WIDE TO LONG FORMAT
# ==========================================================================

debug_print("STARTING PIVOT OPERATION", type = "header", source_id = source_id)

# Step 4.1: Identify all creative columns for pivoting
creative_columns <- grep(
  "(name|url|asset_link|edo_tag|disqo_tag|samba_tag|video_amp_tag|3p_or_1p_tag)",
  names(utm_data_processed), 
  value = TRUE
)

debug_print("Creative columns identified", length(creative_columns), source_id = source_id)

# Step 4.2: Pivot from wide to long format
utm_data_long <- utm_data_processed %>%
  pivot_longer(
    cols = matches("^creative_\\d+_"),                # Select columns starting with 'creative_' + digits + '_'
    names_to = c("creative_num", ".value"),           # Split names into creative number and attribute
    names_pattern = "^creative_(\\d+)_(.*)$",         # Regex pattern to extract number and attribute
    values_drop_na = TRUE                             # Remove rows where the value is NA
  ) %>%
  mutate(creative_num = as.integer(creative_num))     # Convert creative number to integer

debug_print("Pivot operation completed", type = "step", source_id = source_id)
debug_print("Long format data dimensions", dim(utm_data_long), source_id = source_id)


# SECTION 5: FINAL DATA CLEANUP AND PREPARATION
# ==========================================================================

debug_print("FINAL DATA CLEANUP", type = "header", source_id = source_id)

# Step 5.1: Clean up any remaining column name issues
names(utm_data_long) <- str_replace_all(names(utm_data_long), "creative_NA_", "")

debug_print("Column names after pivot", names(utm_data_long), source_id = source_id)

# Step 5.2: Select relevant columns for final output
utm_data_final <- utm_data_long %>% 
  select(2:16)  # Select columns 2 through 16 (adjust as needed)

debug_print("Final data columns", colnames(utm_data_final), source_id = source_id)

# Step 5.3: Rename 'tag' column to 'tag_placement' if it exists
if ("tag" %in% colnames(utm_data_final)) {
  utm_data_final <- utm_data_final %>%
    rename(tag_placement = tag)
  debug_print("Renamed 'tag' column to 'tag_placement'", type = "step", source_id = source_id)
}


# SECTION 6: CREATIVE NAME STANDARDIZATION
# ==========================================================================

debug_print("STANDARDIZING CREATIVE NAMES", type = "header", source_id = source_id)

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

debug_print("Creative name standardization completed", type = "step", source_id = source_id)
debug_print("Unique cleaned names", unique(utm_data_with_clean_names$name), source_id = source_id)
debug_print("Sample original names", head(unique(utm_data_final$name)), source_id = source_id)


# SECTION 7: WRITE DATA TO BIGQUERY
# ==========================================================================

debug_print("WRITING DATA TO BIGQUERY", type = "header", source_id = source_id)

# Step 7.1: Create BigQuery connection
bq_connection <- dbConnect(
  bigrquery::bigquery(),
  project = bq_project_id,
  dataset = bq_dataset,
  billing = bq_project_id
)

debug_print(paste("Connected to BigQuery project:", bq_project_id), type = "step", source_id = source_id)

# Step 7.2: Write data to BigQuery table
DBI::dbWriteTable(
  bq_connection,
  name = bq_table_name,
  value = utm_data_with_clean_names,  # Using utm_data_final (change to utm_data_with_clean_names if you want cleaned names)
  overwrite = TRUE         # Set to FALSE and use append=TRUE to append instead of overwrite
)
    
    debug_print(paste("Data successfully written to table:", bq_table_name), type = "step", source_id = source_id)
    debug_print(paste("Final data dimensions:", nrow(utm_data_final), "rows,", ncol(utm_data_final), "columns"), 
                source_id = source_id)
    
    
    # SECTION 8: COMPLETION
    # ==========================================================================
    
    debug_print("PROCESSING COMPLETED SUCCESSFULLY", type = "success", source_id = source_id)
    debug_print(paste("Processed", nrow(utm_data_final), "records"), source_id = source_id)
    debug_print(paste("Data available in BigQuery table:", paste(bq_project_id, bq_dataset, bq_table_name, sep = ".")), 
                source_id = source_id)
    
    # Return summary information
    return(list(
      source_id = source_id,
      status = "SUCCESS",
      records_processed = nrow(utm_data_final),
      columns_final = ncol(utm_data_final),
      bq_table = paste(bq_project_id, bq_dataset, bq_table_name, sep = "."),
      error_message = NULL
    ))
    
  }, error = function(e) {
    
    debug_print(paste("PROCESSING FAILED:", e$message), type = "error", source_id = source_id)
    
    # Return error information
    return(list(
      source_id = source_id,
      status = "ERROR",
      records_processed = 0,
      columns_final = 0,
      bq_table = paste(bq_project_id, bq_dataset, bq_table_name, sep = "."),
      error_message = e$message
    ))
  })
}

# MAIN EXECUTION LOOP =========================================================

debug_print("STARTING BATCH PROCESSING OF UTM DATA SOURCES", type = "header")

# Initialize results tracking
processing_results <- list()
total_sources <- length(sources_to_process)
successful_sources <- 0
failed_sources <- 0

# Display processing plan
debug_print(paste("Processing", total_sources, "data sources:"), type = "section")
for (source_id in sources_to_process) {
  source_config <- data_sources[data_sources$source_id == source_id, ]
  if (nrow(source_config) > 0) {
    debug_print(paste("- ", source_id, ": ", basename(source_config$excel_file_path), " -> ", source_config$bq_table_name))
  } else {
    debug_print(paste("- ", source_id, ": CONFIGURATION NOT FOUND"), type = "error")
  }
}

# Process each data source
for (i in seq_along(sources_to_process)) {
  
  source_id <- sources_to_process[i]
  
  debug_print(paste("PROCESSING SOURCE", i, "OF", total_sources), type = "section")
  
  # Get configuration for this source
  source_config <- data_sources[data_sources$source_id == source_id, ]
  
  if (nrow(source_config) == 0) {
    debug_print(paste("Configuration not found for source:", source_id), type = "error")
    
    processing_results[[source_id]] <- list(
      source_id = source_id,
      status = "ERROR",
      records_processed = 0,
      columns_final = 0,
      bq_table = "N/A",
      error_message = "Configuration not found"
    )
    failed_sources <- failed_sources + 1
    next
  }
  
  # Process this source
  result <- process_utm_data(source_config)
  processing_results[[source_id]] <- result
  
  # Update counters
  if (result$status == "SUCCESS") {
    successful_sources <- successful_sources + 1
  } else {
    failed_sources <- failed_sources + 1
    
    # Check if we should continue on error
    if (!continue_on_error) {
      debug_print("Stopping processing due to error (continue_on_error = FALSE)", type = "error")
      break
    }
  }
}


# FINAL SUMMARY REPORT
# =============================================================================

debug_print("BATCH PROCESSING COMPLETED", type = "header")

# Display summary statistics
cat("\n", rep("=", 80), "\n", sep = "")
cat("  FINAL PROCESSING SUMMARY\n")
cat(rep("=", 80), "\n", sep = "")
cat("Total sources processed:", total_sources, "\n")
cat("Successful:", successful_sources, "\n")
cat("Failed:", failed_sources, "\n")
cat("Success rate:", round((successful_sources / total_sources) * 100, 1), "%\n")

# Display detailed results
cat("\nDETAILED RESULTS:\n")
cat(rep("-", 80), "\n", sep = "") # Header separator


for (source_id in names(processing_results)) {
  result <- processing_results[[source_id]]
  cat(sprintf("%-12s | %-7s | %8s records | %s\n", 
              result$source_id,
              result$status,
              format(result$records_processed, big.mark = ","),
              if (result$status == "SUCCESS") result$bq_table else result$error_message))
}

cat(rep("-", 80), "\n", sep = "")

# Final message
if (failed_sources == 0) {
  debug_print("ALL SOURCES PROCESSED SUCCESSFULLY!", type = "success")
} else if (successful_sources > 0) {
  debug_print(paste("PARTIAL SUCCESS:", successful_sources, "of", total_sources, "sources processed successfully"), type = "section")
} else {
  debug_print("ALL SOURCES FAILED TO PROCESS", type = "error")
}

debug_print("Script execution completed.")
