################################################################################
#### SIMPLIFIED FPD INGESTION - VERSION 3 ALL FPD FILES via 
#### /Users/eugenetsenter/Library/CloudStorage/GoogleDrive-gene.tsenter@giantspoon.com/.shortcut-targets-by-id/0B0U23i7iN3kZaHJ4NzMyeHp5NW8/Giant Spoon - SHARED (USE THIS ONE)/4. Department Folders/Analytics
#### https://drive.google.com/drive/folders/1d--Bc554eBaRCr8blt1LnUYiOMHQe7jF?usp=drive_link
################################################################################
# Purpose: Rewrite of util_collect_fpd.r as a straightforward, linear script
# with minimal abstractions. Build step-by-step through phases:
# 1. Discover files
# 2. Detect header rows
# 3. Collect column metadata
# 4. Combine all data
#
# Output: Checkpoint CSVs after each phase for inspection
################################################################################

#### LOAD LIBRARIES ####
library(googledrive)
library(googlesheets4)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(janitor)

cat ("\n-----------\n First party data pipeline started at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n-----------\n")

#### CONFIGURATION ####
  # fpd shortcuts folder: 
  # https://drive.google.com/drive/folders/1cMkgbplZ8sPIsDHluIuIEOmbam-BhZtY?usp=drive_link

gdrive_folder_id <- "1cMkgbplZ8sPIsDHluIuIEOmbam-BhZtY"
pattern <- "De Beers | Partner Data"
output_dir <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/FPD_loader/output"
use_saved_phases <- FALSE
# Set the phase you are actively working on (1..7). Saved results will be used for other phases.
current_phase <- 1

# Known KPI metric fields (normalized) to treat as numeric + split across days
# NOTE: keep this list tight to avoid accidentally treating dimensions as metrics.
known_kpi_metrics <- c(
  "spend",
  "impressions",
  "sends",
  "opens",
  "views",
  "completed_views",
  "engagements",
  "clicks",
  "pageviews",
  "conversions",
  "benchmark_metric",
  "benchmark"
)

# Per-client week configuration: which day the reporting week starts on.
# Used to fill in missing `week` values in the final output.
# Clients are matched case-insensitively against the `client` column.
client_week_config <- data.frame(
  client     = c("mass", "oli", "adif", "apollo"),
  week_start = c("Sun",  "Sun", "Sun",  "Sun"),
  stringsAsFactors = FALSE
)
default_week_start <- "Sun"

# Mapping from day abbreviation to lubridate floor_date week_start number
# (lubridate uses: 1=Mon, 2=Tue, ... 7=Sun)
day_to_wstart <- c(Mon = 1, Tue = 2, Wed = 3, Thu = 4, Fri = 5, Sat = 6, Sun = 7)

# Predefine checkpoint paths so they can be reused when loading saved results
phase1_output <- file.path(output_dir, "phase1_discovered_files.csv")
phase2_output <- file.path(output_dir, "phase2_header_detection.csv")
phase3_output <- file.path(output_dir, "phase3_raw_headers.csv")
phase4_output <- file.path(output_dir, "phase4_normalization_mapping.csv")
phase5_output <- file.path(output_dir, "phase5_combined_master_data.csv")
phase6_output <- file.path(output_dir, "phase6_cleaned_master_data.csv")
phase7_output <- file.path(output_dir, "phase7_daily_master_data.csv")
phase6_filter_audit_output <- file.path(output_dir, "phase6_filter_audit.csv")
phase7_validation_output <- file.path(output_dir, "phase7_validation_table.csv")

# Keep validation results available for final end-of-run reporting
validation_table <- data.frame()
validation_diff_cols <- character(0)

################################################################################
#### PHASE 1: GOOGLE DRIVE DISCOVERY ####
################################################################################
# Goal: Find all spreadsheets matching "De Beers | Partner Data" pattern
# Output: Dataframe with sheet IDs, names, URLs
# Checkpoint: phase1_discovered_files.csv

cat("\n=== PHASE 1: GOOGLE DRIVE DISCOVERY ===\n")
# If requested, load saved Phase 1 results unless this is the active phase
if (use_saved_phases && current_phase != 1 && file.exists(phase1_output)) {
  cat("Loading saved Phase 1 results from:", phase1_output, "\n")
  discovered_files <- read_csv(phase1_output, show_col_types = FALSE)
  # ensure columns have expected names
  if (!"sheet_id" %in% names(discovered_files) && "id" %in% names(discovered_files)) {
    discovered_files <- discovered_files %>% rename(sheet_id = id)
  }
  if (!"sheet_name" %in% names(discovered_files) && "name" %in% names(discovered_files)) {
    discovered_files <- discovered_files %>% rename(sheet_name = name)
  }
  cat("✓ Loaded", nrow(discovered_files), "sheets from saved Phase 1 file\n")
} else {
  # Get folder reference and query Drive
  cat("Accessing Google Drive folder:", gdrive_folder_id, "\n")
  gdrive_folder <- drive_get(as_id(gdrive_folder_id))
  cat("Searching for sheets matching pattern:", pattern, "\n")
  discovered_files <- drive_ls(
    as_id(gdrive_folder_id),
    pattern = pattern,
    recursive = TRUE,
    type = "spreadsheet",
    n_max = Inf
  )

  # Add full URLs and normalize column names, extract modified date and user from drive metadata
  discovered_files <- discovered_files %>%
    mutate(
      sheet_url = paste0("https://docs.google.com/spreadsheets/d/", id),
      .after = name
    ) %>%
    rename(sheet_id = id, sheet_name = name) %>%
    mutate(
      # Extract last modified date from drive_resource metadata
      last_modified_time = if_else(
        !is.na(drive_resource),
        tryCatch(
          as.POSIXct(sapply(drive_resource, function(x) x$modifiedTime), format = "%Y-%m-%dT%H:%M:%S"),
          error = function(e) as.POSIXct(NA)
        ),
        as.POSIXct(NA)
      ),
      # Extract last modified by from drive_resource metadata
      last_modified_by = if_else(
        !is.na(drive_resource),
        tryCatch(
          sapply(drive_resource, function(x) x$lastModifyingUser$displayName %||% x$lastModifyingUser$emailAddress %||% NA_character_),
          error = function(e) NA_character_
        ),
        NA_character_
      )
    )

  # Filter out ARCHIVE sheets upstream so they never enter later phases
  # (case-insensitive match on sheet name)
  before_archive_filter <- nrow(discovered_files)
  discovered_files <- discovered_files %>%
    filter(!str_detect(sheet_name, "(?i)archive"))
  cat("✓ Found", before_archive_filter, "matching sheets (", nrow(discovered_files), "after removing ARCHIVE)\n")

  # Write checkpoint
  write_csv(discovered_files, phase1_output)
  cat("✓ Phase 1 checkpoint saved to:", phase1_output, "\n")
}

print(discovered_files)

################################################################################
#### PHASE 2: HEADER ROW DETECTION (PER-FILE) ####
################################################################################
# Goal: For each discovered file, identify where the data table starts
# Method: Read columns A:G, find first row with any text = header row
# Output: Dataframe with sheet name, URL, detected header row number
# Checkpoint: phase2_header_detection.csv

cat("\n=== PHASE 2: HEADER ROW DETECTION ===\n")

# If requested, load saved Phase 2 results unless this is the active phase
if (use_saved_phases && current_phase != 2 && file.exists(phase2_output)) {
  cat("Loading saved Phase 2 results from:", phase2_output, "\n")
  phase2_results <- read_csv(phase2_output, show_col_types = FALSE)
  cat("✓ Loaded", nrow(phase2_results), "phase2 rows from saved file\n")
} else {
  # Initialize results list
  header_detection_results <- list()

  # Loop through each discovered file
  for (i in seq_len(nrow(discovered_files))) {
    sheet_name <- discovered_files$sheet_name[i]
    sheet_id <- discovered_files$sheet_id[i]
    sheet_url <- discovered_files$sheet_url[i]
    last_mod_time <- discovered_files$last_modified_time[i]
    last_mod_by <- discovered_files$last_modified_by[i]
    
    cat("\nProcessing file", i, "of", nrow(discovered_files), ":", sheet_name, "\n")
    
    tryCatch({
      # Read columns A:G from 'data' tab (use for scanning header row)
      # Use a large range (A1:G500) to ensure we capture all rows including those further down
      scan_range <- "A1:G500"
      
      cat("  Scanning columns A:G for header row...\n")
      sheet_data_scan <- suppressMessages(read_sheet(
        ss = sheet_id,
        sheet = "data",
        range = scan_range,
        col_names = FALSE
      ))
      
      if (nrow(sheet_data_scan) == 0) {
        cat("  ✗ ERROR: Sheet is empty\n")
        header_detection_results[[i]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
          last_modified_time = last_mod_time,
          last_modified_by = last_mod_by,
          header_row = NA_integer_,
          status = "empty_sheet",
          stringsAsFactors = FALSE
        )
        next
      }
      
      # Find header row: look for row with most non-empty cells (likely the header)
      # Headers typically have 5+ columns filled
      header_row_detected <- NA_integer_
      max_non_empty <- 0
      
      for (row_num in seq_len(nrow(sheet_data_scan))) {
        row_values <- as.character(sheet_data_scan[row_num, ])
        non_empty_count <- sum(!is.na(row_values) & row_values != "" & trimws(row_values) != "")
        
        # Header should have at least 5 columns and more filled cells than previous rows
        if (non_empty_count >= 5 && non_empty_count > max_non_empty) {
          header_row_detected <- row_num
          max_non_empty <- non_empty_count
        }
      }
      
      if (is.na(header_row_detected)) {
        cat("  ✗ ERROR: Could not find header row (no row with 5+ columns)\n")
        header_detection_results[[i]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
          last_modified_time = last_mod_time,
          last_modified_by = last_mod_by,
          header_row = NA_integer_,
          status = "no_header_found",
          stringsAsFactors = FALSE
        )
        next
      }
      
      cat("  ✓ Header row detected at row:", header_row_detected, "with", max_non_empty, "columns\n")
      header_detection_results[[i]] <- data.frame(
        sheet_name = sheet_name,
        sheet_id = sheet_id,
        sheet_url = sheet_url,
        last_modified_time = last_mod_time,
        last_modified_by = last_mod_by,
        header_row = header_row_detected,
        status = "success",
        stringsAsFactors = FALSE
      )
      
    }, error = function(e) {
      cat("  ✗ ERROR:", e$message, "\n")
      header_detection_results[[i]] <<- data.frame(
        sheet_name = sheet_name,
        sheet_id = sheet_id,
        sheet_url = sheet_url,
        last_modified_time = last_mod_time,
        last_modified_by = last_mod_by,
        header_row = NA_integer_,
        status = paste("error:", e$message),
        stringsAsFactors = FALSE
      )
    })
  }

  # Combine all detection results
  phase2_results <- bind_rows(header_detection_results)

  cat("\n=== PHASE 2 SUMMARY ===\n")
  cat("Total sheets processed:", nrow(phase2_results), "\n")
  cat("Successful detections:", sum(phase2_results$status == "success"), "\n")
  cat("Failed/Empty sheets:", nrow(phase2_results) - sum(phase2_results$status == "success"), "\n")

  # Write checkpoint
  write_csv(phase2_results, phase2_output)
  cat("✓ Phase 2 checkpoint saved to:", phase2_output, "\n")
}

#view(phase2_results)

################################################################################
#### PHASE 3: COLUMN HEADER INGESTION & METADATA COLLECTION ####
################################################################################
    # Goal: Extract all column headers from the 'data' tab of each file
    # Method: Use detected header row from Phase 2, read columns A:Y to get full table width
    # Output: Dataframe with sheet name, URL, column name, position
    # Checkpoint: phase3_raw_headers.csv


    cat("\n=== PHASE 3: COLUMN HEADER INGESTION ===\n")

    # Initialize results list
    header_metadata_results <- list()
    header_metadata_index <- 1

    # Filter to only successful detections from Phase 2
    compute_phase3 <- FALSE
    if (use_saved_phases && current_phase != 3 && file.exists(phase3_output)) {
    cat("Loading saved Phase 3 results from:", phase3_output, "\n")
    phase3_results <- read_csv(phase3_output, show_col_types = FALSE)
    # Validate loaded structure
    if (!"column_name_raw" %in% names(phase3_results)) {
        cat("  ⚠ Saved Phase 3 file missing expected columns; recomputing Phase 3\n")
        compute_phase3 <- TRUE
    } else {
        cat("✓ Loaded", nrow(phase3_results), "phase3 rows from saved file\n")
    }
    } else {
    compute_phase3 <- TRUE
    }

    if (compute_phase3) {
    successful_files <- phase2_results %>%
        filter(status == "success")

    cat("Processing", nrow(successful_files), "sheets with detected headers...\n")

  # Loop through each file with a successful header detection
  for (i in seq_len(nrow(successful_files))) {
    sheet_name <- successful_files$sheet_name[i]
    sheet_id <- successful_files$sheet_id[i]
    sheet_url <- successful_files$sheet_url[i]
    last_mod_time <- successful_files$last_modified_time[i]
    last_mod_by <- successful_files$last_modified_by[i]
    header_row <- successful_files$header_row[i]
    
    cat("\nProcessing file", i, "of", nrow(successful_files), ":", sheet_name, "\n")
        
        tryCatch({
        # Read the data starting from detected header row to column S (full table width)
        # Use this row as header (col_names = TRUE)
      cat("  Reading columns A:Y starting from row", header_row, "...\n")
      
      sheet_data <- suppressMessages(read_sheet(
        ss = sheet_id,
        sheet = "data",
        range = paste0("A", header_row, ":Y"),
        col_names = TRUE
      ))
      
      if (nrow(sheet_data) == 0 || ncol(sheet_data) == 0) {
        cat("  ✗ ERROR: No data found after header row\n")
        # Record an empty result row and continue to next file
        header_metadata_results[[header_metadata_index]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
          last_modified_time = last_mod_time,
          last_modified_by = last_mod_by,
          column_position = NA_integer_,
          column_name_raw = NA_character_,
          stringsAsFactors = FALSE
        )
        header_metadata_index <- header_metadata_index + 1
        next
      }        # Extract column names (as read by googlesheets4)
        col_names <- names(sheet_data)
        
        cat("  ✓ Found", length(col_names), "columns\n")
        
      # Create metadata record for each column
      for (col_idx in seq_along(col_names)) {
        col_name <- col_names[col_idx]
        
        header_metadata_results[[header_metadata_index]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
          last_modified_time = last_mod_time,
          last_modified_by = last_mod_by,
          column_position = col_idx,
          column_name_raw = col_name,
          stringsAsFactors = FALSE
        )
        header_metadata_index <- header_metadata_index + 1
      }        }, error = function(e) {
        cat("  ✗ ERROR:", e$message, "\n")
        })
    }

    # Combine all header metadata
    phase3_results <- bind_rows(header_metadata_results)

    # Add frequency column (count how many sheets have each column name)
    phase3_results <- phase3_results %>%
        group_by(column_name_raw) %>%
        mutate(
        frequency_across_sheets = n_distinct(sheet_name),
        .after = column_name_raw
        ) %>%
        ungroup() %>%
        arrange(column_name_raw, sheet_name)

    # Write checkpoint
    write_csv(phase3_results, phase3_output)
    cat("✓ Phase 3 checkpoint saved to:", phase3_output, "\n")

    }

    if (!compute_phase3) {
    # A saved Phase 3 was loaded earlier; ensure it has the expected helper column
    if (!"frequency_across_sheets" %in% names(phase3_results)) {
        phase3_results <- phase3_results %>%
        group_by(column_name_raw) %>%
        mutate(frequency_across_sheets = n_distinct(sheet_name)) %>%
        ungroup()
    }
    } else {
    # compute_phase3 was TRUE and we already wrote the checkpoint above; nothing more to do
    }

    cat("\n=== PHASE 3 SUMMARY ===\n")
    cat("Total sheets processed:", n_distinct(phase3_results$sheet_name), "\n")
    cat("Total columns extracted:", nrow(phase3_results), "\n")
    cat("Unique column names:", n_distinct(phase3_results$column_name_raw), "\n")

    # Ensure checkpoint path and write updated phase3_results
    phase3_output <- file.path(output_dir, "phase3_raw_headers.csv")
    write_csv(phase3_results, phase3_output)
    cat("✓ Phase 3 checkpoint saved to:", phase3_output, "\n")

    # Show most frequent column names (to help with normalization)
    cat("\nMost frequent column names across sheets:\n")
    col_frequency <- phase3_results %>%
    distinct(column_name_raw, frequency_across_sheets) %>%
    arrange(desc(frequency_across_sheets)) %>%
    head(20)

    #print(col_frequency)

    #view(phase3_results)
#
################################################################################
#### PHASE 4: COLUMN NORMALIZATION MAPPING ####
################################################################################
# Goal: Build a simple normalization mapping from raw column names -> normalized names
# Rules: a few heuristic string matches (package, package_id, dates, metrics). Drop auto-generated
# columns that start with "...". Output: phase4_normalization_mapping.csv

cat("\n=== PHASE 4: COLUMN NORMALIZATION MAPPING ===\n")

# Load saved Phase 4 map if available and not the active phase
if (use_saved_phases && current_phase != 4 && file.exists(phase4_output)) {
  cat("Loading saved Phase 4 mapping from:", phase4_output, "\n")
  phase4_map <- read_csv(phase4_output, show_col_types = FALSE)
  cat("✓ Loaded", nrow(phase4_map), "mappings from saved file\n")
} else {

# Work on distinct raw column names from phase3
unique_cols <- phase3_results %>%
  distinct(column_name_raw, frequency_across_sheets) %>%
  filter(!is.na(column_name_raw))

# Helper: fallback using janitor to produce a clean name
fallback_clean <- function(x) {
  x_char <- as.character(x)
  # make_clean_names returns lower_snake_case
  nm <- tryCatch(janitor::make_clean_names(x_char), error = function(e) NA_character_)
  if (is.na(nm) || nm == "") nm <- NA_character_
  nm
}

mapping_rows <- list()
mi <- 1

for (i in seq_len(nrow(unique_cols))) {
  raw <- unique_cols$column_name_raw[i]
  freq <- unique_cols$frequency_across_sheets[i]
  rule <- NA_character_
  norm <- NA_character_

  # Skip auto-generated blank columns
  if (!is.na(raw) && startsWith(raw, "...")) {
    rule <- "drop_autogenerated"
    norm <- NA_character_
  } else {
    rlow <- tolower(raw)

    # Exact or strong matches first
    if (str_detect(rlow, "package\\s*/\\s*placement|placement\\s*/\\s*package")) {
      norm <- "partner_packagePlacement_name"; rule <- "match_package_placement"
    } else if (str_detect(rlow, "package_id") || str_detect(rlow, "packageid") || str_detect(rlow, "package id")) {
      norm <- "package_id"; rule <- "match_package_id"
    } else if (str_detect(rlow, "\\bP[A-Za-z0-9]{6}\\b")) {
      norm <- "package_id"; rule <- "regex_package_code"
    } else if (str_detect(rlow, "\\bplacement\\b")) {
      norm <- "partner_placement_name"; rule <- "match_placement_name"
    } else if (str_detect(rlow, "\\bcreative\\b")) {
      norm <- "partner_creative_name"; rule <- "match_creative_name"
    } else if (str_detect(rlow, "\\bpackage\\b") || str_detect(rlow, "package_name")) {
      norm <- "package_name"; rule <- "match_package_name"
    } else if (str_detect(rlow, "prisma[_ ]?start|prisma[_ ]?start[_ ]?date")) {
      norm <- "prisma_start_date"; rule <- "match_prisma_start_date"
    } else if (str_detect(rlow, "start[_ ]?date") || str_detect(rlow, "^start")) {
      norm <- "start_date"; rule <- "match_start_date"
    } else if (str_detect(rlow, "prisma[_ ]?end|prisma[_ ]?end[_ ]?date")) {
      norm <- "prisma_end_date"; rule <- "match_prisma_end_date"
    } else if (str_detect(rlow, "end[_ ]?date") || str_detect(rlow, "^end")) {
      norm <- "end_date"; rule <- "match_end_date"
    } else if (str_detect(rlow, "week")) {
      norm <- "week"; rule <- "match_week"
    } else if (str_detect(rlow, "(^|[^a-z])date($|[^a-z])") || rlow == "date") {
      norm <- "date"; rule <- "match_date"
    } else if (str_detect(rlow, "spend|cost|budget")) {
      norm <- "spend"; rule <- "match_spend"
    } else if (str_detect(rlow, "impress")) {
      norm <- "impressions"; rule <- "match_impressions"
    } else if (str_detect(rlow, "click")) {
      norm <- "clicks"; rule <- "match_clicks"
    } else if (str_detect(rlow, "sends?\\b|send\\b") && !str_detect(rlow, "spend")) {
      norm <- "sends"; rule <- "match_sends"
    } else if (str_detect(rlow, "completed[_ ]?view|completedview")) {
      norm <- "completed_views"; rule <- "match_completed_views"
    } else if (str_detect(rlow, "page[_ ]?view")) {
      norm <- "pageviews"; rule <- "match_pageviews"
    } else if (str_detect(rlow, "\\bview\\b")) {
      norm <- "views"; rule <- "match_views"
    } else if (str_detect(rlow, "open")) {
      norm <- "opens"; rule <- "match_opens"
    } else {
      # Fallback: use janitor to create a clean name
      token <- fallback_clean(raw)
      norm <- token
      rule <- "fallback_janitor"
    }
  }

  mapping_rows[[mi]] <- data.frame(
    column_name_raw = raw,
    frequency_across_sheets = freq,
    normalized_name = norm,
    rule_applied = rule,
    stringsAsFactors = FALSE
  )
  mi <- mi + 1
}

phase4_map <- bind_rows(mapping_rows) %>%
  arrange(desc(frequency_across_sheets), column_name_raw)

# Write checkpoint
phase4_output <- file.path(output_dir, "phase4_normalization_mapping.csv")
write_csv(phase4_map, phase4_output)
cat("✓ Phase 4 checkpoint saved to:", phase4_output, "\n")

#cat("\nSample mappings (top rows):\n")
# view(head(phase4_map, 40))


cat("\nPhase 4 complete. Next: Phase 5 will ingest data using this mapping.\n")

} # end - compute phase4_map

################################################################################
#### PHASE 5: DATA INGESTION & COMBINATION ####
################################################################################
# Goal: Read each sheet starting at detected header row (A:Y), normalize columns using
# the Phase 4 mapping, drop auto-generated columns (those starting with "..."),
# clean numeric and date columns, add source metadata, and combine into master CSV.

cat("\n=== PHASE 5: DATA INGESTION & COMBINATION ===\n")

if (use_saved_phases && current_phase != 5 && file.exists(phase5_output)) {
  cat("Loading saved Phase 5 output from:", phase5_output, "\n")
  master_df <- read_csv(phase5_output, show_col_types = FALSE)
  cat("✓ Loaded", nrow(master_df), "rows from saved Phase 5 file\n")
} else {

# Prepare mapping lookup: a named vector raw -> normalized
map_lookup <- phase4_map %>%
  filter(!is.na(normalized_name)) %>%
  select(column_name_raw, normalized_name)
map_vec <- setNames(map_lookup$normalized_name, map_lookup$column_name_raw)

# Metrics to coerce to numeric (use configured KPI list)
metrics_expected <- known_kpi_metrics

combined_list <- list()
ci <- 1

# Ensure list of files to ingest is available (from Phase 2 results)
successful_files <- phase2_results %>% filter(status == "success")

  for (i in seq_len(nrow(successful_files))) {
    sheet_name <- successful_files$sheet_name[i]
    sheet_id <- successful_files$sheet_id[i]
    sheet_url <- successful_files$sheet_url[i]
    last_mod_time <- successful_files$last_modified_time[i]
    last_mod_by <- successful_files$last_modified_by[i]
    header_row <- successful_files$header_row[i]

    cat("\nIngesting file", i, "of", nrow(successful_files), ":", sheet_name, "\n")

  tryCatch({
    df <- suppressMessages(read_sheet(
      ss = sheet_id,
      sheet = "data",
      range = paste0("A", header_row, ":Y"),
      col_names = TRUE
    ))

    # Drop columns that are auto-generated blanks (start with ...)
    df <- df %>% select(-starts_with("..."))

    # If dataframe is empty after dropping, skip
    if (nrow(df) == 0 || ncol(df) == 0) {
      cat("  ✗ Skipping - no usable data after dropping autogenerated columns\n")
      next
    }

    # Current column names (raw) as read
    raw_names <- names(df)

    # Build rename mapping for this sheet
    rename_map <- list()
    for (cn in raw_names) {
      if (cn %in% names(map_vec)) {
        newn <- map_vec[[cn]]
      } else {
        # fallback: janitor clean name
        newn <- janitor::make_clean_names(cn)
      }
      # ensure non-empty
      if (is.na(newn) || newn == "") newn <- cn
      rename_map[[cn]] <- newn
    }

    # Apply renaming (resolve duplicates with make.unique)
    new_names <- unname(unlist(rename_map))
    new_names <- make.unique(new_names, sep = "__")
    names(df) <- new_names

    # --- Normalise common column name variants (undo __N suffixes for canonical fields) ---
    normalize_after_unique <- function(nm) {
      nlow <- tolower(as.character(nm))
      # drop trailing unique suffix like __1, __2
      nlow <- sub("__\\d+$", "", nlow)
      # replace dots/spaces/hyphens with underscore for matching
      # NOTE: In R string literals, "\\." is an invalid escape. Use a safe character class.
      nlow <- gsub("[.[:space:]-]+", "_", nlow)
      if (grepl("^(start[_ ]?date|^start)$", nlow)) return("start_date")
      if (grepl("^(end[_ ]?date|^end)$", nlow)) return("end_date")
      if (grepl("prisma[_ ]?start|prisma[_ ]?start[_ ]?date", nlow)) return("prisma_start_date")
      if (grepl("prisma[_ ]?end|prisma[_ ]?end[_ ]?date", nlow)) return("prisma_end_date")
      if (grepl("\\bweek\\b", nlow)) return("week")
      if (grepl("\\bmonth\\b", nlow)) return("month")
      if (grepl("\\bdate\\b", nlow)) return("date")
      if (grepl("spend|cost|budget", nlow)) return("spend")
      if (grepl("impress", nlow)) return("impressions")
      if (grepl("click", nlow)) return("clicks")
      return(nm)
    }

    canon_names <- vapply(names(df), normalize_after_unique, FUN.VALUE = character(1), USE.NAMES = FALSE)
    names(df) <- make.unique(canon_names, sep = "__")

    # Coerce numeric metric columns: remove $ and , then as.numeric
    library(readr)

    numeric_candidates <- intersect(metrics_expected, names(df))

    if (length(numeric_candidates) > 0) {
        df <- df %>%
            mutate(
            across(
                all_of(numeric_candidates),
                ~ parse_number(as.character(.x))  # handles $, commas, spaces, etc.
            )
            )
    }

    # Parse date columns if present (include prisma_* variants)
    date_candidates <- intersect(c("start_date", "end_date", "prisma_start_date", "prisma_end_date", "week", "date"), names(df))
    for (dc in date_candidates) {
      df[[dc]] <- suppressWarnings(as.Date(as.character(df[[dc]]), tryFormats = c("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y")))
    }

    # Add metadata columns
    df <- df %>% mutate(
      source_file = sheet_name, 
      source_url = sheet_url,
      last_modified_time = last_mod_time,
      last_modified_by = last_mod_by,
      partner_sheet = str_extract(sheet_name, "^[^|]+") %>% str_trim()
    )

    combined_list[[ci]] <- df
    ci <- ci + 1

    cat("  ✓ Ingested", nrow(df), "rows and", ncol(df), "cols\n")

  }, error = function(e) {
    cat("  ✗ ERROR ingesting:", e$message, "\n")
  })
}

# Combine all ingested data
if (length(combined_list) == 0) {
  cat("\nNo data ingested. Phase 5 ends with no output.\n")
} else {
  master_df <- bind_rows(combined_list)

  # Filter rows upstream (before output + before validation) to remove rows with no KPI signal.
  # This matches the intent of the Phase 6 filter but avoids Phase 5 vs Phase 7 mismatches.
  kpi_present <- intersect(known_kpi_metrics, names(master_df))
  if (length(kpi_present) > 0) {
    rows_before <- nrow(master_df)
    master_df <- master_df %>%
      mutate(
        row_kpi_sum = rowSums(across(all_of(kpi_present), ~ dplyr::coalesce(as.numeric(.), 0)), na.rm = TRUE)
      ) %>%
      filter(!(is.na(row_kpi_sum) | row_kpi_sum == 0)) %>%
      select(-row_kpi_sum)
    cat("✓ Phase 5 pre-filter removed", rows_before - nrow(master_df), "rows where KPI metric sum is NA or 0\n")
  } else {
    cat("⚠ Phase 5 pre-filter skipped: none of known_kpi_metrics found in master_df\n")
  }

  phase5_output <- file.path(output_dir, "phase5_combined_master_data.csv")
  write_csv(master_df, phase5_output)
  cat("\n✓ Phase 5 complete. Combined data written to:", phase5_output, "\n")
  cat("Total rows:", nrow(master_df), "Total cols:", ncol(master_df), "\n")
}

} # end - Phase 5 checkpoint conditional

# --- Phase 5 Summary ---
cat("\n=== PHASE 5 SUMMARY ===\n")

# Metric totals
numeric_cols <- names(master_df)[sapply(master_df, is.numeric)]
if (length(numeric_cols) > 0) {
  cat("Metric Totals:\n")
  metric_sums <- master_df %>%
    summarise(across(all_of(numeric_cols), ~ sum(.x, na.rm = TRUE)))
  print(metric_sums)
}

# By sheet summary
cat("\nSummary by Sheet:\n")

# Helper: safe min/max across multiple date columns
safe_min_date <- function(df, cols) {
  cols <- intersect(cols, names(df))
  if (length(cols) == 0) return(as.Date(NA))
  x <- do.call(c, lapply(cols, function(cn) as.Date(df[[cn]])))
  if (all(is.na(x))) as.Date(NA) else min(x, na.rm = TRUE)
}

safe_max_date <- function(df, cols) {
  cols <- intersect(cols, names(df))
  if (length(cols) == 0) return(as.Date(NA))
  x <- do.call(c, lapply(cols, function(cn) as.Date(df[[cn]])))
  if (all(is.na(x))) as.Date(NA) else max(x, na.rm = TRUE)
}

date_cols_any <- c(
  "start_date", "end_date", "prisma_start_date", "prisma_end_date",
  "week", "date", "month",
  "start_date_final", "end_date_final", "date_final"
)

# package count preference: package_id else package_name
package_key <- if ("package_id" %in% names(master_df)) "package_id" else if ("package_name" %in% names(master_df)) "package_name" else NA_character_

sheet_summary <- master_df %>%
  mutate(
    source_file = as.character(source_file),
    source_url = as.character(source_url)
  ) %>%
  group_by(source_file, last_modified_time, last_modified_by) %>%
  summarise(
    row_count = n(),
    package_count = if (!is.na(package_key)) n_distinct(.data[[package_key]], na.rm = TRUE) else NA_integer_,
    spend_sum = if ("spend" %in% names(dplyr::cur_data())) sum(spend, na.rm = TRUE) else NA_real_,
    impressions_sum = if ("impressions" %in% names(dplyr::cur_data())) sum(impressions, na.rm = TRUE) else NA_real_,
    min_date_any = safe_min_date(dplyr::cur_data(), date_cols_any),
    min_date_col = {
      min_val <- safe_min_date(dplyr::cur_data(), date_cols_any)
      cols <- intersect(date_cols_any, names(dplyr::cur_data()))
      if (is.na(min_val) || length(cols) == 0) {
        NA_character_
      } else {
        # Find which column(s) contain this min date
        matches <- sapply(cols, function(cn) any(as.Date(dplyr::cur_data()[[cn]]) == min_val, na.rm = TRUE))
        paste(names(matches)[matches], collapse = ", ")
      }
    },
    min_date_reported = safe_min_date(dplyr::cur_data(), c("date", "week", "month", "start_date")),
    # add column used for max_date_any
    max_date_reported = safe_max_date(dplyr::cur_data(), c("date", "week", "month", "end_date")),
    max_date_any = safe_max_date(dplyr::cur_data(), date_cols_any),
    across(all_of(setdiff(numeric_cols, c("spend", "impressions"))), ~ sum(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  arrange(source_file)

# Update Phase 1 checkpoint with per-sheet summary metrics
if (exists("discovered_files") && nrow(discovered_files) > 0) {
  phase1_enriched <- discovered_files %>%
    left_join(sheet_summary %>% select(source_file, row_count, package_count, spend_sum, impressions_sum, min_date_any, max_date_any),
              by = c("sheet_name" = "source_file"))

  write_csv(phase1_enriched, phase1_output)
  cat("✓ Phase 1 checkpoint updated with per-sheet rollups:", phase1_output, "\n")
}

view(sheet_summary)

cat("\n=== END PHASE 5 SUMMARY ===\n")

################################################################################
#### PHASE 6: CLEANING & DATE FINALIZATION ####
################################################################################
# Goal: Post-process the combined master (`phase5_combined_master_data.csv`) to
# 1) extract `package_id` from `package_name` when missing
# 2) standardize date columns (`start_date`, `end_date`, `week`, `date`, `month`) to Date
# 3) compute `start_date_final` and `end_date_final` for every row

cat("\n=== PHASE 6: CLEANING & DATE FINALIZATION ===\n")

phase6_output <- file.path(output_dir, "phase6_cleaned_master_data.csv")
phase6_filter_audit <- data.frame(
  source_file = character(0),
  filter_reason = character(0),
  removed_rows = integer(0),
  stringsAsFactors = FALSE
)

if (use_saved_phases && current_phase != 6 && file.exists(phase6_output)) {
  cat("Loading saved Phase 6 output from:", phase6_output, "\n")
  phase6_df <- read_csv(phase6_output, show_col_types = FALSE)
  cat("✓ Loaded", nrow(phase6_df), "rows from saved Phase 6 file\n")

  if (file.exists(phase6_filter_audit_output)) {
    phase6_filter_audit <- read_csv(phase6_filter_audit_output, show_col_types = FALSE)
    cat("✓ Loaded Phase 6 filter audit from:", phase6_filter_audit_output, "\n")
  } else {
    cat("  ⚠ Phase 6 filter audit not found at:", phase6_filter_audit_output, "\n")
  }
} else {
  # Read Phase 5 combined master
  if (!file.exists(phase5_output)) stop("Phase 5 output not found: ", phase5_output)
  cat("Reading Phase 5 master from:", phase5_output, "\n")
  phase6_df <- read_csv(phase5_output, show_col_types = FALSE)

  # Normalize column name typos: pacakge_id -> package_id
  if ("pacakge_id" %in% names(phase6_df) && !"package_id" %in% names(phase6_df)) {
    phase6_df <- phase6_df %>% rename(package_id = pacakge_id)
  }

  # Ensure package_id exists
  if (!"package_id" %in% names(phase6_df)) phase6_df$package_id <- NA_character_

  # Attempt extraction of package_id from package_name when missing
  if ("package_name" %in% names(phase6_df)) {
    pk <- as.character(phase6_df$package_name)
    # primary pattern: 7 alnum after first '|' and before next '_'
    extracted <- stringr::str_match(pk, "\\|[^_]*?([A-Za-z0-9]{7})_")[,2]
    # fallback: 7 alnum directly after first |
    missing_idx <- which(is.na(extracted) | extracted == "")
    if (length(missing_idx) > 0) {
      extracted2 <- stringr::str_match(pk[missing_idx], "\\|\\s*([A-Za-z0-9]{7})")[,2]
      extracted[missing_idx] <- extracted2
    }
    fill_idx <- which(is.na(phase6_df$package_id) | phase6_df$package_id == "")
    if (length(fill_idx) > 0) phase6_df$package_id[fill_idx] <- extracted[fill_idx]
  }

  # Date parsing helper using lubridate for robustness
  parse_any_date <- function(x) {
    x_chr <- as.character(x)
    # try multiple orders
    dt <- suppressWarnings(lubridate::parse_date_time(x_chr, orders = c("Y-m-d", "m/d/Y", "d/m/Y", "Ymd", "Y-m", "b Y", "B Y"), tz = "UTC"))
    as.Date(dt)
  }

  # Parse possible date-like columns
  if ("start_date" %in% names(phase6_df)) phase6_df$start_date <- parse_any_date(phase6_df$start_date)
  if ("end_date" %in% names(phase6_df)) phase6_df$end_date <- parse_any_date(phase6_df$end_date)
  if ("prisma_start_date" %in% names(phase6_df)) phase6_df$prisma_start_date <- parse_any_date(phase6_df$prisma_start_date)
  if ("prisma_end_date" %in% names(phase6_df)) phase6_df$prisma_end_date <- parse_any_date(phase6_df$prisma_end_date)
  if ("week" %in% names(phase6_df)) phase6_df$week <- parse_any_date(phase6_df$week)
  if ("date" %in% names(phase6_df)) phase6_df$date <- parse_any_date(phase6_df$date)
  if ("month" %in% names(phase6_df)) {
    phase6_df$month <- parse_any_date(phase6_df$month)
    # Snap to the 1st of the month regardless of input day (e.g., Oct 15 -> Oct 1)
    phase6_df$month <- lubridate::floor_date(phase6_df$month, "month")
  }

  # Compute week_end and month_end
  week_end <- if ("week" %in% names(phase6_df)) phase6_df$week + 6 else as.Date(NA)
  month_end <- if ("month" %in% names(phase6_df)) lubridate::ceiling_date(phase6_df$month, "month") - lubridate::days(1) else as.Date(NA)

  # Compute final start/end dates with precedence rules
  # Prisma dates are planning-level (package-wide) — only use as last resort
  # when no granular date column (start_date, week, month, date) is available.

  # Helper: safely get a date column or NA vector of the right length
  safe_date_col <- function(df, col) {
    if (col %in% names(df)) df[[col]] else rep(as.Date(NA), nrow(df))
  }

  # --- start_date_final precedence ---
  # start_date > week > month > date > end_date > prisma_start_date > prisma_end_date
  start_candidates <- list(
    list(col = safe_date_col(phase6_df, "start_date"),        label = "start_date"),
    list(col = safe_date_col(phase6_df, "week"),              label = "week"),
    list(col = safe_date_col(phase6_df, "month"),             label = "month"),
    list(col = safe_date_col(phase6_df, "date"),              label = "date"),
    list(col = safe_date_col(phase6_df, "end_date"),          label = "end_date"),
    list(col = safe_date_col(phase6_df, "prisma_start_date"), label = "prisma_start_date"),
    list(col = safe_date_col(phase6_df, "prisma_end_date"),   label = "prisma_end_date")
  )

  # Build result + source label via first-non-NA walk
  n_rows <- nrow(phase6_df)
  sdf_val    <- rep(as.Date(NA), n_rows)
  sdf_source <- rep(NA_character_, n_rows)
  for (cand in start_candidates) {
    fill <- which(is.na(sdf_val) & !is.na(cand$col))
    if (length(fill) > 0) {
      sdf_val[fill]    <- cand$col[fill]
      sdf_source[fill] <- cand$label
    }
  }
  phase6_df$start_date_final  <- as.Date(sdf_val, origin = "1970-01-01")
  phase6_df$start_date_source <- sdf_source

  # --- end_date_final precedence ---
  # end_date > week_end > month_end > start_date > date > prisma_end_date > prisma_start_date
  end_candidates <- list(
    list(col = safe_date_col(phase6_df, "end_date"),          label = "end_date"),
    list(col = week_end,                                      label = "week_end"),
    list(col = month_end,                                     label = "month_end"),
    list(col = safe_date_col(phase6_df, "start_date"),        label = "start_date"),
    list(col = safe_date_col(phase6_df, "date"),              label = "date"),
    list(col = safe_date_col(phase6_df, "prisma_end_date"),   label = "prisma_end_date"),
    list(col = safe_date_col(phase6_df, "prisma_start_date"), label = "prisma_start_date")
  )

  edf_val    <- rep(as.Date(NA), n_rows)
  edf_source <- rep(NA_character_, n_rows)
  for (cand in end_candidates) {
    fill <- which(is.na(edf_val) & !is.na(cand$col))
    if (length(fill) > 0) {
      edf_val[fill]    <- cand$col[fill]
      edf_source[fill] <- cand$label
    }
  }
  phase6_df$end_date_final  <- as.Date(edf_val, origin = "1970-01-01")
  phase6_df$end_date_source <- edf_source

  cat("  Date source distribution (start_date_final):\n")
  print(table(phase6_df$start_date_source, useNA = "ifany"))
  cat("  Date source distribution (end_date_final):\n")
  print(table(phase6_df$end_date_source, useNA = "ifany"))

  # Track removals by stable row id for filter-level audit reporting
  phase6_df <- phase6_df %>% mutate(.phase6_row_id = row_number())
  validate_kpis_for_audit <- setdiff(known_kpi_metrics, c("benchmark", "benchmark_metric"))
  audit_metric_cols <- intersect(unique(c(validate_kpis_for_audit, "spend", "impressions", "clicks")), names(phase6_df))
  filter_audit_entries <- list()

  summarize_removed_rows <- function(before_df, after_df, reason_label, metric_cols) {
    removed_ids <- setdiff(before_df$.phase6_row_id, after_df$.phase6_row_id)
    if (length(removed_ids) == 0) return(data.frame())

    removed_df <- before_df %>% filter(.phase6_row_id %in% removed_ids)
    if (!"source_file" %in% names(removed_df)) removed_df$source_file <- NA_character_
    removed_df <- removed_df %>% mutate(source_file = as.character(source_file))

    if (length(metric_cols) > 0) {
      removed_df %>%
        group_by(source_file) %>%
        summarise(
          filter_reason = reason_label,
          removed_rows = n(),
          across(all_of(metric_cols), ~ sum(as.numeric(.x), na.rm = TRUE), .names = "removed_{.col}"),
          .groups = "drop"
        )
    } else {
      removed_df %>%
        group_by(source_file) %>%
        summarise(
          filter_reason = reason_label,
          removed_rows = n(),
          .groups = "drop"
        )
    }
  }

  # Filter 1: Remove rows where source_file contains "archive" (case-insensitive)
  if ("source_file" %in% names(phase6_df)) {
    before_filter <- phase6_df
    rows_before <- nrow(phase6_df)
    phase6_df <- phase6_df %>%
      filter(!str_detect(source_file, "(?i)archive"))
    filter_audit_entries[[length(filter_audit_entries) + 1]] <-
      summarize_removed_rows(before_filter, phase6_df, "archive_source_file", audit_metric_cols)
    cat("  ✓ Removed", rows_before - nrow(phase6_df), "rows with 'archive' in source_file\n")
  }

  # Filter 2: Remove rows where sum of all numeric columns is NA, 0, or blank
  numeric_cols <- setdiff(names(phase6_df)[sapply(phase6_df, is.numeric)], ".phase6_row_id")
  if (length(numeric_cols) > 0) {
    before_filter <- phase6_df
    rows_before <- nrow(phase6_df)
    phase6_df <- phase6_df %>%
      mutate(
        row_metric_sum = rowSums(across(all_of(numeric_cols), ~ coalesce(as.numeric(.), 0)), na.rm = TRUE)
      ) %>%
      filter(!(is.na(row_metric_sum) | row_metric_sum == 0)) %>%
      select(-row_metric_sum)
    filter_audit_entries[[length(filter_audit_entries) + 1]] <-
      summarize_removed_rows(before_filter, phase6_df, "numeric_row_metric_sum_zero_or_na", audit_metric_cols)
    cat("  ✓ Removed", rows_before - nrow(phase6_df), "rows where sum of numeric columns is NA or 0\n")
  }

  # Filter 3: Remove rows where package_name is NA
  if ("package_name" %in% names(phase6_df)) {
    before_filter <- phase6_df
    rows_before <- nrow(phase6_df)
    phase6_df <- phase6_df %>%
      filter(!is.na(package_name) & package_name != "")
    filter_audit_entries[[length(filter_audit_entries) + 1]] <-
      summarize_removed_rows(before_filter, phase6_df, "missing_package_name", audit_metric_cols)
    cat("  ✓ Removed", rows_before - nrow(phase6_df), "rows where package_name is NA or blank\n")
  }

  # Build and write filter-audit checkpoint
  phase6_filter_audit <- bind_rows(filter_audit_entries)
  if (nrow(phase6_filter_audit) > 0) {
    phase6_filter_audit <- phase6_filter_audit %>%
      mutate(source_file = as.character(source_file)) %>%
      arrange(source_file, filter_reason)
  } else {
    phase6_filter_audit <- data.frame(
      source_file = character(0),
      filter_reason = character(0),
      removed_rows = integer(0),
      stringsAsFactors = FALSE
    )
  }

  write_csv(phase6_filter_audit, phase6_filter_audit_output)
  cat("✓ Phase 6 filter audit saved to:", phase6_filter_audit_output, "\n")

  # Remove internal tracking id before writing Phase 6 output
  phase6_df <- phase6_df %>% select(-any_of(".phase6_row_id"))

  # Write Phase 6 checkpoint
  write_csv(phase6_df, phase6_output)
  cat("✓ Phase 6 checkpoint saved to:", phase6_output, "\n")
}

cat("Phase 6 complete. Rows:", if (exists('phase6_df')) nrow(phase6_df) else 0, "\n")

################################################################################
#### PHASE 6.1: SHEET-LEVEL SUMMARY TABLE ####
################################################################################
# Goal: Create a summary table with one row per sheet showing:
# - Partner name, sheet name, URL, last modified info
# - Row count, package count, date range
# - Totals for all KPI metrics

cat("\n=== PHASE 6.1: SHEET-LEVEL SUMMARY TABLE ===\n")

phase6_1_output <- file.path(output_dir, "phase6_1_sheet_summary.csv")

if (use_saved_phases && current_phase != 6 && file.exists(phase6_1_output)) {
  cat("Loading saved Phase 6.1 output from:", phase6_1_output, "\n")
  phase6_1_summary <- read_csv(phase6_1_output, show_col_types = FALSE)
  cat("✓ Loaded", nrow(phase6_1_summary), "sheet summaries from saved Phase 6.1 file\n")
} else {
  # Ensure phase6_df is available
  if (!exists('phase6_df') || nrow(phase6_df) == 0) {
    cat("⚠ Phase 6 data not available. Skipping Phase 6.1.\n")
  } else {
    # Helper functions for date aggregation
    safe_min_date <- function(df, cols) {
      cols <- intersect(cols, names(df))
      if (length(cols) == 0) return(as.Date(NA))
      x <- do.call(c, lapply(cols, function(cn) as.Date(df[[cn]])))
      if (all(is.na(x))) as.Date(NA) else min(x, na.rm = TRUE)
    }

    safe_max_date <- function(df, cols) {
      cols <- intersect(cols, names(df))
      if (length(cols) == 0) return(as.Date(NA))
      x <- do.call(c, lapply(cols, function(cn) as.Date(df[[cn]])))
      if (all(is.na(x))) as.Date(NA) else max(x, na.rm = TRUE)
    }

    # Date columns to consider for range
    date_cols_for_range <- c(
      "start_date", "end_date", "prisma_start_date", "prisma_end_date",
      "week", "date", "month", "start_date_final", "end_date_final"
    )

    # Determine package key (prefer package_id, fallback to package_name)
    package_key <- if ("package_id" %in% names(phase6_df)) "package_id"
                   else if ("package_name" %in% names(phase6_df)) "package_name"
                   else NA_character_

    # Get all numeric columns for metrics
    numeric_cols <- names(phase6_df)[sapply(phase6_df, is.numeric)]

    # Build summary by source_file
    phase6_1_summary <- phase6_df %>%
      mutate(
        source_file = as.character(source_file),
        source_url = as.character(source_url),
        partner_name = as.character(site)
      ) %>%
      group_by(partner_name, source_file, source_url, last_modified_time, last_modified_by) %>%
      summarise(
        row_count = n(),
        package_count = if (!is.na(package_key)) n_distinct(.data[[package_key]], na.rm = TRUE) else NA_integer_,
        date_range_start = safe_min_date(dplyr::cur_data(), date_cols_for_range),
        date_range_end = safe_max_date(dplyr::cur_data(), date_cols_for_range),
        # Date source diagnostics: which column drives start/end dates for this sheet
        start_date_sources = paste(sort(unique(start_date_source[!is.na(start_date_source)])), collapse = ", "),
        end_date_sources   = paste(sort(unique(end_date_source[!is.na(end_date_source)])),     collapse = ", "),
        # If week is a source, show the distinct week start dates used
        week_start_dates_used = if (any(start_date_source == "week", na.rm = TRUE))
          paste(sort(unique(as.character(start_date_final[start_date_source == "week" & !is.na(start_date_source)]))), collapse = ", ")
        else NA_character_,
        # If week is a source, show the day-of-week range (e.g., "Sun-Sat", "Mon-Sun")
        week_day_range = if (any(start_date_source == "week", na.rm = TRUE)) {
          wk_dates <- start_date_final[start_date_source == "week" & !is.na(start_date_source)]
          start_dow <- unique(lubridate::wday(wk_dates, label = TRUE, abbr = TRUE))
          end_dow   <- unique(lubridate::wday(wk_dates + 6, label = TRUE, abbr = TRUE))
          paste(paste(start_dow, collapse = "/"), paste(end_dow, collapse = "/"), sep = "-")
        } else NA_character_,
        # Add totals for all KPI metrics
        across(
          all_of(intersect(known_kpi_metrics, numeric_cols)),
          ~ sum(as.numeric(.x), na.rm = TRUE),
          .names = "{.col}_sum"
        ),
        .groups = "drop"
      ) %>%
      arrange(partner_name, source_file)

    # Write Phase 6.1 checkpoint
    write_csv(phase6_1_summary, phase6_1_output)
    cat("✓ Phase 6.1 checkpoint saved to:", phase6_1_output, "\n")
    cat("✓ Summary created for", nrow(phase6_1_summary), "sheets\n")
  }
}

cat("Phase 6.1 complete.\n")

################################################################################
#### PHASE 7: EXPAND RANGED DATA TO DAILY ROWS ####
################################################################################
# Goal: For rows with a date range (start_date_final -> end_date_final), expand into
# daily rows. Numeric metric columns are divided evenly across days. Add `date_final`
# which records the original end date for each expanded row.

cat("\n=== PHASE 7: EXPAND RANGED DATA TO DAILY ROWS ===\n")

phase7_output <- file.path(output_dir, "phase7_daily_master_data.csv")

if (use_saved_phases && current_phase != 7 && file.exists(phase7_output)) {
  cat("Loading saved Phase 7 output from:", phase7_output, "\n")
  phase7_df <- read_csv(phase7_output, show_col_types = FALSE)
  cat("✓ Loaded", nrow(phase7_df), "rows from saved Phase 7 file\n")
} else {
  # Read Phase 6 cleaned master
  if (!file.exists(phase6_output)) stop("Phase 6 output not found: ", phase6_output)
  cat("Reading Phase 6 master from:", phase6_output, "\n")
  base_df <- read_csv(phase6_output, show_col_types = FALSE)

  # Ensure date columns are Date class
  date_cols <- c("start_date_final", "end_date_final")
  for (dc in date_cols) if (dc %in% names(base_df)) base_df[[dc]] <- as.Date(base_df[[dc]])

  # Determine metric columns to split using configured KPI list only
  metric_cols <- intersect(known_kpi_metrics, names(base_df))

  cat("Detected metric columns to split (known KPI list):", paste(metric_cols, collapse=", "), "\n")

  # Expand rows
  expanded_rows <- list()
  er <- 1

  for (r in seq_len(nrow(base_df))) {
    row <- base_df[r, , drop = FALSE]
    sdate <- row$start_date_final
    edate <- row$end_date_final

    if (is.na(sdate) || is.na(edate) || edate < sdate) {
      # no valid range -> keep as-is. Prefer an existing raw `date` column if present,
      # otherwise fall back to end_date_final or start_date_final.
      if ("date" %in% names(row) && !is.na(row$date)) {
        row$date_final <- as.Date(row$date)
      } else if (!is.na(row$end_date_final)) {
        row$date_final <- as.Date(row$end_date_final)
      } else {
        row$date_final <- as.Date(row$start_date_final)
      }
      expanded_rows[[er]] <- row
      er <- er + 1
      next
    }

    ndays <- as.integer(edate - sdate) + 1
    if (ndays <= 1) {
      # single-day range: prefer raw `date` column if present, else use end_date_final
      if ("date" %in% names(row) && !is.na(row$date)) {
        row$date_final <- as.Date(row$date)
      } else {
        row$date_final <- as.Date(edate)
      }
      expanded_rows[[er]] <- row
      er <- er + 1
      next
    }

    # For metric columns, divide evenly across ndays
    metrics_to_split <- intersect(metric_cols, names(row))
    if (length(metrics_to_split) == 0) metrics_to_split <- character(0)

    # Create daily rows
    for (d in 0:(ndays-1)) {
      newrow <- row
      this_date <- sdate + d
      # assign per-row exploded date into `date_final` (user preference)
      newrow$date_final <- this_date
      # divide metrics
      if (length(metrics_to_split) > 0) {
        for (mc in metrics_to_split) {
          val <- as.numeric(row[[mc]])
          newrow[[mc]] <- ifelse(is.na(val), NA_real_, val / ndays)
        }
      }
      expanded_rows[[er]] <- newrow
      er <- er + 1
    }
  }

  # Bind expanded rows
  phase7_df <- bind_rows(expanded_rows) %>% mutate(data_update_datetime = Sys.time())

  # --- Fill missing week values using per-client config ---
  # For rows where `week` is NA but `date_final` exists, compute the week start
  # using the client's configured week_start day (defaults to Sunday).
  if (!"week" %in% names(phase7_df)) phase7_df$week <- as.Date(NA)

  missing_week_idx <- which(is.na(phase7_df$week) & !is.na(phase7_df$date_final))
  if (length(missing_week_idx) > 0) {
    # Build lookup: lowercase client name -> week_start number
    config_lookup <- setNames(
      day_to_wstart[client_week_config$week_start],
      tolower(client_week_config$client)
    )
    default_ws_num <- day_to_wstart[[default_week_start]]

    # Get each row's client (fall back to default if client column missing or unrecognized)
    if ("client" %in% names(phase7_df)) {
      row_clients <- tolower(as.character(phase7_df$client[missing_week_idx]))
      row_ws_nums <- ifelse(
        row_clients %in% names(config_lookup),
        config_lookup[row_clients],
        default_ws_num
      )
    } else {
      row_ws_nums <- rep(default_ws_num, length(missing_week_idx))
    }

    # Vectorize by unique week_start values to avoid per-row loop
    for (ws in unique(row_ws_nums)) {
      ws_rows <- missing_week_idx[row_ws_nums == ws]
      phase7_df$week[ws_rows] <- lubridate::floor_date(
        phase7_df$date_final[ws_rows], "week", week_start = ws
      )
    }

    cat("  ✓ Filled", length(missing_week_idx), "missing week values using per-client config\n")
    # Show summary of which clients were filled
    if ("client" %in% names(phase7_df)) {
      filled_summary <- table(phase7_df$client[missing_week_idx])
      cat("    Rows filled per client:", paste(names(filled_summary), filled_summary, sep = "=", collapse = ", "), "\n")
    }
  } else {
    cat("  No missing week values to fill.\n")
  }

  # Write Phase 7 checkpoint
  write_csv(phase7_df, phase7_output)
  cat("✓ Phase 7 checkpoint saved to:", phase7_output, "\n")
}

# Enforce integer output values for count metrics in final Phase 7 output.
# This keeps downstream BigQuery typing stable for fields like `clicks`.
integer_output_cols <- intersect(c("impressions", "clicks"), names(phase7_df))
if (length(integer_output_cols) > 0) {
  for (col_name in integer_output_cols) {
    col_vals <- suppressWarnings(as.numeric(phase7_df[[col_name]]))
    phase7_df[[col_name]] <- ifelse(is.na(col_vals), NA_real_, round(col_vals, 0))
  }
  write_csv(phase7_df, phase7_output)
  cat("✓ Rounded final output columns to integers:", paste(integer_output_cols, collapse = ", "), "\n")
}

# --- Validation: compare per-sheet metric totals between Phase 5 and Phase 7 ---
# Build full validation table and enrich with Phase 6 filter-reason audit.
cat("\nBuilding KPI validation table between Phase 5 and Phase 7...\n")
if (file.exists(phase5_output)) {
  phase5_df <- read_csv(phase5_output, show_col_types = FALSE)

  # KPI metrics to validate (exclude benchmarks; those are often non-additive / metadata)
  validate_kpis <- setdiff(known_kpi_metrics, c("benchmark", "benchmark_metric"))
  metrics <- intersect(validate_kpis, intersect(names(phase5_df), names(phase7_df)))

  if (length(metrics) == 0) {
    cat("  No KPI metrics detected for validation.\n")
    validation_table <- data.frame(
      source_file = character(0),
      mismatch_any = logical(0),
      filter_reason = character(0),
      stringsAsFactors = FALSE
    )
    validation_diff_cols <- character(0)
    write_csv(validation_table, phase7_validation_output)
    cat("  ✓ Validation table saved to:", phase7_validation_output, "\n")
  } else {
    cat("  KPI metrics validated:", paste(metrics, collapse = ", "), "\n")

    s5 <- phase5_df %>%
      mutate(source_file = as.character(source_file)) %>%
      group_by(source_file) %>%
      summarise(across(all_of(metrics), ~ sum(as.numeric(.x), na.rm = TRUE)), .groups = "drop")

    s7 <- phase7_df %>%
      mutate(source_file = as.character(source_file)) %>%
      group_by(source_file) %>%
      summarise(across(all_of(metrics), ~ sum(as.numeric(.x), na.rm = TRUE)), .groups = "drop")

    comp <- full_join(s5, s7, by = "source_file", suffix = c(".p5", ".p7"))

    # Compute differences for each metric (round before diffing to avoid floating point accumulation noise)
    # Suggested rounding: 2 decimals is typical for currency; for simplicity we use 6 decimals for all.
    round_digits <- 6
    for (m in metrics) {
      c_p5 <- paste0(m, ".p5")
      c_p7 <- paste0(m, ".p7")
      diff_col <- paste0(m, "_diff")
      comp[[c_p5]][is.na(comp[[c_p5]])] <- 0
      comp[[c_p7]][is.na(comp[[c_p7]])] <- 0
      comp[[diff_col]] <- round(comp[[c_p7]], round_digits) - round(comp[[c_p5]], round_digits)
    }

    # Flag mismatches beyond a tiny tolerance
    tol <- 1e-6
    diff_cols <- grep("_diff$", names(comp), value = TRUE)
    comp$mismatch_any <- apply(abs(comp[, diff_cols, drop = FALSE]), 1, function(x) any(x > tol, na.rm = TRUE))

    # Load Phase 6 filter-audit checkpoint if needed and build aggregated reason text
    if ((!exists("phase6_filter_audit") || !is.data.frame(phase6_filter_audit) || nrow(phase6_filter_audit) == 0) &&
        file.exists(phase6_filter_audit_output)) {
      phase6_filter_audit <- read_csv(phase6_filter_audit_output, show_col_types = FALSE)
    }

    reason_summary <- data.frame(
      source_file = character(0),
      filter_reason = character(0),
      stringsAsFactors = FALSE
    )

    if (exists("phase6_filter_audit") && is.data.frame(phase6_filter_audit) && nrow(phase6_filter_audit) > 0) {
      audit_df <- phase6_filter_audit
      if (!"source_file" %in% names(audit_df)) audit_df$source_file <- NA_character_
      if (!"filter_reason" %in% names(audit_df)) audit_df$filter_reason <- NA_character_
      if (!"removed_rows" %in% names(audit_df)) audit_df$removed_rows <- NA_integer_
      removed_metric_cols <- grep("^removed_", names(audit_df), value = TRUE)

      format_reason_detail <- function(df_row) {
        reason_label <- as.character(df_row[["filter_reason"]])
        if (is.na(reason_label) || reason_label == "") reason_label <- "unknown_filter"

        detail_parts <- c()
        rows_removed <- suppressWarnings(as.numeric(df_row[["removed_rows"]]))
        if (!is.na(rows_removed)) detail_parts <- c(detail_parts, paste0("rows=", as.integer(rows_removed)))

        if (length(removed_metric_cols) > 0) {
          for (metric_col in removed_metric_cols) {
            val <- suppressWarnings(as.numeric(df_row[[metric_col]]))
            if (is.na(val) || abs(val) <= 1e-12) next
            metric_name <- sub("^removed_", "", metric_col)
            val_chr <- if (metric_name == "spend") {
              format(round(val, 2), nsmall = 2, trim = TRUE, scientific = FALSE)
            } else {
              format(round(val, 6), trim = TRUE, scientific = FALSE)
            }
            detail_parts <- c(detail_parts, paste0(metric_name, "=", val_chr))
          }
        }

        if (length(detail_parts) == 0) {
          reason_label
        } else {
          paste0(reason_label, "(", paste(detail_parts, collapse = ", "), ")")
        }
      }

      audit_df$reason_detail <- vapply(
        seq_len(nrow(audit_df)),
        function(i) format_reason_detail(audit_df[i, , drop = FALSE]),
        FUN.VALUE = character(1)
      )

      reason_summary <- audit_df %>%
        mutate(source_file = as.character(source_file)) %>%
        arrange(source_file, filter_reason) %>%
        group_by(source_file) %>%
        summarise(
          filter_reason = paste(reason_detail, collapse = "; "),
          .groups = "drop"
        )
    }

    validation_table <- comp %>%
      left_join(reason_summary, by = "source_file") %>%
      mutate(filter_reason = if_else(is.na(filter_reason), "", filter_reason)) %>%
      arrange(source_file)

    validation_diff_cols <- diff_cols
    write_csv(validation_table, phase7_validation_output)
    cat("  ✓ Validation table saved to:", phase7_validation_output, "\n")
  }
} else {
  cat("  ⚠ Phase 5 checkpoint not found; skipping validation table build.\n")
  validation_table <- data.frame(
    source_file = character(0),
    mismatch_any = logical(0),
    filter_reason = character(0),
    stringsAsFactors = FALSE
  )
  validation_diff_cols <- character(0)
  write_csv(validation_table, phase7_validation_output)
  cat("  ✓ Empty validation table saved to:", phase7_validation_output, "\n")
}

cat("Phase 7 complete. Rows:", if (exists('phase7_df')) nrow(phase7_df) else 0, "\n")


#### write to BQ --using write_to_bq  ####

  cat("\n=== Starting BigQuery upload process ===\n")
  # Before uploading: replace NA in numeric columns with 0 to avoid NULLs where zeros are expected
  if (exists('phase7_df')) {
    numeric_cols_bq <- names(phase7_df)[sapply(phase7_df, is.numeric)]
    if (length(numeric_cols_bq) > 0) {
      phase7_df[numeric_cols_bq] <- lapply(phase7_df[numeric_cols_bq], function(x) { x[is.na(x)] <- 0; x })
      # Update the Phase 7 checkpoint so it's consistent with upload-ready data
      write_csv(phase7_df, phase7_output)
      cat("  ✓ Replaced NA with 0 for numeric columns before BQ upload: ", paste(numeric_cols_bq, collapse=", "), "\n")
    }
  }

  # remove previous bq table (with error handling)
  tryCatch({
    bq_table <- bq_table(project = "looker-studio-pro-452620", dataset = "landing", table = "fpd_data_ranged")
    bq_table_delete(bq_table)
    cat("Deleted old BigQuery table\n")
  },
  error = function(e) {
    cat("Note: Could not delete old table (may not exist):", e$message, "\n")
  })

  write_to_bq <- function(data, dataset, table) {
    # Define the BigQuery project and dataset
    project_id <- "looker-studio-pro-452620"
    
    cat("Writing", nrow(data), "rows to BigQuery...\n")
    
    # Wrap in tryCatch to handle errors
    tryCatch({
      # Write the data to BigQuery
      bq_table <- bq_table(project = project_id, dataset = dataset, table = table)
      
      # Use bq_perform_upload to write the data
      bq_table_upload(bq_table, data, write_disposition = "WRITE_TRUNCATE")
      
      cat("✓ Data written to BigQuery table:", table, "\n")
    },
    error = function(e) {
      cat("✗ ERROR writing to BigQuery:", e$message, "\n")
      stop("Failed to write to BigQuery")
    })
  }
  
  write_to_bq(phase7_df, "landing", "fpd_data_ranged")

  cat("\n-----------\n-----------\n First party data pipeline completed at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")

  cat("\n=== FINAL VALIDATION SUMMARY (PHASE 5 vs PHASE 7) ===\n")
  if (exists("validation_table") && is.data.frame(validation_table)) {
    if (nrow(validation_table) == 0) {
      cat("No validation rows available (validation table is empty).\n")
    } else {
      final_mismatches <- validation_table %>% filter(mismatch_any)
      show_cols <- unique(c("source_file", validation_diff_cols, "filter_reason"))
      show_cols <- show_cols[show_cols %in% names(validation_table)]

      if (nrow(final_mismatches) == 0) {
        cat("✓ All per-sheet KPI metric totals match between Phase 5 and Phase 7 (within tolerance).\n")
      } else {
        cat("✗ Detected differences in per-sheet KPI totals for", nrow(final_mismatches), "sheets.\n")
        print(final_mismatches[, show_cols, drop = FALSE])
      }
    }

    cat("Validation table saved to:", phase7_validation_output, "\n")
  } else {
    cat("⚠ Validation table not generated.\n")
  }


# --- OPTIONAL: Run post-processing script ---
# this script layers on manually updated first party data [currently for ADIF] needs to be updated for general use]
# manually update data here: https://docs.google.com/spreadsheets/d/1kUD8gVrHAAaZbULtFgDZl1hgGU-7Ut8fSdNJDgsZwfE/edit?gid=1894007924#gid=1894007924
#source("/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data_loaders/FPD_loader/manually_updated_data_loader.r")
