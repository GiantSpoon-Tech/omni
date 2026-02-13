################################################################################
#### SIMPLIFIED FPD INGESTION - VERSION 2 (LINEAR, NO ABSTRACTIONS)
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
cat ("\n-----------\nADIF first party data pipeline started at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n-----------\n")
#### CONFIGURATION ####
gdrive_folder_id <- "1EyN93JE7v4OXjMMQREVuZ4ZN7xEed5WB"
pattern <- "De Beers | Partner Data"
output_dir <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/apollo/data"
use_saved_phases <- TRUE
# Set the phase you are actively working on (1..7). Saved results will be used for other phases.
current_phase <- 1

# Predefine checkpoint paths so they can be reused when loading saved results
phase1_output <- file.path(output_dir, "phase1_discovered_files.csv")
phase2_output <- file.path(output_dir, "phase2_header_detection.csv")
phase3_output <- file.path(output_dir, "phase3_raw_headers.csv")
phase4_output <- file.path(output_dir, "phase4_normalization_mapping.csv")
phase5_output <- file.path(output_dir, "phase5_combined_master_data.csv")
phase6_output <- file.path(output_dir, "phase6_cleaned_master_data.csv")
phase7_output <- file.path(output_dir, "phase7_daily_master_data.csv")

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

  # Add full URLs and normalize column names
  discovered_files <- discovered_files %>%
    mutate(
      sheet_url = paste0("https://docs.google.com/spreadsheets/d/", id),
      .after = name
    ) %>%
    rename(sheet_id = id, sheet_name = name)

  cat("✓ Found", nrow(discovered_files), "matching sheets\n")

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
    
    cat("\nProcessing file", i, "of", nrow(discovered_files), ":", sheet_name, "\n")
    
    tryCatch({
      # Read columns A:G from 'data' tab (use for scanning header row)
      # Use a large range (A1:G500) to ensure we capture all rows including those further down
      scan_range <- "A1:G500"
      
      cat("  Scanning columns A:G for header row...\n")
      sheet_data_scan <- read_sheet(
        ss = sheet_id,
        sheet = "data",
        range = scan_range,
        col_names = FALSE
      )
      
      if (nrow(sheet_data_scan) == 0) {
        cat("  ✗ ERROR: Sheet is empty\n")
        header_detection_results[[i]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
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

view(phase2_results)

################################################################################
#### PHASE 3: COLUMN HEADER INGESTION & METADATA COLLECTION ####
################################################################################
# Goal: Extract all column headers from the 'data' tab of each file
# Method: Use detected header row from Phase 2, read columns A:S to get full table width
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
    header_row <- successful_files$header_row[i]
    
    cat("\nProcessing file", i, "of", nrow(successful_files), ":", sheet_name, "\n")
    
    tryCatch({
      # Read the data starting from detected header row to column S (full table width)
      # Use this row as header (col_names = TRUE)
      cat("  Reading columns A:S starting from row", header_row, "...\n")
      
      sheet_data <- read_sheet(
        ss = sheet_id,
        sheet = "data",
        range = paste0("A", header_row, ":S"),
        col_names = TRUE
      )
      
      if (nrow(sheet_data) == 0 || ncol(sheet_data) == 0) {
        cat("  ✗ ERROR: No data found after header row\n")
        # Record an empty result row and continue to next file
        header_metadata_results[[header_metadata_index]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
          column_position = NA_integer_,
          column_name_raw = NA_character_,
          stringsAsFactors = FALSE
        )
        header_metadata_index <- header_metadata_index + 1
        next
      }
      
      # Extract column names (as read by googlesheets4)
      col_names <- names(sheet_data)
      
      cat("  ✓ Found", length(col_names), "columns\n")
      
      # Create metadata record for each column
      for (col_idx in seq_along(col_names)) {
        col_name <- col_names[col_idx]
        
        header_metadata_results[[header_metadata_index]] <- data.frame(
          sheet_name = sheet_name,
          sheet_id = sheet_id,
          sheet_url = sheet_url,
          column_position = col_idx,
          column_name_raw = col_name,
          stringsAsFactors = FALSE
        )
        header_metadata_index <- header_metadata_index + 1
      }
      
    }, error = function(e) {
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
print(col_frequency)

view(phase3_results)

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
    } else if (str_detect(rlow, "start[_ ]?date") || str_detect(rlow, "^start")) {
      norm <- "start_date"; rule <- "match_start_date"
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

cat("\nSample mappings (top rows):\n")
view(head(phase4_map, 40))


  cat("\nPhase 4 complete. Next: Phase 5 will ingest data using this mapping.\n")

} # end - compute phase4_map

################################################################################
#### PHASE 5: DATA INGESTION & COMBINATION ####
################################################################################
# Goal: Read each sheet starting at detected header row (A:S), normalize columns using
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

# Metrics to coerce to numeric
metrics_expected <- c("spend", "impressions", "clicks", "sends", "opens", "pageviews", "views", "completed_views")

combined_list <- list()
ci <- 1

# Ensure list of files to ingest is available (from Phase 2 results)
successful_files <- phase2_results %>% filter(status == "success")

for (i in seq_len(nrow(successful_files))) {
  sheet_name <- successful_files$sheet_name[i]
  sheet_id <- successful_files$sheet_id[i]
  sheet_url <- successful_files$sheet_url[i]
  header_row <- successful_files$header_row[i]

  cat("\nIngesting file", i, "of", nrow(successful_files), ":", sheet_name, "\n")

  tryCatch({
    df <- read_sheet(
      ss = sheet_id,
      sheet = "data",
      range = paste0("A", header_row, ":S"),
      col_names = TRUE
    )

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

    # Coerce numeric metric columns: remove $ and , then as.numeric
    numeric_candidates <- intersect(metrics_expected, names(df))
    if (length(numeric_candidates) > 0) {
        df <- df %>%
          mutate(across(all_of(numeric_candidates), ~ as.numeric(str_replace_all(as.character(.x), "[\\$,.]", ""))))
    }

    # Parse date columns if present
    date_candidates <- intersect(c("start_date", "end_date", "week", "date"), names(df))
    for (dc in date_candidates) {
      df[[dc]] <- suppressWarnings(as.Date(as.character(df[[dc]]), tryFormats = c("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y")))
    }

    # Add metadata columns
    df <- df %>% mutate(
      source_file = sheet_name, 
      source_url = sheet_url,
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
  phase5_output <- file.path(output_dir, "phase5_combined_master_data.csv")
  write_csv(master_df, phase5_output)
  cat("\n✓ Phase 5 complete. Combined data written to:", phase5_output, "\n")
  cat("Total rows:", nrow(master_df), "Total cols:", ncol(master_df), "\n")
}

} # end - Phase 5 checkpoint conditional

################################################################################
#### PHASE 6: CLEANING & DATE FINALIZATION ####
################################################################################
# Goal: Post-process the combined master (`phase5_combined_master_data.csv`) to
# 1) extract `package_id` from `package_name` when missing
# 2) standardize date columns (`start_date`, `end_date`, `week`, `date`, `month`) to Date
# 3) compute `start_date_final` and `end_date_final` for every row

cat("\n=== PHASE 6: CLEANING & DATE FINALIZATION ===\n")

phase6_output <- file.path(output_dir, "phase6_cleaned_master_data.csv")

if (use_saved_phases && current_phase != 6 && file.exists(phase6_output)) {
  cat("Loading saved Phase 6 output from:", phase6_output, "\n")
  phase6_df <- read_csv(phase6_output, show_col_types = FALSE)
  cat("✓ Loaded", nrow(phase6_df), "rows from saved Phase 6 file\n")
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
  if ("week" %in% names(phase6_df)) phase6_df$week <- parse_any_date(phase6_df$week)
  if ("date" %in% names(phase6_df)) phase6_df$date <- parse_any_date(phase6_df$date)
  if ("month" %in% names(phase6_df)) phase6_df$month <- parse_any_date(phase6_df$month)

  # Compute week_end and month_end
  week_end <- if ("week" %in% names(phase6_df)) phase6_df$week + 6 else as.Date(NA)
  month_end <- if ("month" %in% names(phase6_df)) lubridate::ceiling_date(phase6_df$month, "month") - lubridate::days(1) else as.Date(NA)

  # Compute final start/end dates with precedence rules
  # start_date_final precedence: start_date, week, month, date, end_date
  phase6_df$start_date_final <- as.Date(dplyr::coalesce(
    if ("start_date" %in% names(phase6_df)) phase6_df$start_date else as.Date(NA),
    if ("week" %in% names(phase6_df)) phase6_df$week else as.Date(NA),
    if ("month" %in% names(phase6_df)) phase6_df$month else as.Date(NA),
    if ("date" %in% names(phase6_df)) phase6_df$date else as.Date(NA),
    if ("end_date" %in% names(phase6_df)) phase6_df$end_date else as.Date(NA)
  ), origin = "1970-01-01")

  # end_date_final precedence: end_date, week_end, month_end, start_date, date
  phase6_df$end_date_final <- as.Date(dplyr::coalesce(
    if ("end_date" %in% names(phase6_df)) phase6_df$end_date else as.Date(NA),
    week_end,
    month_end,
    if ("start_date" %in% names(phase6_df)) phase6_df$start_date else as.Date(NA),
    if ("date" %in% names(phase6_df)) phase6_df$date else as.Date(NA)
  ), origin = "1970-01-01")

  # Filter 1: Remove rows where source_file contains "archive" (case-insensitive)
  if ("source_file" %in% names(phase6_df)) {
    rows_before <- nrow(phase6_df)
    phase6_df <- phase6_df %>%
      filter(!str_detect(source_file, "(?i)archive"))
    cat("  ✓ Removed", rows_before - nrow(phase6_df), "rows with 'archive' in source_file\n")
  }

  # Filter 2: Remove rows where sum of all numeric columns is NA, 0, or blank
  numeric_cols <- names(phase6_df)[sapply(phase6_df, is.numeric)]
  if (length(numeric_cols) > 0) {
    rows_before <- nrow(phase6_df)
    phase6_df <- phase6_df %>%
      mutate(
        row_metric_sum = rowSums(across(all_of(numeric_cols), ~ coalesce(as.numeric(.), 0)), na.rm = TRUE)
      ) %>%
      filter(!(is.na(row_metric_sum) | row_metric_sum == 0)) %>%
      select(-row_metric_sum)
    cat("  ✓ Removed", rows_before - nrow(phase6_df), "rows where sum of numeric columns is NA or 0\n")
  }

  # Write Phase 6 checkpoint
  write_csv(phase6_df, phase6_output)
  cat("✓ Phase 6 checkpoint saved to:", phase6_output, "\n")
}

cat("Phase 6 complete. Rows:", if (exists('phase6_df')) nrow(phase6_df) else 0, "\n")

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

  # Identify numeric metric columns dynamically (exclude metadata/date columns)
  non_metric_candidates <- c("source_file", "source_url", "partner_sheet", "sheet_name", "sheet_id", "campaign", "package_name", "package_id", "start_date", "end_date", "week", "date", "month", "start_date_final", "end_date_final", "date_final")
  metric_cols <- names(base_df)[sapply(base_df, is.numeric) | sapply(base_df, function(x) all(grepl("^\\s*$|^-?\\d+(\\.\\d+)?$", as.character(x)[!is.na(as.character(x))], perl=TRUE)))]
  # Remove obvious non-metrics
  metric_cols <- setdiff(metric_cols, non_metric_candidates)

  cat("Detected metric columns to split:", paste(metric_cols, collapse=", "), "\n")

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
  phase7_df <- bind_rows(expanded_rows)

  # --- Validation: compare per-sheet metric totals between Phase 5 and Phase 7 ---
  cat("\nValidating metric totals per sheet between Phase 5 and Phase 7...\n")
  if (file.exists(phase5_output)) {
    phase5_df <- read_csv(phase5_output, show_col_types = FALSE)

    # Detect numeric metric columns present in both datasets
    num5 <- names(phase5_df)[sapply(phase5_df, is.numeric)]
    num7 <- names(phase7_df)[sapply(phase7_df, is.numeric)]
    # Exclude obvious metadata columns
    non_metric_candidates <- c("start_date_final", "end_date_final")
    metrics <- intersect(setdiff(num5, non_metric_candidates), setdiff(num7, non_metric_candidates))

    if (length(metrics) == 0) {
      cat("  No numeric metrics detected for validation.\n")
    } else {
      cat("  Metrics validated:", paste(metrics, collapse = ", "), "\n")

      s5 <- phase5_df %>%
        mutate(source_file = as.character(source_file)) %>%
        group_by(source_file) %>%
        summarise(across(all_of(metrics), ~ sum(.x, na.rm = TRUE)), .groups = "drop")

      s7 <- phase7_df %>%
        mutate(source_file = as.character(source_file)) %>%
        group_by(source_file) %>%
        summarise(across(all_of(metrics), ~ sum(.x, na.rm = TRUE)), .groups = "drop")

      comp <- full_join(s5, s7, by = "source_file", suffix = c(".p5", ".p7"))

      # Compute differences for each metric
      for (m in metrics) {
        c_p5 <- paste0(m, ".p5")
        c_p7 <- paste0(m, ".p7")
        diff_col <- paste0(m, "_diff")
        comp[[c_p5]][is.na(comp[[c_p5]])] <- 0
        comp[[c_p7]][is.na(comp[[c_p7]])] <- 0
        comp[[diff_col]] <- comp[[c_p7]] - comp[[c_p5]]
      }

      # Flag mismatches beyond a tiny tolerance (to avoid floating point noise)
      tol <- 1e-6
      diff_cols <- grep("_diff$", names(comp), value = TRUE)
      comp$mismatch_any <- apply(abs(comp[, diff_cols, drop = FALSE]), 1, function(x) any(x > tol, na.rm = TRUE))

      mismatches <- comp %>% filter(mismatch_any)
      if (nrow(mismatches) == 0) {
        cat("  ✓ All per-sheet metric totals match between Phase 5 and Phase 7 (within tolerance).\n")
      } else {
        cat("  ✗ Detected differences in per-sheet totals for", nrow(mismatches), "sheets. Sample differences:\n")
        print(head(mismatches[, c("source_file", diff_cols)], 20))
      }
    }
  } else {
    cat("  ⚠ Phase 5 checkpoint not found; skipping validation.\n")
  }


  # Write Phase 7 checkpoint
  write_csv(phase7_df, phase7_output)
  cat("✓ Phase 7 checkpoint saved to:", phase7_output, "\n")
}

cat("Phase 7 complete. Rows:", if (exists('phase7_df')) nrow(phase7_df) else 0, "\n")


#### write to BQ --using write_to_bq  ####

  cat("\n=== Starting BigQuery upload process ===\n")
  
  # remove previous bq table (with error handling)
  tryCatch({
    bq_table <- bq_table(project = "looker-studio-pro-452620", dataset = "landing", table = "adif_fpd_data_ranged")
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
      bq_perform_upload(bq_table, data, write_disposition = "WRITE_TRUNCATE")
      
      cat("✓ Data written to BigQuery table:", table, "\n")
    },
    error = function(e) {
      cat("✗ ERROR writing to BigQuery:", e$message, "\n")
      stop("Failed to write to BigQuery")
    })
  }
  
  write_to_bq(phase7_df, "landing", "adif_fpd_data_ranged")

# use cat to print completion messege including timestamp "ADIF first party data pipeline completed at: <timestamp>"in normal fomat
  cat("\n-----------\n-----------\nADIF first party data pipeline completed at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")


