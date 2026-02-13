#### Header ####
  library(googledrive)
  library(dplyr)
  library(stringr)
  library(lubridate)
  library(googlesheets4)
  library(janitor)



#### Configuration ####
  gdrive_folder_id <- "1zXzm13JIdldmO7L1_yM4AqNof4pNX04D"
  pattern <- "Apollo | Partner Data Collection | "
  range <- "A15:p"
  dataset <- "landing"
  table <- "apollo_fpd_data"
#

#### Main Code - 1 - Combine all google sheets into one DF ####
  #go to the folder in Google Drive
    gdrive_folder <- drive_get(as_id(gdrive_folder_id))
    2
  #

  # Find all files in the gDrive folder and subfolders

    gdrive_files <- drive_ls(
      as_id(gdrive_folder_id),
      pattern = pattern,
      recursive = TRUE,
      type = "spreadsheet",
      n_max = Inf
    )
  #

  # Import and clean data from Google Sheets
    # Description:
      # •	Reads all Google Sheets listed in gdrive_files (each with an id and name).
      # •	Adds a source_file column to track origin.
      # •	Skips empty or NA-only sheets.
      # •	Detects and converts date-like columns to proper Date objects.
      # •	Returns a named list of cleaned data frames, one per sheet.
    #

    # Pull in data from each Google Sheet
      # Create a named list of data frames by reading multiple Google Sheets
      gdrive_dfs <- setNames(
      # Iterate over each row in gdrive_files (each row represents a file)
      lapply(seq_len(nrow(gdrive_files)), function(i) {
      # Extract the file name for labeling later
      name <- gdrive_files$name[i]
      url <- paste0("https://docs.google.com/spreadsheets/d/", gdrive_files$id[i])  
      # Read the Google Sheet by its file ID
        # 'range' defines the cell range to read (can be NULL for entire sheet)
        # col_types = NULL lets read_sheet guess data types
        # col_names = TRUE means the first row is used as column names
      df <- read_sheet(
          gdrive_files$id[i],
          range = range,
          col_types = NULL,
          col_names = TRUE
        ) %>%
      # Add a column indicating which file the data came from
      mutate(source_file = name,
             source_url = url
             
             )  
      # Skip completely empty or all-NA sheets
        if (nrow(df) == 0 || all(is.na(df))) {
          return(NULL)
        }
        
      # Identify any columns whose names contain "date" (case-insensitive)
        date_cols <- grep("date", names(df), ignore.case = TRUE, value = TRUE)
        
      # Convert each detected date column from character to Date format
        for (col in date_cols) {
          df[[col]] <- as.Date(df[[col]], format = "%m/%d/%Y")
          # Adjust format string above if your sheets use another date format
        }
      
      # Convert numeric-like columns to numeric
        number_cols <- grep("num|count|total|impressions|clicks|sends|opens|views|pageviews|completed_views", names(df), ignore.case = TRUE, value = TRUE)
        for (col in number_cols) {
          df[[col]] <- as.numeric(df[[col]])
        } 
      # Return the cleaned data frame for this sheet
        return(df)
      }),
      
      # Name each element in the output list with the file name
        gdrive_files$name
    )

    # Clean up and combine data frames
      # See which files are empty
    names(gdrive_dfs)[sapply(gdrive_dfs, function(x) is.null(x) || nrow(x) == 0)]
      # Remove NULL elements (blank sheets)
      gdrive_dfs <- gdrive_dfs[!sapply(gdrive_dfs, is.null)]
      # Combine all data frames into one
      gdrive_data <- bind_rows(gdrive_dfs, .id = "source_file")
      # Remove unwanted columns (e.g., those starting with "...")
      gdrive_data <- gdrive_data %>%
        select(-starts_with("...")) %>%
        clean_names() %>%
        rename(package_name = package_placement_please_use_gs_placement_package_names_only,
              #week = week_first_sunday_of_week
              )
          if (!"week" %in% colnames(gdrive_data)) {
            gdrive_data <- gdrive_data %>%
              mutate(week = as.Date(NA))
          }
      colnames(gdrive_data)
    #
#
#### Main Code - 2 - fix dates ####
str(gdrive_data)
# add start_date, end_date, impressions, clicks, sends, opens, pageviews, views, completed_views columns if missing
  if (!"start_date" %in% colnames(gdrive_data)) {
    gdrive_data <- gdrive_data %>%
      mutate(start_date = as.Date(NA))
  }
  if (!"end_date" %in% colnames(gdrive_data)) {
    gdrive_data <- gdrive_data %>%
      mutate(end_date = as.Date(NA))
  }
  if (!"impressions" %in% colnames(gdrive_data)) {
    gdrive_data <- gdrive_data %>%
      mutate(impressions = 0)
  }
  if (!"clicks" %in% colnames(gdrive_data)) {
    gdrive_data <- gdrive_data %>%
      mutate(clicks = 0)
  }
  if (!"sends" %in% colnames(gdrive_data)) {
    gdrive_data <- gdrive_data %>%
      mutate(sends = 0)
  }
  if (!"opens" %in% colnames(gdrive_data)) {
    gdrive_data <- gdrive_data %>%
      mutate(opens = 0)
  }
  if (!"pageviews" %in% colnames(gdrive_data)) {
    gdrive_data <- gdrive_data %>%
      mutate(pageviews = 0)
  }
  if (!"views" %in% colnames(gdrive_data)) {
    gdrive_data <- gdrive_data %>%
      mutate(views = 0)
  }
  if (!"completed_views" %in% colnames(gdrive_data)) {
    gdrive_data <- gdrive_data %>%
      mutate(completed_views = 0)
}
# remove rows with NA in date columns
gdrive_data_filtered <- gdrive_data %>%
  filter(!(is.na(start_date) & is.na(end_date) & is.na(week)))
# expand date ranges into individual dates

ranged_data <- gdrive_data_filtered %>%
  mutate(
    # First non-NA of the three for the start
    start_date2 = coalesce(.data$start_date, .data$week, .data$date),

    # End date logic differs (week spans 7 days), so use case_when
    end_date2 = case_when(
      !is.na(.data$end_date) ~ .data$end_date,
      !is.na(.data$week)     ~ .data$week + 6,  # 7-day range
      !is.na(.data$date)     ~ .data$date,
      TRUE                   ~ as.Date(NA)
    ),

    # Build daily sequence; if either end is NA, return NA (empty later)
    date_seq = ifelse(
      is.na(start_date2) | is.na(end_date2),
      list(as.Date(character())),
      map2(start_date2, end_date2, ~ seq(.x, .y, by = "day"))
    )
  ) %>%
  unnest(date_seq, keep_empty = FALSE, names_repair = "check_unique") %>%
  group_by(start_date2, end_date2, .add = FALSE) %>%
  mutate(
    days_in_range = as.numeric(first(end_date2 - start_date2 + 1L))
  ) %>%
  ungroup() %>%
  # Spread metrics evenly across days in range
  mutate(across(
    c(spend, impressions, clicks, sends, opens, pageviews, views, completed_views),
    ~ .x / days_in_range
  )) %>%
  # Optional: rename back to original names if you want
  rename(
    #start_date = start_date2,
    #end_date   = end_date2,
    date_final = date_seq
  )

write_csv(gdrive_data, "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/data/adif_fpd_data.csv")
#### write to BQ --using write_to_bq  ####
  write_to_bq <- function(data, dataset, table) {
    library(bigrquery)
    
    # Define the BigQuery project and dataset
    project_id <- "looker-studio-pro-452620"
    
    # Write the data to BigQuery
    bq_table <- bq_table(project = project_id, dataset = dataset, table = table)

    # Use bq_table_upload to write the data
    bq_table_upload(bq_table, data, write_disposition = "WRITE_TRUNCATE")
    
    cat("Data written to BigQuery table:", table, "\n")
  }
  write_to_bq(gdrive_data, dataset, table)
  
  
  

bigrquery::bq_project_query("looker-studio-pro-452620", query)
# Load the bigrquery library
library(bigrquery)

# Set the project ID (matches your project's convention)
project_id <- "looker-studio-pro-452620"

# Define the SQL query as a string in R (remove extra single quotes)
query <- "create or replace table looker-studio-pro-452620.landing.adif_prmsaDCM_bymonth as SELECT t1.*,
  t2.initative,
FROM
  looker-studio-pro-452620.landing.adif_prmsaDCM_bymonth AS t1
left JOIN
   looker-studio-pro-452620.20250327_data_model.prisma_porcessed AS t2
ON
  t1.package_id = t2.package_id"

# Authenticate with BigQuery (this may prompt for OAuth or use existing credentials)
bigrquery::bq_auth()

# Execute the DDL query
job <- bigrquery::bq_perform_query(
  query = query, 
  billing = project_id
)

# Optional: Wait for the job to complete and check status
bigrquery::bq_job_wait(job)

# Print result if needed
cat("Query executed. Job ID:", bq_job_extract(job, "jobReference")$jobId, "\n")
