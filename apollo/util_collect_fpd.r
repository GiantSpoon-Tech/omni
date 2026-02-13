getwd()
#find all files in google drive folder called "Apollo" and subfolders containing "Apollo | First Party Data"


library(googledrive)
library(dplyr)
library(stringr)
library(lubridate)
library(googlesheets4)

# Define the folder ID for "Apollo"
apollo_folder_id <- "1zXzm13JIdldmO7L1_yM4AqNof4pNX04D"


#go to the folder "Apollo" in Google Drive
apollo_folder <- drive_get(as_id(apollo_folder_id))
2


# Find all files in the "Apollo" folder and subfolders

apollo_files <- drive_ls(
  as_id(apollo_folder_id),
  pattern = "Apollo | Partner Data Collection | ",
  recursive = TRUE,
  type = "spreadsheet",
  n_max = Inf
)


# Read all sheets into individual data frames and store in a list
apollo_dfs <- setNames(
  lapply(seq_len(nrow(apollo_files)), function(i) {
    name <- apollo_files$name[i]
    url <- paste0("https://docs.google.com/spreadsheets/d/", apollo_files$id[i])
    read_sheet(apollo_files$id[i],
               range = "A15:p"
                ) %>%
      mutate(source_file = name,
             source_url = url
      )
  }),
  apollo_files$name  # This sets the names of the list elements
)


apollo_dfs <- setNames(
  lapply(seq_len(nrow(apollo_files)), function(i) {
    name <- apollo_files$name[i]
    url <- paste0("https://docs.google.com/spreadsheets/d/", apollo_files$id[i])

    df <- read_sheet(apollo_files$id[i],
                     range = "A15:p", col_types = "Dcccccccddddddd?"
                ) %>%
      mutate(source_file = name,
             source_url = url
      )

    # Return NULL for empty sheets (will be filtered out)
    if (nrow(df) == 0 || all(is.na(df))) {
      return(NULL)
    }
    
    return(df)
  }),
  apollo_files$name
)


# See which files are empty
names(apollo_dfs)[sapply(apollo_dfs, function(x) nrow(x) == 0)]

# Remove NULL elements (blank sheets)
apollo_dfs <- apollo_dfs[!sapply(apollo_dfs, is.null)]

# Combine all data frames into one
apollo_data <- bind_rows(apollo_dfs, .id = "source_file")


#write to BQ --using write_to_bq 



write_to_bq()

write_to_bq <- function(data, dataset, table) {
  library(bigrquery)
  
  # Define the BigQuery project and dataset
  project_id <- "looker-studio-pro-452620"
  
  # Write the data to BigQuery
  bq_table <- bq_table(project = project_id, dataset = dataset, table = table)
  
  # Use bq_perform_upload to write the data
  bq_perform_upload(bq_table, data, write_disposition = "WRITE_TRUNCATE")
  
  cat("Data written to BigQuery table:", table, "\n")
}
