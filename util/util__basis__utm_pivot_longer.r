library(readxl)
library(dplyr)
library(tidyr)
library(janitor)
library(stringr)
library("DBI")

# LOAD NEW UTM DATA FROM EXCEL FILE
  # Autodetect how many rows to skip by finding the header row with "Property" in first column
  file_path <- '/Users/eugenetsenter/Docs/Clients/MassMutual_files/MMM 2025/Basis_UTMS/Mass Mutual_Full Funnel Branding FY25_Q2 UTMs.xlsx'
  sheet_name <- "Original Q2 Tsheet + UTMs"

  # Read first 20 rows to find the header row
  temp_df <- read_excel(
    file_path,
    sheet = sheet_name,
    n_max = 20,
    col_names = FALSE
  )

  # Find the row that contains "Property" in the first column
  header_row <- which(temp_df[[1]] == "Property")
  if(length(header_row) == 0) {
    stop("Could not find header row with 'Property' in first column")
  }

  # Calculate skip value (header row position minus 1)
  skip_rows <- header_row[1] - 1

  cat("Found header row at position", header_row[1], "- skipping", skip_rows, "rows\n")

  # Now read the actual data with the correct skip value
  new_df <- read_excel(
    file_path,
    sheet = sheet_name,
    skip = skip_rows,
  ) %>%
    clean_names()

  colnames(new_df)
 new_df <- new_df[-c(1),]

new_df <- new_df %>%
  mutate(across(everything(), as.character)) # Convert all columns to character type

#data.frame(colnames(new_df))
df_cleanup <- data.frame(orig = colnames(new_df)) 
df_cleanup[43,"orig"] <- "asset_link_number_5"
df_cleanup[52,"orig"] <- "asset_link_number_6"
df_cleanup <- df_cleanup %>%
  mutate(
    creative_number_new = paste0("creative",str_extract(orig, "_number_\\d+")),
    creative_fieldname_new = gsub("creative_", "", gsub("_number_\\d+", "", df_cleanup$orig)) # Remove the "creative_number_" part
  )

df_cleanup$cleaned_name <- paste(df_cleanup$creative_number_new,df_cleanup$creative_fieldname_new,sep="_")
df_cleanup$cleaned_name[1:7] <- df_cleanup$orig[1:7] # Keep the first 7 columns unchanged
colnames(new_df) <- df_cleanup$cleaned_name
# LOAD OLD UTM DATA FROM BIGQUERY TABLE

  #try(con <- bq_con)
  con <- dbConnect(
    bigrquery::bigquery(),
    project = "looker-studio-pro-452620",
    dataset = "20250327_data_model",
    billing = "looker-studio-pro-452620"
  )

  # old_df <- dbGetQuery(con, "SELECT * FROM basis_utms_raw_25Q1_v2")
  # # Check if the column names in the dataframe match the BigQuery table schema

  # colnames_df <- as.list(colnames(new_df))

  # # Get table schema using bigrquery
  # colnames_bq <- as.list(bq_table_fields(bq_table("looker-studio-pro-452620", "20250327_data_model", "basis_utms_raw_25Q1_v2")))
  # # Compare the column names
  # if (!all(colnames_df %in% colnames_bq)) {
  #   stop("Column names in the dataframe do not match the BigQuery table schema")
  # }

  # # Compare column names between new_df and old_df
  # col_comparison <- data.frame(
  #   old_df_cols = names(old_df),
  #   new_df_cols = names(new_df),
  #   match = names(old_df) == names(new_df)
  # )

  # # Print the comparison
  # print(col_comparison)

  # # Check if all columns match
  # all_match <- all(names(old_df) == names(new_df))
  # cat("All column names match:", all_match, "\n")

  # # Find columns that don't match
  # if (!all_match) {
  #   mismatched_positions <- which(names(old_df) != names(new_df))
  #   cat("Mismatched columns at positions:", paste(mismatched_positions, collapse = ", "), "\n")
    
  #   # Show the mismatched columns
  #   for (pos in mismatched_positions) {
  #     cat("Position", pos, "- old_df:", names(old_df)[pos], "| new_df:", names(new_df)[pos], "\n")
  #   }
  # }

  # # Alternative: Check for columns present in one but not the other
  # cols_only_in_old <- setdiff(names(old_df), names(new_df))
  # cols_only_in_new <- setdiff(names(new_df), names(old_df))

  # if (length(cols_only_in_old) > 0) {
  #   cat("Columns only in old_df:", paste(cols_only_in_old, collapse = ", "), "\n")
  # }

  # if (length(cols_only_in_new) > 0) {
  #   cat("Columns only in new_df:", paste(cols_only_in_new, collapse = ", "), "\n")
  # }

  # # If you want to make new_df column names match old_df exactly:
  # # names(new_df) <- names(old_df)



# TODO: upload df to bq table "basis_utms_raw_25Q1_v2"
# Upload raw df to BigQuery table "basis_utms_raw_25Q2_v2"
dbWriteTable(
  con,
  name = "basis_utms_raw_25Q2_v2",
  value = new_df,
  overwrite = TRUE # set to FALSE and use append=TRUE to append instead
)



df <- new_df

# bq_table_create(
#   project = "looker-studio-pro-452620",
#   dataset = "20250327_data_model",
#   table = "basis_utms_raw_25Q1_v2",
#   schema = list(
#     list(name = "creative_num", type = "INTEGER"),
#     list(name = "creative_name", type = "STRING"),
#     list(name = "creative_url", type = "STRING"),
#     list(name = "creative_asset_link", type = "STRING"),
#     list(name = "creative_edo_tag", type = "STRING"),
#     list(name = "creative_disqo_tag", type = "STRING"),
#     list(name = "creative_video_amp_tag", type = "STRING"),
#     list(name = "creative_3p_or_1p_tag", type = "STRING")
#   )
# )

names(df)
# Make all creative columns follow creative_<number>_<attribute>
names_test <- str_remove(
  str_replace(
    str_replace(
      names(df),
      "creative_",
      ""
      ),
    "number_",
    ""
    ),
    "\\d*"
  )
unique(names_test)
names_prefix <- paste0("creative_",str_extract(names(df),"-?\\d*\\.?\\d+"),"_", names_test)
names(df) <- names_prefix


names(df) <- names(df) |>
  # Remove trailing numbers from names: creative_1__name_1 → creative_1__name
  str_replace("(_name|_url|_asset_link|_edo_tag|_disqo_tag|_video_amp_tag|_3p_or_1p_tag)_\\d+$", "\\1") |>
  # Remove all sequences of 2 or more underscores: creative_1__name → creative_1_name
  str_replace_all("_+", "_") |>
  # Fix creative_3__url_3_number → creative_3_url
  str_replace("_url_3_number", "_url")


# 2️⃣  Identify all creative columns ---------------------------------------
cre_cols <- grep("(name|url|asset_link|edo_tag|disqo_tag|samba_tag|video_amp_tag|3p_or_1p_tag)",
                 names(df), value = TRUE)

# 3️⃣  Pivot longer ---------------------------------------------------------
creative_long <- df %>%                             # Start a pipeline with the dataframe 'df'
  pivot_longer(                                     # Reshape data from wide to long format
    cols = matches("^creative_\\d+_"),              # Select columns that start with 'creative_' followed by digits and an underscore
    names_to = c("creative_num", ".value"),         # Split column names into 'creative_num' and the rest as new columns
    names_pattern = "^creative_(\\d+)_(.*)$",       # Regex to extract the creative number and attribute from column names
    values_drop_na = TRUE                           # Drop rows where the value is NA
  ) %>%
  mutate(creative_num = as.integer(creative_num))   # Convert 'creative_num' from character to integer

names(creative_long) <-
  str_replace_all(names(creative_long), "creative_NA_", "") # Replace underscores with dots in column names




utms_table <- creative_long %>% select(
  #-contains("rotation")
  2:16
  )

#print as tidy list of columns
cat(paste0("`", colnames(utms_table), "`", collapse = ",\n"))

project <- "looker-studio-pro-452620"
dataset <- "20250327_data_model"
billing <- "looker-studio-pro-452620"
table <- "basis_utms_pivoted_q2"



 # Rename Column 'tag' to 'tag_placement' if it exists
if ("tag" %in% colnames(utms_table)) {
  utms_table <- utms_table %>%
    rename(tag_placement = tag)
}


# standardize values in name column
cr_cleanup <-data.frame(orig = (utms_table$name))  # Check unique values in the 'name' column
cr_cleanup$cleaned_name <- str_to_title(cr_cleanup$orig)

cr_cleanup$cleaned_name <- str_replace_all(cr_cleanup$cleaned_name, "[^A-Za-z0-9]", "") # Remove special characters

utms_table <- utms_table %>%
  mutate(
    name = str_to_title(name), # Convert to title case
    name = str_replace_all(name, "[^A-Za-z0-9]", ""), # Remove special characters
    name = str_replace_all(name, "\\s+", "_") # Replace spaces with underscores
  )

colnames(utms_table)
unique(utms_table$name) # Check unique values in the 'name' column

# TODO: use bigrquery to write to table "basis_utms_pivoted"

# 4️⃣  Write to BigQuery ---------------------------------------
# 4.1  Create a connection to BigQuery using bigrquery
library(bigrquery)

# 4.2  Create a connection to BigQuery using bigrquery
con <- dbConnect(
  bigrquery::bigquery(),
  project = "looker-studio-pro-452620",
  dataset = "20250327_data_model",
  billing = "looker-studio-pro-452620"
)
# 4.3  Write the data to BigQuery

DBI::dbWriteTable(
  con,
  name = "basis_utms_pivoted_q2_v2",
  #name = "basis_utms_pivoted",
  value = utms_table,
  overwrite = TRUE, # set to FALSE and use append=TRUE to append instead
  #append = TRUE # set to TRUE to append instead of overwrite
)

# delete basis_utms_pivoted table
   #query <- "DROP TABLE basis_utms_pivoted_q2"
   #dbExecute(con, query)
