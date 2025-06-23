library(readxl)
library(dplyr)
library(tidyr)
library(janitor)
library(stringr)

df <- read_excel(
  '/Users/eugenetsenter/Downloads/MASSMUTUAL003CP_Q1_Trafficking Sheet_DSP_CTV, OLV, & Audio ONLY_3.17.25 (2).xlsx',
  sheet = "Q1 Tsheet",
  skip = 6,
) %>%
  clean_names()
test <- db_query_fields(con, "basis_utms_raw_25Q1_v2")
# TODO: upload df to bq table "basis_utms_raw_25Q1_v2"
# Upload raw df to BigQuery table "basis_utms_raw_25Q1_v2"
dbWriteTable(
  con,
  name = "basis_utms_raw_25Q1_v2",
  value = df,
  overwrite = TRUE # set to FALSE and use append=TRUE to append instead
)





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


# Make all creative columns follow creative_<number>_<attribute>
names_test <- str_remove(str_replace(str_replace(names(df),"creative_",""),"number_",""),"\\d*")
names_prefix <- paste0("creative_",str_extract(names(df),"-?\\d*\\.?\\d+"),"__", names_test)
names(df) <- names_prefix


names(df) <- names(df) |>
  # Remove trailing numbers from names: creative_1__name_1 → creative_1__name
  str_replace("(_name|_url|_asset_link|_edo_tag|_disqo_tag|_video_amp_tag|_3p_or_1p_tag)_\\d+$", "\\1") |>
  # Remove all sequences of 2 or more underscores: creative_1__name → creative_1_name
  str_replace_all("_+", "_") |>
  # Fix creative_3__url_3_number → creative_3_url
  str_replace("_url_3_number", "_url")


# 2️⃣  Identify all creative columns ---------------------------------------
cre_cols <- grep("(name|url|asset_link|edo_tag|disqo_tag|video_amp_tag|3p_or_1p_tag)",
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
table <- "basis_utms_pivoted"

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

dbWriteTable(
  con,
  name = "basis_utms_pivoted",
  value = utms_table,
  overwrite = TRUE # set to FALSE and use append=TRUE to append instead
)

# delete basis_utms_pivoted table
  # query <- "DROP TABLE basis_utms_pivoted"
  # dbExecute(con, query)
