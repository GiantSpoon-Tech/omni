# Basis Delevery Sheet for Looker Studio
getwd()
#rm(list =ls())
# install.packages("googlesheets4")
library(googlesheets4)
library(googledrive)
library(httr)
# install.packages("janitor")
library(janitor)
library(tidyverse)
# ls("package:janitor")

##CHange time out time for API 
# Still failed even though dataset was writning out completely 
# Not necessary with new chunking approach below
httr::set_config(config(timeout = 6000))


googledrive::drive_auth(email = "gene.tsenter@giantspoon.com")

2
gs4_auth(email = "gene.tsenter@giantspoon.com")
# GOOGLE CLOUD STORAGE FILE UPDATES DAILY @ 2PM
GCS_CSV_URL <- 'https://storage.googleapis.com/mft_from_bq/delivery_mft.csv'
SPREADSHEET_ID <- '1Ijjn9qHgiLf5FQ4sRh0-AjkoA6khLQyyOyuXB2bz-As'
TARGET_SHEET_NAME <- 'digital_raw_2025'

All_Delivery_Final <- 
#pull data from Google Cloud Storage
read_csv(GCS_CSV_URL)


#All_Delivery_Final
names(All_Delivery_Final)



## For first time write out 100 rows and then clear it and keep names (fix variable names when needed)
# range_clear(ss = sheet_url, range = "All_Delivery_Final!A1:Z")
# All_Delivery_Final_bld <- All_Delivery_Final[1:100,]
# write_sheet(All_Delivery_Final_bld , ss = sheet_url, sheet = sheet_name)



# claer everything from A2 to L but to leeave the columns names in place 
range_clear(ss = SPREADSHEET_ID, range = "digital_raw_2025!A2:l")

2 

#sleep for 30 seconds to clear everything 
Sys.sleep(30)


# write in chunks 
# altered seq from 1 ot 2 to keep the names 
write_in_chunks <- function(data, sheet_url, sheet_name, chunk_size = 10000) {
  total_rows <- nrow(data)
  for (start_row in seq(1, total_rows, by = chunk_size)) {
    end_row <- min(start_row + chunk_size - 1, total_rows)
    chunk <- data[start_row:end_row, ]
    base::message("Script: Basis_Delivery_to_Google_Sheet_for_Looker_Studio_Refresh")
    sheet_append(ss = sheet_url, data = chunk, sheet = sheet_name)
  }
}


write_in_chunks(All_Delivery_Final, sheet_url, sheet_name, chunk_size = 10000)

# sum(All_Delivery_Final$Impressions, na.rm = TRUE)
