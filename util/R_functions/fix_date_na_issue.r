# Script to analyze and handle NA values in the Date column

library(dplyr)

# Load the data
mft_data <- readRDS("landing_mft_data.rds")

str(mft_data)
a <- mft_data %>% group_by(Date, DCM_Placement) %>%
  summarise(total_imps = sum(Impressions, na.rm = TRUE)) %>%
  arrange(Date) %>%
  filter(is.na(Date)) %>% view()


# Compute date statistics ignoring NA
min_date   <- min(mft_data$Date, na.rm = TRUE)
max_date   <- max(mft_data$Date, na.rm = TRUE)
na_count   <- sum(is.na(mft_data$Date))
date_range <- range(mft_data$Date, na.rm = TRUE)

cat("Minimum date:", min_date, "\n")
cat("Maximum date:", max_date, "\n")
cat("Number of NA values in Date column:", na_count, "\n")
cat("Date range:", date_range[1], "to", date_range[2], "\n")

# Create a cleaned version without NA dates
mft_data_clean <- mft_data %>%
  filter(!is.na(Date))

cat("Original rows:", nrow(mft_data), "\n")
cat("Rows after removing NA dates:", nrow(mft_data_clean), "\n")
