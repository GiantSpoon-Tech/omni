# install.packages(c("dplyr","tidyr","readr","writexl")) # run once if needed
library(dplyr)
library(tidyr)
library(readr)

infile  <- 
#"/Users/eugenetsenter/Looker_clonedRepo/looker_personal/colAndTableNames.csv"

outfile <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/dim_model/tables_as_columns.csv"

df <- readr::read_csv(infile, show_col_types = FALSE) %>%
  mutate(across(everything(), ~trimws(as.character(.)))) %>%
  distinct(full_table_name, column_name, .keep_all = TRUE)

# group into lists, then unnest_wider after padding
tbl_lists <- df %>%
  group_by(full_table_name) %>%
  summarise(fields = list(column_name), .groups = "drop")

max_len <- max(lengths(tbl_lists$fields))

# build a named list of equal-length vectors
cols <- setNames(
  lapply(tbl_lists$fields, function(v){ length(v) <- max_len; v }),
  tbl_lists$full_table_name
)

wide <- as.data.frame(cols, check.names = FALSE)

# optional Excel
write_csv(wide, outfile)
