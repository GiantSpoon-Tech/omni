library(readr)
library(dplyr)
library(bigrquery)

cols_df <- read.csv("/Users/eugenetsenter/Downloads/bq-results-20250814-190323-1755198219370 - bq-results-20250814-190323-1755198219370.csv")
cols_df <- cols_df %>% dplyr::filter(grepl("history_dedupe",table_name))
colnames(cols_df)
a <- unique(cols_df$column_name,cols_df$full_table_name)
#select distinct values for col name and full table name
b <- cols_df %>% select(column_name,full_table_name) %>% distinct()
write_csv(b,"colAndTableNames.csv")
cols_df2 <- cols_df %>% select(unique(cols_df$column_name))
as_tibble_col(unique(cols_df$table_name))


df <- read.csv("/Users/eugenetsenter/Looker_clonedRepo/looker_personal/bquxjob_19364c6_198aebe95ee.csv")
df2 <- df %>% select(column_name) %>% distinct()
write_csv(df2,"colnames.csv")
e