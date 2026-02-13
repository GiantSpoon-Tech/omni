# import necessary libraries
install.packages("googleCloudStorageR")
library(googleCloudStorageR)
library(bigrquery)

# set project and dataset
project <- "looker-studio-pro-452620"
dataset <- "repo_mart"
table <- "mft_clean_view"
full_table_id <- sprintf("%s.%s.%s", project, dataset, table)

# authenticate with Google Cloud
bq_auth()
gcs_auth(
    email = "gene.tsenter@giantspoon.com"
    
)


# query BigQuery table
query <- sprintf("SELECT * FROM `%s`", full_table_id)
df <- bq_project_query(project, query) %>% bq_table_download()

# write to rds
saveRDS(df, "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data/mft_clean_view.rds")
# write dataframe to CSV
write.csv(df, "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data/delivery_mft.csv", row.names = FALSE)

# upload CSV to Google Cloud Storage
gcs_upload("/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/data/delivery_mft.csv", bucket = "mft_from_bq")
