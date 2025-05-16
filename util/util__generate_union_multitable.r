# nolint start
library(bigrquery)
library(dplyr)
library(glue)
library(stringr)

bq_project  <- "giant-spoon-299605"
bq_dataset  <- "tiktok_ads_tiktok_ads"

tables_filter <- c(  # only these tables
                    #   "campaign_history" 
                    #   , "ad_history"
                    #   , 
                      )  # nolint

connectToBigQuery <- function(project_id = bq_project) {
  # Attempt to connect to the BigQuery project
  con <- NULL
  tryCatch(
    {
      con <- dbConnect(
        bigquery(),
        project = project_id
      )
      message("Connected successfully to project: ", project_id)
    },
    error = function(e) {
      message("Failed to connect to project: ", project_id)
      message("Error: ", e$message)
    }
  )
  return(con)
}
connectToBigQuery()
generate_union_all <- function(project, dataset, table_like = "%", target = NULL, selected_tables = NULL) {
  # Get connection to use for SQL quoting
  con <- connectToBigQuery(project)
  
  # 1. pull metadata
  if (is.null(selected_tables)) {
    # Use regular glue for the table identifier parts and glue_sql only for the LIKE parameter
    query <- glue("
      SELECT table_name, column_name, data_type
      FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name LIKE ")
    
    # Add the properly quoted table_like parameter
    query <- paste0(query, dbQuoteLiteral(con, table_like))
    
    columns <- bq_project_query(
      project,
      query
    ) %>% bq_table_download()
  } else {
    # Use the selected tables instead
    # Create a WHERE clause with IN operator
    tables_list <- paste0(
      "('", 
      paste(selected_tables, collapse = "','"), 
      "')"
    )
    
    query <- glue("
      SELECT table_name, column_name, data_type
      FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name IN {tables_list}
    ")
    
    columns <- bq_project_query(
      project,
      query
    ) %>% bq_table_download()
    
    # Check if any tables weren't found
    found_tables <- unique(columns$table_name)
    missing_tables <- setdiff(selected_tables, found_tables)
    
    if (length(missing_tables) > 0) {
      warning(glue("The following tables were not found: {paste(missing_tables, collapse = ', ')}"))
    }
  }

  # 2. master column list and their types (ordered)
  master_cols_info <- columns %>%
    select(column_name, data_type) %>%
    distinct() %>%
    group_by(column_name) %>%
    # For each column, determine the most appropriate type
    # This is a simplified approach - in a real scenario, you might need more complex type resolution
    summarize(data_type = first(data_type), .groups = "drop") %>%
    arrange(column_name)
  
  master_cols <- master_cols_info$column_name

  # 3. helper that builds a SELECT for one table
  build_select <- function(tbl) {
    present <- columns %>% filter(table_name == tbl) %>% pull(column_name)
    select_list <- sapply(
      1:length(master_cols),
      function(i) {
        c <- master_cols[i]
        type <- master_cols_info$data_type[i]
        
        if (c %in% present) {
          # Column exists in this table, cast it to ensure type consistency
          glue("CAST({c} AS {type}) AS {c}")
        } else {
          # Column doesn't exist, create NULL with the right type
          glue("CAST(NULL AS {type}) AS {c}")
        }
      }
    )
    # Add a source_table column with the table name
    source_table_col <- glue("'{tbl}' AS source_table")
    select_list <- c(select_list, source_table_col)
    
    glue("SELECT {str_flatten(select_list, ', ')} FROM `{project}.{dataset}.{tbl}`")
  }

  # 4. stitch the UNION ALL
  sql <- columns$table_name %>%
    unique() %>%
    sort() %>%
    purrr::map_chr(build_select) %>%
    str_flatten("\nUNION ALL\n")

  # 5. add CREATE TABLE if requested
  if (!is.null(target))
    sql <- glue("CREATE OR REPLACE TABLE {target} AS\n{sql}")

  return(sql)
}

# # Example usage 1 - Union ALL tables
# qry <- generate_union_all(
#   project  = "giant-spoon-299605",
#   dataset  = "tiktok_ads_tiktok_ads",
#   table_like = "%",                                   # all tables
#   target = "giant-spoon-299605.data_model_2025.tiktok_unionall"  # optional
# )
# cat(qry)   # inspect or bq_project_query() it

# Example usage 2 - Union specific tables
qry_selected <- generate_union_all(
  project  = bq_project,
  dataset  = bq_dataset,
  selected_tables = tables_filter,  # only these tables
  #target = "giant-spoon-299605.data_model_2025.tiktok_selected_union"  # optional
)
cat(qry_selected)   # inspect or bq_project_query() it

# Run the query
#a <- tbl(bq_project_query("giant-spoon-299605", qry))
# as_bq_dataset(a)
# nolint end