library(bigrquery)
library(dplyr)
library(glue)
library(stringr)
library(tidyr)

# Default project and dataset
bq_project <- "giant-spoon-299605"
bq_dataset <- "tiktok_ads"

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

#' Get a dataframe of all columns in all tables in a dataset
#'
#' @param project BigQuery project ID
#' @param dataset BigQuery dataset name
#' @param table_like Optional pattern to filter tables (default: all tables)
#' @param selected_tables Optional vector of specific table names to include
#' @return A dataframe with columns: table_name, column_name, data_type, ordinal_position
#'
get_dataset_columns <- function(project, dataset, table_like = "%", selected_tables = NULL) {
  # Get connection to use for SQL quoting
  con <- connectToBigQuery(project)
  
  # Build query based on whether specific tables are selected
  if (is.null(selected_tables)) {
    # Use regular glue for the table identifier parts and glue_sql only for the LIKE parameter
    query <- glue("
      SELECT 
        table_name, 
        column_name, 
        data_type,
        ordinal_position
      FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name LIKE ")
    
    # Add the properly quoted table_like parameter
    query <- paste0(query, dbQuoteLiteral(con, table_like))
    
  } else {
    # Use the selected tables instead
    # Create a WHERE clause with IN operator
    tables_list <- paste0(
      "('", 
      paste(selected_tables, collapse = "','"), 
      "')"
    )
    
    query <- glue("
      SELECT 
        table_name, 
        column_name, 
        data_type,
        ordinal_position
      FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name IN {tables_list}
    ")
  }
  
  # Add ordering to make the results more useful
  query <- paste0(query, " ORDER BY table_name, ordinal_position")
  
  # Execute query and download results
  columns_df <- bq_project_query(
    project,
    query
  ) %>% bq_table_download()
  
  # Check if any tables weren't found (if selected_tables was provided)
  if (!is.null(selected_tables)) {
    found_tables <- unique(columns_df$table_name)
    missing_tables <- setdiff(selected_tables, found_tables)
    
    if (length(missing_tables) > 0) {
      warning(glue("The following tables were not found: {paste(missing_tables, collapse = ', ')}"))
    }
  }
  
  return(columns_df)
}

#' Get a summary of column usage across tables
#'
#' @param columns_df A dataframe returned by get_dataset_columns
#' @return A dataframe with columns: column_name, count, tables, data_types
#'
summarize_column_usage <- function(columns_df) {
  columns_df %>%
    group_by(column_name) %>%
    summarize(
      count = n(),
      tables = paste(sort(unique(table_name)), collapse = ", "),
      data_types = paste(sort(unique(data_type)), collapse = ", "),
      .groups = "drop"
    ) %>%
    arrange(desc(count))
}

#' Create a wide-format table showing which columns exist in which tables
#'
#' @param columns_df A dataframe returned by get_dataset_columns
#' @return A dataframe with tables as rows and columns as columns, with TRUE/FALSE values
#'
create_column_presence_matrix <- function(columns_df) {
  # Create a presence matrix
  presence_matrix <- columns_df %>%
    select(table_name, column_name) %>%
    distinct() %>%
    mutate(present = TRUE) %>%
    pivot_wider(
      id_cols = table_name,
      names_from = column_name,
      values_from = present,
      values_fill = FALSE
    )
  
  return(presence_matrix)
}

#' Find common columns across all tables
#'
#' @param columns_df A dataframe returned by get_dataset_columns
#' @return A dataframe with common columns and their data types
#'
find_common_columns <- function(columns_df) {
  # Count unique tables in the input
  n_tables <- length(unique(columns_df$table_name))

  # Find columns that appear in all tables
  common_cols <- columns_df %>%
    group_by(column_name) %>%
    summarize(
      table_count = n_distinct(table_name),
      data_types = paste(sort(unique(data_type)), collapse = ", "),
      .groups = "drop"
    ) %>%
    filter(table_count == n_tables) %>%
    select(column_name, data_types) %>%
    arrange(column_name)

  return(common_cols)
}

#! Example usage !!!!
# Get all columns from all tables in the dataset --
# UNCOMMENT BELOW
  # all_columns <- get_dataset_columns(
  #   project = bq_project,
  #   dataset = bq_dataset
  # )

  # # Print the first few rows
  # head(all_columns)

  # # Get a summary of column usage
  # column_summary <- summarize_column_usage(all_columns)
  # head(column_summary)

  # # Create a presence matrix
  # presence_matrix <- create_column_presence_matrix(all_columns)
  # head(presence_matrix[, 1:10])  # Show first 10 columns only

  # # Find common columns
  # common_columns <- find_common_columns(all_columns)
  # print(common_columns)

  # # Example with specific tables
  # selected_columns <- get_dataset_columns(
  #   project = bq_project,
  #   dataset = bq_dataset,
  #   selected_tables = c("adgroup_history", "campaign_history")
  # )

  # # Print the first few rows of the selected tables
  # head(selected_columns)

#' Get a dataframe of all columns in a single table
#'
#' @param project BigQuery project ID
#' @param dataset BigQuery dataset name
#' @param table Table name
#' @return A dataframe with columns: table_name, column_name, data_type, ordinal_position, project, dataset
#'
get_table_columns <- function(project, dataset, table) {
  # Get connection to use for SQL quoting
  con <- connectToBigQuery(project)
  
  # Build query for a single table
  query <- glue("
    SELECT 
      '{project}' as project,
      '{dataset}' as dataset,
      table_name, 
      column_name, 
      data_type,
      ordinal_position
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = ")
  
  # Add the properly quoted table parameter
  query <- paste0(query, dbQuoteLiteral(con, table))
  
  # Add ordering to make the results more useful
  query <- paste0(query, " ORDER BY ordinal_position")
  
  # Execute query and download results
  columns_df <- bq_project_query(
    project,
    query
  ) %>% bq_table_download()
  
  # Check if the table wasn't found
  if (nrow(columns_df) == 0) {
    warning(glue("Table '{table}' was not found in {project}.{dataset}"))
  }
  
  return(columns_df)
}

#' Compare tables across different datasets
#'
#' @param table1 A list with project, dataset, and table for the first table
#' @param table2 A list with project, dataset, and table for the second table
#' @return A dataframe with columns: table_name, column_name, data_type, ordinal_position, project, dataset
#'
compare_tables_across_datasets <- function(table1, table2) {
  # Get columns for each table
  columns_table1 <- get_table_columns(
    project = table1$project,
    dataset = table1$dataset,
    table = table1$table
  )
  
  columns_table2 <- get_table_columns(
    project = table2$project,
    dataset = table2$dataset,
    table = table2$table
  )
  
  # Combine the results
  combined_columns <- bind_rows(columns_table1, columns_table2)
  
  return(combined_columns)
}

# Example usage for comparing tables across datasets
cross_dataset_comparison <- compare_tables_across_datasets(
  table1 = list(project = bq_project, dataset = "tiktok_ads", table = "campaign_report_lifetime"),
  table2 = list(project = bq_project, dataset = "tiktok_ads_tiktok_ads", table = "tiktok_ads__campaign_report")
)

# Print the first few rows of the comparison
head(cross_dataset_comparison)

# You can use the existing functions with the combined results
# For example, to create a presence matrix showing which columns exist in which tables
comparison_matrix <- create_column_presence_matrix(cross_dataset_comparison)
# convert to long table with table names as columns and column names as rows

comparison_matrix <- comparison_matrix %>%
  pivot_longer(
    cols = -table_name,
    names_to = "column_name",
    values_to = "present"
  ) %>%
  mutate(present = ifelse(present, "Yes", "No")) %>%
  pivot_wider(names_from = table_name, values_from = present) 





head(comparison_matrix)

# Or to find common columns between the two tables
common_columns_across_datasets <- find_common_columns(cross_dataset_comparison)
print(common_columns_across_datasets)
