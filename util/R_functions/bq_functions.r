# source required helper functions into an variable
#source("/Users/eugenetsenter/.Rprofile")

sys.source("/Users/eugenetsenter/Docs/R_Studio_Projects/Func_DayToWeek.R", envir = utils, keep.source = FALSE)
sys.source("/Users/eugenetsenter/Docs/R_Studio_Projects/Extract_pID_func.R", envir = utils, keep.source = FALSE)

# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #
#                              write to big query  v1                          #
  # #' Upload (snapshot or append) to DF to BigQuery and return a refresh summary
  #     #' and return a refresh summary
  #     #' Adds `data_refresh_date = today()`, uploads to
  #     #' `looker-studio-pro-452620.<dataset>.<table>`, then returns a tibble
  #     #' of recent refresh dates with row counts (last 5).
  #     #'
  #     #' Use when:
  #     #' * `mode = "truncate"` to publish a fresh, complete snapshot (overwrite).
  #     #' * `mode = "append"` to add rows to an existing table (schema-compatible).
  #     #'
  #     #' @param data    data.frame/tibble to upload.
  #     #' @param dataset BigQuery dataset name.
  #     #' @param table   BigQuery table name.
  #     #' @param mode    One of `c("truncate","append")`; controls write behavior.
  #     #'
  #     #' @return A tibble with columns `data_refresh_date` and `row_count` (last 5).
  #     #' @examples
  #     #' \dontrun{
  #     #' # Overwrite table with a full snapshot
  #     #' f_write_to_bq(df, "landing", "tv_national_estimates", mode = "truncate")
  #     #'
  #     #' # Append new rows
  #     #' f_write_to_bq(df, "landing", "tv_national_estimates", mode = "append")
  #     #' }
  # f_write_to_bq <- function(data, dataset, table, mode = c("truncate", "append")) {
  #   library(bigrquery)

  #   mode <- match.arg(mode)
  #   write_disposition <- if (mode == "truncate") "WRITE_TRUNCATE" else "WRITE_APPEND"

  #   # add refresh stamp without requiring library() calls for dplyr/lubridate
  #   data <- dplyr::mutate(data, data_refresh_date = lubridate::today())

  #   project_id <- "looker-studio-pro-452620"
  #   bq_tbl <- bigrquery::bq_table(project = project_id, dataset = dataset, table = table)

  #   bigrquery::bq_table_upload(
  #     bq_tbl,
  #     values = data,
  #     write_disposition = write_disposition
  #   )

  #   query <- sprintf(
  #     "SELECT data_refresh_date, COUNT(*) AS row_count
  #     FROM `%s.%s.%s`
  #     GROUP BY data_refresh_date
  #     ORDER BY data_refresh_date DESC
  #     LIMIT 5",
  #     project_id, dataset, table
  #   )
  #   q <- bigrquery::bq_project_query(project_id, query)
  #   result <- bigrquery::bq_table_download(q)

  #   cat(
  #     "Wrote", nrow(data), "rows to",
  #     sprintf("%s.%s.%s", project_id, dataset, table),
  #     "with mode", mode, "\n",
  #     "Latest refresh:", as.character(max(result$data_refresh_date)),
  #     "rows:", result$row_count[result$data_refresh_date == max(result$data_refresh_date)], "\n"
  #   )

  #   result
  # }

# ---------------------------------------------------------------------------- #
#                              write to big query  v2                          #
# ---------------------------------------------------------------------------- #
    #### write to BQ with schema error handling & email alerts ####
    script_file <- tryCatch({
        # Try multiple methods to get the script path
        # Method 1: commandArgs (works with Rscript)
        args <- commandArgs(trailingOnly = FALSE)
        file_arg <- grep("^--file=", args, value = TRUE)
        if (length(file_arg) > 0) {
          return(sub("^--file=", "", file_arg))
        }
        
        # Method 2: sys.frames (works when source()'d)
        frames <- sys.frames()
        for (i in seq_along(frames)) {
          if (!is.null(frames[[i]]$ofile)) {
            return(frames[[i]]$ofile)
          }
        }
        
        # Method 3: RStudio API (works in RStudio)
        if (requireNamespace("rstudioapi", quietly = TRUE) && 
            rstudioapi::isAvailable()) {
          context <- rstudioapi::getActiveDocumentContext()
          if (!is.null(context$path) && context$path != "") {
            return(context$path)
          }
        }
        
        # Fallback
        return("unknown_script")
      }, error = function(e) {
        return("unknown_script")
      })
    f_write_to_bq <- function(data) {
      library(bigrquery)
      library(gmailr)
      
      data <- data %>% mutate(data_refresh_date = today())
      
      # Script file name for error reporting
            # Dynamically detect script file path
      
      # Create BigQuery table reference using outer scope variables
      bq_tbl <- bq_table(project = project_id, dataset = dataset, table = table)
      
      # Helper function to send failure email
      send_failure_email <- function(error_type, error_details) {
        tryCatch({
          # Get authenticated user's email
          user_email <- gm_profile()$emailAddress
          
          email_body <- paste0(
            "BigQuery Write Failure Alert\n\n",
            "Timestamp: ", Sys.time(), "\n",
            "Project: ", project_id, "\n",
            "Dataset: ", dataset, "\n",
            "Table: ", table, "\n",
            "Error Type: ", error_type, "\n\n",
            "Error Details:\n", error_details, "\n\n",
            "---\n",
            "This is an automated alert from the TV National data loader script."
          )
          
          email <- gm_mime() %>%
            gm_to(user_email) %>%
            gm_from(user_email) %>%
            gm_subject(sprintf("%s.%s BQ table failed to update from r script", dataset, table)) %>%
            gm_text_body(email_body)
          
          gm_send_message(email)
          cat("📧 Failure notification email sent to:", user_email, "\n")
        }, error = function(email_err) {
          cat("⚠ Failed to send email notification:", conditionMessage(email_err), "\n")
        })
      }

      # Try to write data; if column incompatibility error occurs, delete table and retry
      write_result <- tryCatch(
        {
          # Attempt to upload data
          bq_table_upload(bq_tbl, data, write_disposition = "WRITE_TRUNCATE")
          cat("✓ Data written successfully to BigQuery table:", table, "\n")
          TRUE
        },
        error = function(e) {
          error_msg <- conditionMessage(e)
          
          # Check if error is related to schema/column incompatibility
          is_schema_error <- grepl("schema|column|field|type mismatch|incompatible", 
                                    error_msg, ignore.case = TRUE)
          
          if (is_schema_error) {
            cat("⚠ Schema incompatibility detected. Deleting existing table and retrying...\n")
            cat("Error details:", error_msg, "\n")
            
            # Delete the table
            tryCatch(
              {
                bq_table_delete(bq_tbl)
                cat("✓ Table deleted successfully\n")
              },
              error = function(del_err) {
                cat("Note: Table may not exist or couldn't be deleted:", conditionMessage(del_err), "\n")
              }
            )
            
            # Retry the upload
            tryCatch(
              {
                bq_table_upload(bq_tbl, data, write_disposition = "WRITE_TRUNCATE")
                cat("✓ Data written successfully after table recreation\n")
                TRUE
              },
              error = function(retry_err) {
                retry_msg <- conditionMessage(retry_err)
                cat("✗ Failed to write data after table deletion:", retry_msg, "\n")
                
                # Send failure email - failed even after retry
                send_failure_email(
                  error_type = "Schema Error - Retry Failed",
                  error_details = paste0(
                    "Initial Error:\n", error_msg, "\n\n",
                    "Retry Error:\n", retry_msg
                  )
                )
                
                stop(retry_err)
              }
            )
          } else {
            # If not a schema error, re-throw the original error and send email
            cat("✗ Write failed with non-schema error:", error_msg, "\n")
            
            # Send failure email
            send_failure_email(
              error_type = "Non-Schema Error",
              error_details = error_msg
            )
            
            stop(e)
          }
        }
      )

      # Verify the write by querying the table
      query <- sprintf(
        "SELECT data_refresh_date, COUNT(*) as row_count FROM `%s.%s.%s` GROUP BY data_refresh_date ORDER BY data_refresh_date DESC LIMIT 5",
        project_id,
        dataset,
        table
      )
      result <- bq_project_query(project_id, query) %>% bq_table_download()

      cat("Latest data refresh date:", as.character(max(result$data_refresh_date)), "\n",
          "Total rows:", sum(result$row_count), "\n"
      )
    }
    f_write_to_bq(df)
    #
    #
    #
    #



#
#


# ---------------------------------------------------------------------------- #
#
#' @title f_gmail_csv_to_dataframe
    #' Download and Process Gmail CSV Attachment
    #'
    #' Searches Gmail, downloads the most recent CSV attachment, and returns a cleaned dataframe
    #'
    #' @param search_criteria Character string for Gmail search (e.g., 'subject:"TV | National | daily scheadule"')
    #' @param num_results Integer, number of threads to retrieve (default: 10)  
    #' @param attachment_index Integer, which attachment to download (1-based, default: 1)
    #' @param validate_date Logical, whether to validate email date matches today (default: FALSE)
    #' @param filter_na_month Logical, whether to filter out rows with NA month values (default: TRUE)
    #' @return Dataframe with cleaned column names and auto-detected column types
    #' @export
    #' 
    
f_gmail_csv_to_dataframe <- function(
  search_criteria, 
  num_results = 10, 
  attachment_index = 1, 
  validate_date = FALSE,
  filter_na_month = TRUE
) {
  require("gmailr")
  require("janitor")
  require("readr")
  require("stringr")
  require("lubridate")
  
  # Step 1: Search for threads matching criteria
  my_threads <- gm_threads(search = search_criteria, num_results = num_results)
  
    #   if (length(my_threads$thread_id) == 0) {
    #     stop("No email threads found matching the search criteria")
    #   }
  
  # Step 2: Get the latest thread
  latest_thread <- gm_thread(gm_id(my_threads)[[1]])
  my_msg <- latest_thread$messages[[1]]
  msg_id <- my_msg[[1]]
  
  # Step 3: Optional date validation
   email_date_str <- stringr::str_sub(gm_date(my_msg), 6, 16)
    email_date <- as.Date(email_date_str, format = "%d %b %Y")

  if (validate_date) {
    email_date_str <- stringr::str_sub(gm_date(my_msg), 6, 16)
    email_date <- as.Date(email_date_str, format = "%d %b %Y")
    
    if (email_date != today()) {
      warning(glue::glue("Email date ({email_date}) does not match today. Data may be stale."))
    } else {
      cat(glue::glue("Email date matches today: {email_date}"))
    }
  }
  
  # Step 4: Get attachments
  att <- gm_attachments(my_msg)
  
  if (nrow(att) == 0) {
    stop("No attachments found in the email")
  }
  
  if (attachment_index > nrow(att)) {
    stop(glue::glue("Attachment index {attachment_index} is greater than available attachments ({nrow(att)})"))
  }
  
  # Step 5: Download attachment
  att_id <- att[[attachment_index, 4]]
  att_name <- att[[attachment_index, 1]]
  cat(glue::glue("Downloading attachment: {att_name}"))
  
  att_content <- gm_attachment(att_id, msg_id)
  temp_file <- tempfile(fileext = paste0("_", att_name))
  gm_save_attachment(att_content, filename = temp_file)
  
  # Step 6: Read CSV data with automatic type detection
  cat("Reading CSV file...")
  
  raw_df <- readr::read_csv(
    temp_file,
    guess_max = 10,
    na = c("", "NA", "N/A", "Invalid Number")
  ) %>%
  janitor::clean_names() %>%
   mutate(email_date = email_date)
  
  # Optional filtering
  if (filter_na_month && "month" %in% names(raw_df)) {
    initial_rows <- nrow(raw_df)
    raw_df <- raw_df %>% dplyr::filter(!is.na(month))
    filtered_rows <- initial_rows - nrow(raw_df)
    cat(glue::glue("Filtered out {filtered_rows} rows with NA month values"))
  }
  
  cat(glue::glue("Successfully processed {nrow(raw_df)} rows with {ncol(raw_df)} columns"))
  cat(glue::glue("Column names: {paste(names(raw_df), collapse = ', ')}"))
  
  return(raw_df)
}
