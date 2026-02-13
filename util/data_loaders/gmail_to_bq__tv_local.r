
# Gmail to Drive TV Local Data Loader Script ###
#
# ---------------------------------------------------------------------------- #
#                                    HEADER                                    #
# ---------------------------------------------------------------------------- #
# clean environment ----
    # rm(list = ls())
# load .rprofile
    # source("/Users/eugenetsenter/.Rprofile")

# load libraries ----
  library("gmailr")
  library(lubridate)
  library(googlesheets4)
  library(tidyverse)
  library(httpuv)
  library(stringr)
  library(xfun)
# ---------------------------------------------------------------------------- #
#                              Configuration                                   #
# ---------------------------------------------------------------------------- #
# ------------------------ search criteria for gmailr ------------------------ #
search_criteria <- 'subject:"TV | Local | daily scheadule" -National'
 
# ------------------------------- BQ table info ------------------------------ #
project_id <- "looker-studio-pro-452620"
dataset <- "landing"
table <- "tv_local_estimates"


# ---------------------------------------------------------------------------- #
#                                   Main Code                                  #
# ---------------------------------------------------------------------------- #

# ------------------------------ pull gmail data ----------------------------- #

  #retrieve threads matching search --------------------- #
  my_threads <- gm_threads(search = search_criteria,
                          num_results = 10)



  # my_threads <- gm_threads(search = 'subject:"TV | Local | daily scheadule" -National',
  #                          num_results = 10)
  2
  my_threads


  # retrieve the latest thread by retrieving the first ID
  latest_thread <- gm_thread(gm_id(my_threads)[[1]])
  # messages in the thread will now be in a list

  # retrieve parts of a specific message
  (my_msg <- latest_thread$messages[[1]])
  (msg_id <- (my_msg[[1]]))

  # retrieve attachments
  (att <- gm_attachments(my_msg) )
  typeof(att)
  arrange(att)
  (att_id <- att[[1,4]])
  (att_name <- att[[1,1]])

  ## 2nd attachment

  # (att_id_2 <- att[[3,4]])
  # (att_name_2 <- att[[3,1]])

  att2 <- gm_attachment(att_id, msg_id)

  #Test Date of Email vs today
  (r_date <- str_sub(gm_date(my_msg), 6, 16))
  (r_date <- as.Date(r_date, format = "%d %b %Y"))

  today()
  #log to console

  cat(paste0("latest data date: ", r_date, " --  matches script run date: ", r_date == today()))



  # ----------------- write the attachement to local temp file ----------------- #
  temp_file <- tempfile(fileext = paste0("_", att_name))
  gm_save_attachment(att2, filename = temp_file)


  library(janitor)

#
# ----------------------------- load & clean data ---------------------------- #
  numeric_cols <- c(
    "net_cost",
    "net_impressions",
    "total_units"
  )

  raw_df <- readr::read_csv(
    temp_file,
    guess_max = 100,
    na = c("", "NA", "N/A", "Invalid Number"),
    #col_types = readr::cols('Weeks in WEEK_BEGIN_DATE' = readr::col_date(), 'Date' = readr::col_date())
    "%m/%d/%y"
    #col_types = readr::cols(.default = readr::col_character())
    ) %>%
    janitor::clean_names()
  str(raw_df)

  raw_df <- readr::read_csv(
    temp_file,
    guess_max = 100,
    na = c("", "NA", "N/A", "Invalid Number"),
    col_types = readr::cols(
      Date = readr::col_date(format = "%m/%d/%y")
    )
  ) %>%
    janitor::clean_names()
  str(raw_df)
  glimpse(raw_df)
  #type_convert(raw_df)
  # list the column names seperated by commas for easier reference
  colnames(raw_df) %>%  
    paste(collapse = ", ")

  if ("month" %in% names(raw_df)) {
    raw_df <- raw_df %>% dplyr::filter(!is.na(month))
  }
  unique(raw_df$date)
  str(raw_df)
  #lubridate::as_date(raw_df$date, format = "%m-%d-%Y")
  # Fix: After janitor::clean_names(), check for the actual column name
  # Check which column name exists before mutate
  # Map old column names to new column names after clean_names()
  net_cost_col <- if("total_net_cost" %in% names(raw_df)) {
    "total_net_cost"
  } else if("total_net" %in% names(raw_df)) {
    "total_net"
  } else if("total_planned_net" %in% names(raw_df)) {
    "total_planned_net"
  } else {
    NULL
  }
  
  # Check which impressions column exists
  impressions_col <- if("total_total_impressions_buyers_estimate" %in% names(raw_df)) {
    "total_total_impressions_buyers_estimate"
  } else if("total_planned_impressions_all_demos" %in% names(raw_df)) {
    "total_planned_impressions_all_demos"
  } else if("total_planned_impressions" %in% names(raw_df)) {
    "total_planned_impressions"
  } else {
    NULL
  }
  
  # Check which units column exists
  units_col <- if("total_units" %in% names(raw_df)) {
    "total_units"
  } else if("total_planned_spots" %in% names(raw_df)) {
    "total_planned_spots"
  } else {
    NULL
  }
  
  # If column not found, print available columns for debugging
  if(is.null(net_cost_col)) {
    cat("Warning: total_net_cost column not found. Available columns:", paste(names(raw_df), collapse = ", "), "\n")
  }
  
  df <- raw_df %>% mutate(
    year = as.integer(year),
    quarter = as.character(quarter),
    month = as.integer(month),
    week = (weeks_in_week_begin_date),
    #date = as.Date(Date),
    advertiser = as.character(advertiser),
    campaign_name = as.character(campaign_name),
    market = as.character(market),
    media_outlet = as.character(media_outlet),
    # Use the correct column name that was found above
    net_cost = if(!is.null(net_cost_col)) {
      .data[[net_cost_col]]
    } else {
      NA_real_
    },
    # Fix: Use the correct impressions column name
    net_impressions = if(!is.null(impressions_col)) {
      .data[[impressions_col]]
    } else {
      NA_real_
    },
    # Fix: Use the correct units column name
    total_units = if(!is.null(units_col)) {
      .data[[units_col]]
    } else {
      NA_real_
    }) %>%
    mutate(
      type = "Local"
      ) %>%
    select(
      year,
      quarter,
      month,
      #week,
      date,
      advertiser,
      type,
      campaign_name,
      program_name,
      market,
      media_outlet,
      net_cost,
      net_impressions,
      total_units
    ) %>%
    # dplyr::mutate(
    #   dplyr::across(
    #     dplyr::all_of(numeric_cols),
    #     ~ readr::parse_number(.x, na = c("", "NA", "N/A", "Invalid Number"))
    #   )
    # )  
    mutate(
      net_impressions = ifelse(is.na(net_impressions), 0, net_impressions * 1000)
    )
#
  # ---------------------------------------------------------------------------- #
  #                              write to big query                              #
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
#
# --------------------------------------END----------------------------------- #
# ---------------------------------------------------------------------------- #