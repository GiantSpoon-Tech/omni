# !!! update 1/31/25: use Extract_pID_func instead of Extract_pID_func2!!!


Extract_pID_func <- function(df_with_PrismaName) {
  # Find the column name that contains the string "package"
  package_name <- (df_with_PrismaName %>%map_lgl(~ any(grepl("package|standalone", ., ignore.case = TRUE))) %>%
                     which() %>%
                     names()) %>% ifelse(length(.) == 0, stop("No column containing 'package' or 'standalone' found."), .)
  # If there are multiple matches, filter those containing "package"
  if (length(package_name) > 1) {
    package_name <- package_name[grepl("package", package_name, ignore.case = TRUE)]
  }
  
  len <- str_count(df_with_PrismaName[[package_name]][1],"_")
  # Step 3a: Split the column into multiple parts and ensure new columns start with "X"
  split_cols <- as.data.frame(str_split_fixed(df_with_PrismaName[[package_name]], "_", len))
  #colnames(split_cols) <- paste0("X", seq_len(ncol(split_cols)))
  
  # Step 3b: Bind split columns back to the original dataframe
  df1 <- bind_cols(df_with_PrismaName, split_cols)
  # Modify only columns that end in a number
  colnames(df1) <- ifelse(grepl("\\d$", colnames(df1)), 
                          sub("^.", "X", colnames(df1)), 
                          colnames(df1))
  
  print(colnames(df1))
  
  # Create Final Data Frame
  return(df_with_prisma_id2 <- df1 %>% mutate(
    PackageID = str_split_fixed(X11, "\\|", 2)[,2],
    CPM = is.numeric(ifelse(
      grepl("_", str_extract(df1[[package_name]], "\\d+\\.?\\d*(?=\\D*$)")), 
      sub(".*_(\\d+\\.?\\d*)$", "\\1", df1), 
      str_extract(df1[[package_name]], "\\d+\\.?\\d*(?=\\D*$)"))
    ),
    cost_type = sub("^.*_([^_]+)_[^_]*$", "\\1", df1[[package_name]]),
    campaign_friendly = str_replace(X4, "MassMutual", ""),
    package_friendly = paste(campaign_friendly, " | ", X7, " | ", X12, " | ", PackageID, sep = "")
  )
  )
  #(df_with_prisma_id)
} #works better
#Metadata_func(MMM_6_Output_raw)
#' Extract_pID_func
#' A function to process a data frame with Prisma package names and extract relevant components.
#'
#' @param df_with_PrismaName A data frame containing a column with package-related strings.
#' @return A data frame with additional columns: PackageID, CPM, cost_type, campaign_friendly, and package_friendly.
#' @import dplyr, stringr
#'
Extract_pID_func2 <- function(df_with_PrismaName) {
  
  # Step 1: Identify the column containing "package" or "standalone"
  package_name <- df_with_PrismaName %>%
    select_if(~ any(grepl("package|standalone", ., ignore.case = TRUE))) %>%
    names() 
  # If there are multiple matches, filter those containing "package"
  if (length(package_name) > 1) {
    package_name <- package_name[grepl("package", package_name, ignore.case = TRUE)]
  }
  
  # Error handling: Check if a package-related column was found
  if (length(package_name) == 0) {
    stop("No column containing 'package' or 'standalone' found.")
  }
  
  # Step 2: Calculate the maximum number of underscores in the first row
  len <- max(str_count(df_with_PrismaName[[package_name]], "_"))
  
  # Step 3a: Split the column into multiple parts and ensure new columns start with "X"
  split_cols <- as.data.frame(str_split_fixed(df_with_PrismaName[[package_name]], "_", len))
  colnames(split_cols) <- paste0("X", seq_len(ncol(split_cols)))
  
  # Step 3b: Bind split columns back to the original dataframe
  df1 <- bind_cols(df_with_PrismaName, split_cols)
  
  # Step 4: Create the final data frame by adding new columns
  df_with_prisma_id <- df1 %>%
    mutate(
      PackageID = str_extract(X11, "(?<=\\|).+"),
      CPM = is.numeric(ifelse(
        grepl("_", str_extract(df1[[package_name]], "\\d+\\.?\\d*(?=\\D*$)")), 
        sub(".*_(\\d+\\.?\\d*)$", "\\1", df1), 
        str_extract(df1[[package_name]], "\\d+\\.?\\d*(?=\\D*$)"))
      ),
      cost_type = str_extract(df1[[package_name]], "(?<=_)[^_]+(?=_[^_]*$)"),
      campaign_friendly = str_remove(X4, "MassMutual"),
      package_friendly = str_c(campaign_friendly, " | ", X7, " | ", X12, " | ", PackageID)
    )
  
  # Return the final data frame
  return(df_with_prisma_id)
} #WIP
#Extract_pID_func(MMM_6_Output_raw)
  