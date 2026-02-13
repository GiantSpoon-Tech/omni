# DayToWeek Function
# This function adds a week identifier column to a dataframe based on specified week start day.
# It creates a comprehensive date reference table with week start/end dates for all possible week start days (Sat-Fri)
# and joins it to the input dataframe to append the requested week's week start date.
#
# Parameters:
#   Dataframe: A dataframe containing at least a 'Date' column of date values
#   wk_st_day: A string specifying the column name from the date reference table to add,
#              e.g. "wk_start_mon" for weeks starting on Monday, "wk_start_sun" for Sunday start, etc.
#
# Returns:
#   The input Dataframe with an additional column named as specified in wk_st_day containing
#   the corresponding week start date for each Date row.
#
# Dependencies:
#   Requires lubridate for date manipulation, dplyr for data joining.

DayToWeek <- function (Dataframe, wk_st_day)
{
    # Load necessary libraries for date manipulation, data processing, and potential Google Sheets integration
    library(googlesheets4)  # For Google Sheets operations
    library(tidyverse)      # Collection of packages for data science (includes dplyr, tidyr, etc.)
    library(httpuv)         # HTTP server for web applications
    library(googledrive)    # Google Drive API integration
    library(stringr)        # String manipulation functions
    library(readxl)         # Read Excel files
    library(lubridate)      # Date/time manipulation functions (essential for week calculations)
    library(dplyr)          # Data manipulation (joins, selects)
    library(zoo)            # Time series and zoo objects
    library(gargle)         # OAuth2 authentication for Google APIs

    # Create a sequence of daily dates from October 1, 2015 to December 31, 2030
    # This provides a comprehensive date reference covering a wide range for data processing
    dt <- seq(as.Date("10/1/2015", "%m/%d/%Y"), as.Date("12/31/2030",
        "%m/%d/%Y"), by = "day")

    # Create a comprehensive date reference dataframe with week calculations for all possible week start days
    # Columns:
    # - dt: the date
    # - dow: abbreviated day of week (Mon, Tue, etc.)
    # - dow_long: full day of week name (Monday, Tuesday, etc.)
    # - wk_end_* : end of week dates for weeks starting on the specified day (* = sat, sun, mon, etc.)
    # - wk_start_* : start of week dates for weeks starting on the specified day (* = sat, sun, mon, etc.)
    # week_count = 7 starts a new week on specified day; ceiling_date gives last day of week, floor_date gives first
    dfdt <- data.frame(dt = dt, dow = lubridate::wday(dt, label = TRUE),
        dow_long = lubridate::wday(dt, label = TRUE, abbr = FALSE), wk_end_sat = lubridate::ceiling_date(dt -
            1, unit = "week", week_start = "Sat"), wk_end_sun = lubridate::ceiling_date(dt -
            1, unit = "week", week_start = "Sun"), wk_end_mon = lubridate::ceiling_date(dt -
            1, unit = "week", week_start = "Mon"), wk_end_tue = lubridate::ceiling_date(dt -
            1, unit = "week", week_start = "Tue"), wk_end_wed = lubridate::ceiling_date(dt -
            1, unit = "week", week_start = "Wed"), wk_end_thu = lubridate::ceiling_date(dt -
            1, unit = "week", week_start = "Thu"), wk_end_fri = lubridate::ceiling_date(dt -
            1, unit = "week", week_start = "Fri"), wk_start_sat = lubridate::floor_date(dt,
            unit = "week", week_start = "Sat"), wk_start_sun = lubridate::floor_date(dt,
            unit = "week", week_start = "Sun"), wk_start_mon = lubridate::floor_date(dt,
            unit = "week", week_start = "Mon"), wk_start_tue = lubridate::floor_date(dt,
            unit = "week", week_start = "Tue"), wk_start_wed = lubridate::floor_date(dt,
            unit = "week", week_start = "Wed"), wk_start_thu = lubridate::floor_date(dt,
            unit = "week", week_start = "Thu"), wk_start_fri = lubridate::floor_date(dt,
            unit = "week", week_start = "Fri"))

    # Perform a left join between the input Dataframe and the date reference table
    # Join on Date column (Dataframe) with dt column (dfdt) to match dates
    # Add the specified wk_st_day column from dfdt to Dataframe, containing the week start date
    # Left join ensures all rows from input Dataframe are preserved, even if date not in reference
    return(Dataframe <- Dataframe %>% left_join(dfdt %>% select(dt,
        wk_st_day), by = c(Date = "dt")))
}
