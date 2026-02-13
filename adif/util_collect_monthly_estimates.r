# create a df from https://docs.google.com/spreadsheets/d/1FLn_mge8Tz4wKqEmrIPQG_B2_MecS-9kNiQThQaXe_8/edit?gid=1391620677#gid=1391620677
library(googledrive)
library(dplyr)
library(stringr)
library(lubridate)
library(googlesheets4)
library(janitor)
library(tidyr)
 
sheet_id <- "1FLn_mge8Tz4wKqEmrIPQG_B2_MecS-9kNiQThQaXe_8"


sheet_id <- "1FLn_mge8Tz4wKqEmrIPQG_B2_MecS-9kNiQThQaXe_8"

raw_estimates <- read_sheet(
  sheet_id,
  range = "'Media Plan Monthlies '!B6:P65",
  col_names = c("Channel", "Partner", "Tactics", "Flight", "September_spend", "September_impressions", "October_spend", "October_impressions", "November_spend", "November_impressions", "December_spend", "December_impressions", "Total__est_Spend", "planned_CPM", "total_Est_Impressions")
) %>% clean_names() %>% fill(channel, .direction = "down") %>% filter(!is.na(partner))




clean_estimates <- raw_estimates %>%
    mutate(
        across(matches("spend|impressions|planned_cpm|total_est_impressions|total_est_spend"),~ as.numeric(gsub("[^0-9.]", "", .))),
    )

monthly_estimates <- clean_estimates %>%
  pivot_longer(
    cols = starts_with(c("september", "october", "november", "december")),
    names_to = c("month", ".value"),
    names_sep = "_"
  ) 

library(dplyr)
library(stringr)
library(lubridate)
library(purrr)
library(tidyr)

# Helper: last day of month
last_day <- function(d) ceiling_date(d, "month") - days(1)

# Parse a single flight string into start/end Dates
parse_flight_one <- function(s, default_year = 2025) {
  if (is.null(s) || length(s) == 0) return(list(start = as.Date(NA), end = as.Date(NA)))
  s0 <- tolower(as.character(s)) |> 
    str_trim() |>
    str_replace_all("[–—]", "-") |>         # normalize dashes
    str_replace_all("\\s+", "") |>          # remove spaces
    str_replace_all("utc", "") |>
    str_replace_all("^flight$", "") |>      # header junk
    str_replace_all("sept", "sep")          # normalize month name

  if (s0 == "") return(list(start = as.Date(NA), end = as.Date(NA)))

  # ISO date only (same start/end)
  if (str_detect(s0, "^\\d{4}-\\d{2}-\\d{2}$")) {
    d <- ymd(s0)
    return(list(start = d, end = d))
  }

  # Two specific mm/dd separated by comma -> treat as range
  s0 <- str_replace_all(s0, ",", "-")

  # Month-name range like "oct-dec"
  mon_re <- "jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec"
  if (str_detect(s0, sprintf("^(%s)-(%s)$", mon_re, mon_re))) {
    m1 <- str_match(s0, sprintf("^(%s)-(%s)$", mon_re, mon_re))[,2]
    m2 <- str_match(s0, sprintf("^(%s)-(%s)$", mon_re, mon_re))[,3]
    m1i <- match(m1, month.abb |> tolower() |> str_sub(1,3))
    m2i <- match(m2, month.abb |> tolower() |> str_sub(1,3))
    d1 <- make_date(default_year, m1i, 1)
    d2 <- make_date(default_year, m2i, 1) |> last_day()
    # handle wrap (e.g., nov-jan)
    if (!is.na(d1) && !is.na(d2) && d2 < d1) d2 <- make_date(default_year + 1, m2i, 1) |> last_day()
    return(list(start = d1, end = d2))
  }

  # Range like mm/dd-mm/dd
  if (str_detect(s0, "^\\d{1,2}/\\d{1,2}-\\d{1,2}/\\d{1,2}$")) {
    parts <- str_split(s0, "-", n = 2)[[1]]
    d1 <- mdy(paste0(parts[1], "/", default_year))
    d2 <- mdy(paste0(parts[2], "/", default_year))
    # handle wrap-around year (rare here, but safe)
    if (!is.na(d1) && !is.na(d2) && d2 < d1) d2 <- d2 + years(1)
    return(list(start = d1, end = d2))
  }

  # Single mm/dd -> same start/end
  if (str_detect(s0, "^\\d{1,2}/\\d{1,2}$")) {
    d <- mdy(paste0(s0, "/", default_year))
    return(list(start = d, end = d))
  }

  # Single ISO with time stripped already caught; also try ymd_hms just in case
  if (str_detect(s0, "^\\d{4}-\\d{2}-\\d{2}t?")) {
    d <- ymd_hms(s0, quiet = TRUE)
    if (is.na(d)) d <- ymd(s0, quiet = TRUE)
    d <- as_date(d)
    return(list(start = d, end = d))
  }

  # Fallback: NA
  list(start = as.Date(NA), end = as.Date(NA))
}

# Ensure 'flight' is a simple character vector (yours was a <list>)
monthly_estimates <- monthly_estimates %>%
  mutate(
    flight_chr = map_chr(flight, ~ if (length(.x)) as.character(.x) else NA_character_),
    parsed = map(flight_chr, parse_flight_one, default_year = 2025),
    start_date = as.Date(map(parsed, "start") |> unlist()),
    end_date   = as.Date(map(parsed, "end")   |> unlist())
  ) %>%
  select(-parsed, -flight_chr)

# ---- Split monthly totals evenly across ISO weeks ----
# Helper to build a table of ISO week spans overlapped by the given month & flight window
build_weeks <- function(start_date, end_date, month_label, default_year = 2025) {
  if (is.na(month_label)) return(tibble())
  m <- match(tolower(month_label), tolower(month.name))
  if (is.na(m)) return(tibble())
  # Prefer the year from the placement start_date if available; otherwise fallback
  y <- if (!is.na(start_date)) lubridate::year(start_date) else default_year
  month_start <- lubridate::make_date(y, m, 1)
  month_end   <- (lubridate::ceiling_date(month_start, "month") - lubridate::days(1))

  # Intersect the flight window with the month window
  ps <- if (!is.na(start_date)) pmax(start_date, month_start) else month_start
  pe <- if (!is.na(end_date))   pmin(end_date,   month_end)   else month_end
  if (is.na(ps) || is.na(pe) || pe < ps) return(tibble())

  # Generate ISO week buckets (weeks start on Monday)
  ws0 <- lubridate::floor_date(ps, unit = "week", week_start = 1)
  we_last <- lubridate::floor_date(pe, unit = "week", week_start = 1)
  weeks <- seq(ws0, we_last, by = "1 week")
  tibble(week_start = weeks,
         week_end   = pmin(weeks + lubridate::days(6), pe))
}

weekly_estimates <- monthly_estimates %>%
  mutate(row_id = dplyr::row_number()) %>%
  mutate(week_tbl = purrr::pmap(
    list(start_date, end_date, month),
    ~ build_weeks(..1, ..2, ..3)
  )) %>%
  tidyr::unnest(week_tbl) %>%
  dplyr::group_by(row_id) %>%
  dplyr::mutate(
    weeks_in_bucket = dplyr::n(),
    weekly_spend = dplyr::if_else(is.na(spend), NA_real_, spend / weeks_in_bucket),
    weekly_impressions = dplyr::if_else(is.na(impressions), NA_real_, impressions / weeks_in_bucket)
  ) %>%
  dplyr::ungroup() %>%
  dplyr::select(
    channel, partner, tactics, flight, month,
    start_date, end_date,
    week_start, week_end,
    weekly_spend, weekly_impressions,
    spend, impressions, total_est_spend, total_est_impressions, planned_cpm, total_est_impressions
  )


glimpse(monthly_estimates)
glimpse(weekly_estimates)

monthly_estimates$flight <- as.character(monthly_estimates$flight)
weekly_estimates$flight <- as.character(weekly_estimates$flight)

raw_estimates$flight

write.csv(monthly_estimates, "adif/data/monthly_estimates.csv", row.names = FALSE)
write.csv(weekly_estimates, "adif/data/weekly_estimates.csv", row.names = FALSE)
bq_table_upload(bq_table(project = "looker-studio-pro-452620", dataset = "landing", table = "adif_monthly_estimates"), monthly_estimates)
bq_table_upload(bq_table(project = "looker-studio-pro-452620", dataset = "landing", table = "adif_weekly_estimates"), weekly_estimates)

## The following code snippets are provided for context only and are not part of the current file.append(monthly_estimates)