# =============================================================================
# Purpose:  Pull recent Basis UTM data, flag dupes, summarise impressions
# Author:   Gene
# Date:     2025-07-16
# =============================================================================

library(DBI)        # DB-agnostic interface
library(bigrquery)  # BigQuery backend
library(dplyr)      # Tidy data ops
library(glue)       # Safe SQL string interpolation

# ---- 1. Connect ----
con <- dbConnect(
  bigrquery::bigquery(),
  project = "looker-studio-pro-452620"
)
on.exit(dbDisconnect(con), add = TRUE)

# ---- 2. Define table & date window ----
tbl_id   <- `looker-studio-pro-452620.utm_scrap.basis_utms_4`
since_dt <- "2025-01-01"

# ---- 3. Pull data + flag duplicates inside BigQuery ----
sql <- glue_sql("
  SELECT *,
         COUNT(*) OVER (PARTITION BY del_key, date) AS row_dupe_count   -- duplicates across *all* cols
  FROM   `looker-studio-pro-452620.utm_scrap.basis_utms_4`
  WHERE  date >= {since_dt}
", .con = con)

utm_data4 <- dbGetQuery(con, sql)

# ---- 4. Inspect duplicates ----
dupes <- utm_data4 %>% filter(row_dupe_count > 1)

if (nrow(dupes)) {
  message(glue("Found {nrow(dupes)} duplicate rows (same across all columns):"))
  print(dupes)
} else {
  message("No duplicate rows.")
}

# ---- 5. Aggregate impressions (deduped) ----
agg <- utm_data4 %>%                                   # already restricted by date
  distinct() %>%                                       # drop exact dupes
  group_by(campaign) %>%
  summarise(total_impressions = sum(impressions, na.rm = TRUE), .groups = "drop") %>%
  arrange(desc(total_impressions))

print(agg)