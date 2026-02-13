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
  project = "giant-spoon-299605"
)
on.exit(dbDisconnect(con), add = TRUE)

# ---- 2. Define table & date window ----
tbl_id <- DBI::SQL("giant-spoon-299605.ALL_DCM_adswerve.DCMtoBigquery_Last14D_0125_cm360_1123381_1389414566_20250702_20250715_20250716_004006")

since_dt <- "2025-01-01"

# ---- 3. Pull data + flag duplicates inside BigQuery ----
sql <- glue_sql("
  SELECT
    t.* ,                                           -- keep all original cols
    COUNT(*) OVER (PARTITION BY t.date, t.ad) 
      AS row_dupe_count                             -- new analytic col
  FROM   {tbl_id} AS t
  WHERE  t.date >= {since_dt}
", .con = con)
utm_data4 <- dbGetQuery(con, sql)
str(utm_data4)

library(dplyr)                     # ensure attached
dupes <- utm_data4 %>% dplyr::filter(row_dupe_count > 1)
nrow(dupes)                        # should run without error



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