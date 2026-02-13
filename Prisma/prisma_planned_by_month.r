
source("/Users/eugenetsenter/.Rprofile")
#utils <- new.env(parent = globalenv())


sys.source("/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/R_functions/bq_functions.r", envir = utils, keep.source = FALSE)
sys.source("/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/R_functions/Extract_pID_func.R", envir = utils, keep.source = FALSE)
sys.source("/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/R_functions/Func_DayToWeek.R", envir = utils, keep.source = FALSE)

df <- utils$f_gmail_csv_to_dataframe(
  search_criteria = 'prisma__planned_by_month',
  num_results = 10,
  attachment_index = 1,
  validate_date = FALSE,
  filter_na_month = TRUE
) 

df <- df %>%
  dplyr::  mutate(
    date = mdy(month),
    Month = month(mdy(month))
)

str(df)
# pull dcm data from BQ
dcm <- 
  dbGetQuery(
    bq_con,
    glue::glue(
      "SELECT *
      FROM `looker-studio-pro-452620.landing.dcm_adswerve_processed`"
    )
  ) 

dcm_ref <- dplyr::tbl(bq_con, "looker-studio-pro-452620.repo_stg.dcm_plus_utms")

colnames(dcm_ref)

dcm_data <- dcm_ref %>% filter(
    date >= "2025-01-01",
    p_advertiser_name == "Forevermark US"
    ) %>%
    collect() %>%
  mutate(Month = month(date))
    
str(dcm_data)

joined <- dcm_data %>%
  full_join(df, by = "Month", "package_id")


join_test <- joined %>%
group_by(p_package_friendly) %>% summarise(
    sum(impressions),
    sum(daily_recalculated_cost)
)

join_test
)
    

df <- df %>% mutate(key = paste0(package_id, Month))
str(df)

dcm_data_2 <- dcm_data %>%
  mutate(Month = month(date)) %>%
  mutate(key = paste0(package_id, Month)) %>%


  group_by(package_id, Month, key) %>%
  summarise(
    impressions = sum(impressions, na.rm = TRUE),
    daily_recalculated_cost = sum(daily_recalculated_cost, na.rm = TRUE)
  ) %>% ungroup()

(dcm_data_2_test <- dcm_data_2 %>%
  group_by(package_id,Month,key) %>%
  summarise(
    impressions = sum(impressions, na.rm = TRUE),
    daily_recalculated_cost = sum(daily_recalculated_cost, na.rm = TRUE),
  ) %>% filter (package_id == "P37K85G"))

str(df$Month)

str(dcm_data_2$Month)
df_test <- df %>%
  group_by(package_id, Month) %>%
  summarise(
    planned_imps = sum(planned_units, na.rm = TRUE),
    planned_cost = sum(planned_amount, na.rm = TRUE),
    .groups = "drop"
  ) %>% filter (package_id == "P37K85G")

df_test

join_prsm_to_dcm <- df %>%
  left_join(
    dcm_data_2, by = "key"
  ) %>% rename( "Month_p" = Month.x , "Month_dcm" = Month.y, package_id = package_id.x, package_id_dcm = package_id.y) #%>% drop(package_id.y
  

str(join_prsm_to_dcm)

join_prsm_to_dcm %>% filter (advertiser_name == "Forevermark US") %>% view() 



dcm_data_2_test

utils$f_write_to_bq(join_prsm_to_dcm,"landing","adif_prmsaDCM_bymonth")
