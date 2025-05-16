library(readxl)
library(dplyr)
library(tidyr)
library(janitor)   
library(stringr)

df <- read_excel(
  '/Users/eugenetsenter/Downloads/MASSMUTUAL003CP_Q1_Trafficking Sheet_DSP_CTV, OLV, & Audio ONLY_3.17.25 (2).xlsx',
  sheet = "Q1 Tsheet",
  skip = 6,
) %>% 
  clean_names()                          # camelCase → snake_case



# Make all creative columns follow creative_<number>_<attribute>
names_test <- str_remove(str_replace(str_replace(names(df),"creative_",""),"number_",""),"\\d*") 
names_prefix <- paste0("creative_",str_extract(names(df),"-?\\d*\\.?\\d+"),"__", names_test)
names(df) <- names_prefix


names(df) <- names(df) |>
  # Remove trailing numbers from names: creative_1__name_1 → creative_1__name
  str_replace("(_name|_url|_asset_link|_edo_tag|_disqo_tag|_video_amp_tag|_3p_or_1p_tag)_\\d+$", "\\1") |>
  # Remove all sequences of 2 or more underscores: creative_1__name → creative_1_name
  str_replace_all("_+", "_") |>
  # Fix creative_3__url_3_number → creative_3_url
  str_replace("_url_3_number", "_url")


# 2️⃣  Identify all creative columns ---------------------------------------
cre_cols <- grep("(name|url|asset_link|edo_tag|disqo_tag|video_amp_tag|3p_or_1p_tag)",
                 names(df), value = TRUE)

library(tidyr)
creative_long <- df %>%
  pivot_longer(
    cols = matches("^creative_\\d+_"),
    names_to = c("creative_num", ".value"),
    names_pattern = "^creative_(\\d+)_(.*)$",
    values_drop_na = TRUE
  ) %>%
  mutate(creative_num = as.integer(creative_num))
# 
