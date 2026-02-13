# -*- coding: utf-8 -*-
"""mft_download_0519

# 📊 Basis MFT Data Validation Report

The data referenced in this notebook can be cross-checked at the following Looker Studio dashboard:

🔗 [Looker Studio Report](https://lookerstudio.google.com/reporting/71e16a01-567f-487f-b71e-28076aaba2ee/page/p_65s1luv5pd/edit)

---
"""

import os
from google.cloud import bigquery
import pandas as pd

# Remove Colab auth and use standard credentials
# Ensure you have set GOOGLE_APPLICATION_CREDENTIALS environment variable
# or use explicit credentials file
client = bigquery.Client()

# @title Pull view [looker-studio-pro-452620] [repo_mart] [mft_clean_view ]


query = "SELECT * FROM `looker-studio-pro-452620.repo_mart.mft_clean_view`"
df = client.query(query).to_dataframe()
#df.to_csv('/Users/eugenetsenter/Docs/delivery_mft.csv', index=False)

# @title EXPORT TO GOOGLE CLOUD STORAGE [mft_from_bq]

# from google.colab import auth
# auth.authenticate_user()

project_id = 'looker-studio-pro-452620'
bucket_name = 'mft_from_bq'  # Replace with your bucket name
file_name = 'delivery_mft.csv'

# Upload the DataFrame to GCS
df.to_csv(f'gs://{bucket_name}/{file_name}', index=False)

print(f'DataFrame saved to gs://{bucket_name}/{file_name}')

"""# Tests & Validations"""



# @title Check for duplicated rows
# prompt: test if there are any duplicate lines in df and print  if any are detected along with the placement ID

import pandas as pd

# Assuming df is already loaded from the BigQuery table 'looker-studio-pro-452620.repo_mart.mft_clean_view'

duplicates = df[df.duplicated(keep=False)]  # keep=False marks all duplicates as True

if not duplicates.empty:
    print("❌ Duplicate rows detected:")
    for index, row in duplicates.iterrows():
        print(f"Row {index + 1}:")  # Adding 1 to index for human-readable row numbers
        print(row)
        # Print the placement_name for the duplicate row (if it exists in the DataFrame)
        if 'placement_name' in df.columns:
            print(f"Placement Name: {row['placement_name']}")
        print("---")
else:
    print("✅ No duplicate rows found. \n")

# prompt: check if max(date) = yesterday.  if so print "data up to date" if not print "NOT UP TO DATE", new line "data updated through: [max(date]"
import pandas as pd

# Ensure max_date is a date (not datetime with time)
max_date = pd.to_datetime(df['date']).max().normalize()

# Get yesterday's date (also normalized to midnight)
yesterday = pd.Timestamp.now().normalize() - pd.DateOffset(days=1)

if max_date == yesterday:
    print("✅ data up to date through: ")
    print(max_date.strftime('%Y-%m-%d'))
else:
    print("❌ NOT UP TO DATE")
    print(f"data updated through: {max_date.strftime('%Y-%m-%d')}")

# @title MISSING UTMS

import pandas as pd
from tabulate import tabulate

# ── 1. Define “blank” markers & build mask ───────────────────────────────
blank_vals = {'', 'not set', '(not set)', 'null'}
utm_series = df['utm_content']

blank_mask = (
      utm_series.isna()
   |  utm_series.fillna('').str.strip().str.lower().isin(blank_vals)
)

blank_rows = df.loc[blank_mask]

# ── 2. Overall totals ────────────────────────────────────────────────────
total_imps     = df['Impressions'].sum()
total_cost     = df['Cost'].sum()
total_places   = df[['campaign', 'placement_name']].drop_duplicates().shape[0]

blank_imps     = blank_rows['Impressions'].sum()
blank_cost     = blank_rows['Cost'].sum()
blank_places   = blank_rows[['campaign', 'placement_name']].drop_duplicates().shape[0]

# % calculations
placements_pct = (blank_places / total_places * 100) if total_places else 0
imp_pct        = (blank_imps   / total_imps   * 100) if total_imps   else 0
cost_pct       = (blank_cost   / total_cost   * 100) if total_cost   else 0

# ── 3. One-line summary ──────────────────────────────────────────────────
print(f"📌 Number of placements with blank/null utm_content: {blank_places:,} of [{total_places}] "
      f"({placements_pct:.2f}% of total)")
print(f"🧮 Total impressions: {blank_imps:,.0f} ({imp_pct:.2f}% of total)")
print(f"💰 Total spend: ${blank_cost:,.0f} ({cost_pct:.2f}% of total)")

# ── 4. Top-10 offending placements by spend ──────────────────────────────
top10 = (
    blank_rows.groupby(['placement_name'])[['Impressions', 'Cost']]
    .sum()
    .sort_values(by='Cost', ascending=False)
    .head(20)
    .reset_index()
)

# Nicely format numbers
top10['Impressions'] = top10['Impressions'].map('{:,.0f}'.format)
top10['Cost']        = top10['Cost'].map('${:,.0f}'.format)

print('\n🔝 Top 20 placements with blank/null utm_content (ranked by spend)')
print(tabulate(top10, headers='keys', tablefmt='github', showindex=False))

# @title Aggregates and totals
# Step 1: Group and aggregate
# Count blank rows per (campaign, date)
blank_counts = (
    blank_rows.groupby(['campaign', 'date'])
    .size()
    .rename('blank_rows_count')
    .reset_index()
)

# Join with df on campaign and date
df_with_blanks = df.merge(blank_counts, on=['campaign', 'date'], how='left')

# Aggregate final results by campaign
result1 = df_with_blanks.groupby('campaign').agg({
    'Impressions': 'sum',
    'Cost': 'sum',
    'date': ['min', 'max'],
    'blank_rows_count': 'first'  # Assumes one value per campaign
})
result2 = pd.DataFrame({
    'Impressions_sum': [df['Impressions'].sum()],
    'Cost_sum': [df['Cost'].sum()],
    'date_min': [df['date'].min()],
    'date_max': [df['date'].max()]
})

# Step 2: Rename columns
# Step 2: Flatten column names
result1.columns = ['_'.join(col).strip() for col in result1.columns.values]
result1 = result1.reset_index()

# Step 3: Round and format
result1['Cost_sum'] = result1['Cost_sum'].round(0)
result1['Impressions_sum'] = result1['Impressions_sum'].apply(lambda x: f"{x:,.0f}")
result1['Cost_sum'] = result1['Cost_sum'].apply(lambda x: f"${x:,.0f}")
result1['date_min'] = pd.to_datetime(result1['date_min']).dt.strftime('%Y-%m-%d')
result1['date_max'] = pd.to_datetime(result1['date_max']).dt.strftime('%Y-%m-%d')

result2['Impressions_sum'] = result2['Impressions_sum'].apply(lambda x: f"{x:,.0f}")
result2['Cost_sum'] = result2['Cost_sum'].apply(lambda x: f"${x:,.0f}")
# Display result
result2

result1

"""#scrap"""

# import pandas as pd

# # Total dataset stats
# total_impressions = df['Impressions'].sum()
# total_spend = df['Cost'].sum()

# # Filter for blank or null utm_content
# blank_utm_content = df[df['utm_content'].isna() | (df['utm_content'].str.strip() == '')]

# # Calculate metrics
# num_placements = blank_utm_content.shape[0]
# blank_impressions = blank_utm_content['Impressions'].sum()
# blank_spend = blank_utm_content['Cost'].sum()

# imp_percentage = (blank_impressions / total_impressions) * 100 if total_impressions else 0
# spend_percentage = (blank_spend / total_spend) * 100 if total_spend else 0

# # Get unique "campaign/placement_name" combos
# campaign_placements = (
#     blank_utm_content[['campaign', 'placement_name']]
#     .drop_duplicates()
#     .apply(lambda row: f"{row['campaign']}/{row['placement_name']}", axis=1)
#     .tolist()
# )

# # Output
# print(f"📌 Number of placements with blank/null utm_content: {num_placements}")
# print(f"🧮 Total impressions: {blank_impressions:,.0f} ({imp_percentage:.2f}% of total)")
# print(f"💰 Total spend: ${blank_spend:,.0f} ({spend_percentage:.2f}% of total)")
# print("📋 Unique Campaign/Placement list:")
# for item in campaign_placements:
#     print(f"  - {item}")

### {removed} download df as /Users/eugenetsenter/Docs/delivery_mft.csv

#df.to_csv('delivery_mft.csv', index=False)

# files.download('delivery_mft.csv')

# Commented out IPython magic to ensure Python compatibility.
# %whos
