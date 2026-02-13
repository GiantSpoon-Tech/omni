# ADIF TV & Digital Data Pipeline

Complete documentation of the ADIF (Advertising Intelligence & Forecasting) data pipeline that unifies TV and digital advertising data for Forevermark US campaigns.

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES (Landing)                      │
├─────────────────────────────────────────────────────────────────┤
│  DIGITAL SOURCES:                    TV SOURCES:                │
│  • DCM.20250505_costModel_v5        • tv_local_estimates        │
│  • landing.adif_fpd_data_ranged     • tv_national_estimates     │
│  • 20250327_data_model.prisma...                                │
└────────────────┬───────────────────────────────┬────────────────┘
                 │                               │
                 ▼                               ▼
┌─────────────────────────────┐   ┌─────────────────────────────┐
│   STAGING LAYER (Views)     │   │    TV COMBINED (View)       │
│ adif__prisma_expanded_plus  │   │  Simple UNION of local      │
│ _dcm_view_v3_test           │   │  and national estimates     │
│ (Complex FULL OUTER JOINs)  │   │                             │
└────────────────┬────────────┘   └────────────┬────────────────┘
                 │                               │
                 └───────────────┬───────────────┘
                                 ▼
                   ┌─────────────────────────────┐
                   │     MART LAYER (Final)      │
                   │  adif__tv_digital_unioned   │
                   │  (Union All + Calculated)   │
                   └─────────────────────────────┘
```

## Table of Contents

- [Data Sources](#data-sources)
- [Staging Layer](#staging-layer)
- [Mart Layer](#mart-layer)
- [Data Ingestion Scripts](#data-ingestion-scripts)
- [Key Concepts](#key-concepts)
- [Current State](#current-state)

---

## Data Sources

### Digital Sources (3 tables)

#### 1. DCM Cost Model
**Table**: `looker-studio-pro-452620.DCM.20250505_costModel_v5`
**Rows**: 129,733
**Purpose**: DoubleClick Campaign Manager delivery data

**Key Fields**:
- `package_id` - Unique package identifier
- `date` - Delivery date
- `impressions` - Ad impressions delivered
- `media_cost` - Actual media spend
- `clicks` - Click-through events
- `rich_media_video_plays` - Video play metrics
- `rich_media_video_completions` - Video completion metrics

**Update Frequency**: Daily (automated)

---

#### 2. First-Party Data (FPD)
**Table**: `looker-studio-pro-452620.landing.adif_fpd_data_ranged`
**Rows**: 1,620
**Purpose**: Partner-reported performance metrics

**Key Fields**:
- `package_id` - Matches DCM package ID
- `date_final` - Reporting date
- `impressions`, `clicks`, `spend` - Performance metrics
- `sends`, `opens` - Email-specific metrics
- `partner_creative_name` - Creative identifier
- `benchmark`, `benchmark_metric` - Performance benchmarks

**Source**: Google Sheets via `util_collect_fpd_v2.r`
**Update Frequency**: Daily (via R script)

**Special Feature**: Includes creative-level granularity that DCM may not capture

---

#### 3. Prisma Media Planning
**Table**: `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
**Rows**: 666,155
**Purpose**: Media planning and budgeting data

**Key Fields**:
- `package_id` - Package identifier
- `date` - Planning date
- `planned_daily_spend_pk` - Planned daily spend at package level
- `planned_daily_impressions_pk` - Planned daily impressions
- `report_date` - When the plan was generated
- `advertiser_name` - Client/advertiser name
- `package_type` - Package classification

**Update Frequency**: Daily (automated)

---

### TV Sources (2 tables)

#### 4. TV Local Estimates
**Table**: `looker-studio-pro-452620.landing.tv_local_estimates`
**Rows**: 352
**Purpose**: Local market TV campaign estimates

**Key Fields**:
- `date` - Broadcast date
- `advertiser` - Client name
- `media_outlet` - TV network/station
- `program_name` - TV program
- `net_impressions` - Estimated impressions
- `net_cost` - Estimated cost
- `market` - Geographic market
- `type`, `campaign_name` - Campaign metadata

**Source**: Google Sheets via `util_collect_monthly_estimates.r`

---

#### 5. TV National Estimates
**Table**: `looker-studio-pro-452620.landing.tv_national_estimates`
**Rows**: 711
**Purpose**: National TV campaign estimates

**Key Fields**: Same schema as `tv_local_estimates`

**Source**: Google Sheets via `util_collect_monthly_estimates.r`

---

## Staging Layer

### Digital Staging: `adif__prisma_expanded_plus_dcm_view_v3_test`

**Location**: `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`

This is the most complex component of the pipeline. It performs sophisticated multi-source integration using deterministic, null-safe FULL OUTER JOINs.

#### Processing Steps

**1. DCM Normalization (CTE: `a`)**
```sql
-- Normalizes package IDs (handles duplicates/aliases)
CASE
  WHEN package_id = 'P3923KK' THEN 'P37K96P'
  WHEN package_id = 'P37K96T' THEN 'P37K96P'
  -- ... (6 total mappings)
  ELSE package_id
END AS package_id
```
- Aggregates daily delivery metrics
- Prefixes columns with `d_` (e.g., `d_daily_recalculated_cost`, `d_impressions`)
- Groups by: `package_id`, `flight_status_flag`, `DATE(date)`

**2. FPD Processing (CTEs: `fpd_raw` → `fpd`)**
- Applies same package ID normalization as DCM
- Aggregates metrics to package + date level
- Concatenates multiple creatives per day using `STRING_AGG`
- Prefixes columns with `fpd_` (e.g., `fpd_impressions`, `fpd_spend`)

**3. Prisma Processing (CTEs: `b_raw` → `b`)**
- Filters: `advertiser_name = 'Forevermark US'` AND `package_type != 'Child'`
- Deduplicates by taking latest `report_date`
- Aggregates planned metrics to package + date level
- Keeps: `planned_daily_spend_pk`, `planned_daily_impressions_pk`

**4. Multi-Source Join Strategy**

**Step 1** (CTE: `a_fpd`):
```sql
-- FULL OUTER JOIN DCM + FPD
-- Keeps orphaned days that exist only in DCM OR only in FPD
SELECT
  COALESCE(da.package_id, df.package_id) AS package_id_joined,
  COALESCE(da.date, df.date) AS date,
  da.* EXCEPT(package_id, date),
  df.* EXCEPT(package_id, date)
FROM a AS da
FULL OUTER JOIN fpd AS df
  ON da.package_id = df.package_id
 AND da.date = df.date
```

**Step 2** (CTE: `t`):
```sql
-- FULL OUTER JOIN (DCM+FPD) + Prisma
-- Keeps orphaned days that exist only in Prisma too
FROM a_fpd AS af
FULL OUTER JOIN b AS db
  ON af.package_id_joined = db.package_id
 AND af.date = db.date
```

**5. Final Coalescing (CTE: `final`)**
```sql
-- Creates final_* columns with data prioritization
COALESCE(fpd_spend, d_daily_recalculated_cost) AS final_spend,
COALESCE(fpd_impressions, d_daily_recalculated_imps) AS final_impressions,
COALESCE(fpd_clicks, d_clicks) AS final_clicks
```

**Data Prioritization Hierarchy**: FPD → DCM → NULL

**6. Package-Level Rollups (CTE: `pkg`)**
- Calculates package-level totals: `pkg_act_imps`, `pkg_act_spend`
- Provides context for daily metrics

#### Output Schema (Key Columns)

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `package_id_joined` | STRING | Coalesced | Normalized package ID |
| `date` | DATE | Coalesced | Calendar date |
| `final_spend` | FLOAT | FPD→DCM | Actual spend (prioritized) |
| `final_impressions` | INTEGER | FPD→DCM | Actual impressions (prioritized) |
| `final_clicks` | INTEGER | FPD→DCM | Clicks delivered |
| `planned_daily_spend_pk` | FLOAT | Prisma | Planned spend |
| `planned_daily_impressions_pk` | INTEGER | Prisma | Planned impressions |
| `fpd_creative` | STRING | FPD | Concatenated creative names |
| `flight_status_flag` | STRING | DCM | Campaign flight status |

---

### TV Staging: `tv_combined`

**Location**: `looker-studio-pro-452620.landing.tv_combined`

Simple union of local and national TV estimates:

```sql
SELECT * FROM `looker-studio-pro-452620.landing.tv_local_estimates`
UNION ALL
SELECT * FROM `looker-studio-pro-452620.landing.tv_national_estimates`
```

**Total Rows**: 1,063 (352 local + 711 national)

---

## Mart Layer

### Final Output: `adif__tv_digital_unioned`

**Location**: `looker-studio-pro-452620.repo_mart.adif__tv_digital_unioned`
**Total Rows**: 12,085

This is the unified reporting view consumed by Looker Studio and other BI tools.

#### Structure

**CTE: `d` (Digital)**
```sql
SELECT
  date,
  advertiser_name as advertiser,
  "digital" as type,
  CASE
    WHEN REGEXP_CONTAINS(package_name, "ooh|OOH") THEN "OOH"
    ELSE channel_group
  END as channel,
  initiative as package_name,
  package_id_joined as package_id,
  supplier_code,
  gsMediaTeam_channel,
  (pkg_act_imps) as total_pkg_impressions,
  (pkg_act_spend) as total_pkg_cost,
  sum(final_spend) as actual_cost,
  sum(final_impressions) as actual_impressions,
  sum(planned_daily_impressions_pk) as planned_impressions,
  sum(planned_daily_spend_pk) as planned_cost,
  sum(fpd_impressions) as fpd_impressions,
  sum(fpd_spend) as fpd_cost
FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
GROUP BY 1,2,3,4,5,6,7,8,9,10
```

**CTE: `tv` (TV)**
```sql
SELECT
  date,
  advertiser as advertiser,
  "tv" as type,
  "linear" as channel,
  concat(media_outlet," - ",program_name) as package_name,
  concat(media_outlet," - ",program_name) as package_id,
  media_outlet as SITE,
  "linear" as gsMediaTeam_channel,
  null as total_pkg_impressions,
  null as total_pkg_cost,
  null as actual_cost,
  null as actual_impressions,
  sum(net_impressions) as planned_impressions,
  sum(net_cost) as planned_cost,
  null as fpd_impressions,
  null as fpd_cost
FROM `looker-studio-pro-452620.landing.tv_combined`
GROUP BY 1,2,3,4,5,6,7,8
```

**Final Select (with calculated fields)**:
```sql
SELECT *,
  CASE
    WHEN actual_impressions IS NULL THEN u.planned_impressions
    WHEN actual_impressions < 100 THEN u.planned_impressions
    ELSE actual_impressions
  END as estimated_impressions,

  CASE
    WHEN actual_cost IS NULL THEN u.planned_cost
    ELSE actual_cost
  END as estimated_cost
FROM (
  SELECT * FROM d
  UNION ALL
  SELECT * FROM tv
) u
```

#### Output Schema (18 columns)

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Calendar date |
| `advertiser` | STRING | Client/advertiser name |
| `type` | STRING | "digital" or "tv" |
| `channel` | STRING | Media channel (e.g., "display", "social", "linear") |
| `package_name` | STRING | Campaign package name |
| `package_id` | STRING | Unique package identifier |
| `supplier_code` | STRING | Media supplier/platform code |
| `gsMediaTeam_channel` | STRING | Internal channel classification |
| `total_pkg_impressions` | FLOAT | Package-level total impressions |
| `total_pkg_cost` | FLOAT | Package-level total cost |
| `actual_cost` | FLOAT | Actual delivered cost |
| `actual_impressions` | FLOAT | Actual delivered impressions |
| `planned_cost` | FLOAT | Planned/budgeted cost |
| `planned_impressions` | FLOAT | Planned/budgeted impressions |
| `fpd_cost` | FLOAT | First-party reported cost |
| `fpd_impressions` | FLOAT | First-party reported impressions |
| `estimated_impressions` | FLOAT | **Calculated**: actual (if ≥100) else planned |
| `estimated_cost` | FLOAT | **Calculated**: actual else planned |

---

## Data Ingestion Scripts

### 1. `util_collect_fpd_v2.r`

**Location**: `adif/util_collect_fpd_v2.r`
**Purpose**: Ingests First-Party Data from Google Sheets

#### Configuration
```r
gdrive_folder_id <- "1EyN93JE7v4OXjMMQREVuZ4ZN7xEed5WB"
pattern <- "De Beers | Partner Data"
output_dir <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/data"
```

#### 7-Phase Pipeline

**Phase 1: Google Drive Discovery**
- Searches folder for sheets matching pattern "De Beers | Partner Data"
- Extracts metadata: sheet ID, name, URL, last modified date
- **Output**: `phase1_discovered_files.csv`

**Phase 2: Header Detection**
- Reads first 20 rows of each sheet
- Detects header row automatically (looks for column name patterns)
- **Output**: `phase2_header_detection.csv`

**Phase 3: Raw Header Collection**
- Extracts actual column names from detected header rows
- Stores raw headers for inspection
- **Output**: `phase3_raw_headers.csv`

**Phase 4: Column Normalization**
- Maps raw headers to normalized KPI metric names
- Known metrics: `spend`, `impressions`, `clicks`, `sends`, `opens`, `views`, etc.
- **Output**: `phase4_normalization_mapping.csv`

**Phase 5: Data Combination**
- Reads all sheets using detected headers
- Combines into master dataset
- **Output**: `phase5_combined_master_data.csv`

**Phase 6: Data Cleaning**
- Converts KPI metrics to numeric
- Handles missing values
- **Output**: `phase6_cleaned_master_data.csv`

**Phase 7: Daily Disaggregation**
- Splits monthly/range data into daily records
- Evenly distributes metrics across date ranges
- **Output**: `phase7_daily_master_data.csv`
- **BigQuery Upload**: `landing.adif_fpd_data_ranged`

#### Key Features
- **Checkpoint System**: Each phase saves CSV for debugging
- **Incremental Processing**: Can skip phases using `use_saved_phases = TRUE`
- **Flexible Header Detection**: Handles inconsistent sheet structures

---

### 2. `util_collect_monthly_estimates.r`

**Location**: `adif/util_collect_monthly_estimates.r`
**Purpose**: Processes TV monthly estimates from Google Sheets

#### Source Sheet
```r
sheet_id <- "1FLn_mge8Tz4wKqEmrIPQG_B2_MecS-9kNiQThQaXe_8"
range <- "'Media Plan Monthlies '!B6:P65"
```

#### Processing Steps

**1. Read & Clean**
```r
raw_estimates <- read_sheet(sheet_id, range, col_names = c(...))
clean_estimates <- raw_estimates %>%
  mutate(across(matches("spend|impressions|cpm"), ~ as.numeric(gsub("[^0-9.]", "", .))))
```

**2. Pivot to Long Format**
```r
monthly_estimates <- clean_estimates %>%
  pivot_longer(
    cols = starts_with(c("september", "october", "november", "december")),
    names_to = c("month", ".value"),
    names_sep = "_"
  )
```

**3. Flight Date Parsing**

Supports multiple date formats:
- ISO dates: `2025-06-30`
- MM/DD format: `10/15`
- Month ranges: `oct-dec`
- MM/DD ranges: `10/1-10/31`

```r
parse_flight_one <- function(s, default_year = 2025) {
  # Handles various flight date formats
  # Returns: list(start = Date, end = Date)
}
```

**4. Weekly Disaggregation**
```r
weekly_estimates <- monthly_estimates %>%
  mutate(week_tbl = pmap(list(start_date, end_date, month), ~ build_weeks(...))) %>%
  unnest(week_tbl) %>%
  mutate(
    weekly_spend = spend / weeks_in_bucket,
    weekly_impressions = impressions / weeks_in_bucket
  )
```

**Key Logic**:
- Splits monthly totals into ISO weeks (Monday-start)
- Distributes spend/impressions evenly across weeks
- Handles partial weeks at month boundaries

#### Outputs

**Local CSVs**:
- `adif/data/monthly_estimates.csv`
- `adif/data/weekly_estimates.csv`

**BigQuery Tables**:
- `landing.adif_monthly_estimates`
- `landing.adif_weekly_estimates`

---

## Key Concepts

### 1. Data Prioritization Hierarchy

When multiple sources report the same metric for the same package + date:

```
FPD (First-Party Data) → DCM (Third-Party) → Planned (Prisma) → NULL
```

**Rationale**: First-party data from partners is considered most accurate, followed by DCM's third-party tracking. Planned data is only used when actuals are unavailable.

### 2. Orphan Day Handling

**Problem**: Different data sources may have gaps on different days.

**Solution**: FULL OUTER JOINs preserve all days from all sources.

**Example**:
- Day 1: DCM has data, FPD missing → Row created with DCM data, FPD columns NULL
- Day 2: Both have data → Row created with both sources coalesced
- Day 3: FPD has data, DCM missing → Row created with FPD data, DCM columns NULL

This approach enables:
- Complete data lineage visibility
- Data quality monitoring (gap detection)
- Flexible downstream aggregation

### 3. Package ID Normalization

**Problem**: Different systems use different IDs for the same campaign package.

**Solution**: Deterministic mapping applied uniformly across DCM and FPD:

```sql
CASE
  WHEN package_id = 'P3923KK' THEN 'P37K96P'
  WHEN package_id = 'P37K96T' THEN 'P37K96P'
  WHEN package_id = 'P37MLHQ' THEN 'P37K96P'
  WHEN package_id = 'P37MLHP' THEN 'P37K96P'
  WHEN package_id = 'P37K96S' THEN 'P37K96P'
  WHEN package_id = 'P37DSDJ' THEN 'P37DSDR'
  ELSE package_id
END
```

This prevents double-counting and enables accurate cross-source joins.

### 4. Estimated Metrics Logic

**Estimated Impressions**:
```sql
CASE
  WHEN actual_impressions IS NULL THEN planned_impressions
  WHEN actual_impressions < 100 THEN planned_impressions  -- Threshold filter
  ELSE actual_impressions
END
```

**Estimated Cost**:
```sql
CASE
  WHEN actual_cost IS NULL THEN planned_cost
  ELSE actual_cost
END
```

**Rationale**: Very low impression counts (< 100) are treated as incomplete data and replaced with planned values.

---

## Current State

### Data Volumes

| Layer | Component | Rows | Update Frequency |
|-------|-----------|------|------------------|
| **Landing** | DCM Cost Model | 129,733 | Daily |
| | FPD Data | 1,620 | Daily (via R) |
| | Prisma Expanded | 666,155 | Daily |
| | TV Local Estimates | 352 | Daily (via R) |
| | TV National Estimates | 711 | Daily (via R) |
| **Staging** | Digital (view) | ~11,278 | Real-time |
| | TV Combined (view) | 1,063 | Real-time |
| **Mart** | TV Digital Unioned | 12,085 | Real-time |

### Data Coverage

**Digital**:
- **Records**: 11,278 (93.3% of total)
- **Advertisers**: 1 (Forevermark US)
- **Channels**: 9 (display, social, video, OOH, etc.)
- **Date Range**: 2025-09-23 to 2026-12-31
- **Estimated Spend**: $20.2M
- **Estimated Impressions**: 818M

**TV**:
- **Records**: 807 (6.7% of total)
- **Advertisers**: 6
- **Channels**: 1 (linear)
- **Date Range**: 2025-06-30 to 2026-03-23
- **Estimated Spend**: $22.1M
- **Estimated Impressions**: 643M

**Combined**:
- **Total Estimated Spend**: $42.3M
- **Total Estimated Impressions**: 1.46B

### Last Updated

All tables refreshed: **January 9, 2026**
- DCM: 17:03:09
- FPD: 17:21:32
- Prisma: 02:00:19
- TV Local: 17:13:57
- TV National: 17:14:01

---

## Known Issues

### Issue 1: Duplicate OOH Channel Logic
**Location**: `repo_mart.adif__tv_digital_unioned` (lines 7-9)

```sql
CASE
  WHEN REGEXP_CONTAINS(package_name, "ooh|OOH") THEN "OOH"
  WHEN REGEXP_CONTAINS(package_name, "ooh|OOH") THEN "OOH"  -- Duplicate!
  ELSE channel_group
END as channel
```

**Impact**: Minor (no functional issue, just redundant)

### Issue 2: Column Naming Inconsistency
**Location**: TV CTE uses `media_outlet as SITE` but output column is `supplier_code`

**Impact**: None (column aliasing works correctly in UNION)

---

## Usage Examples

### Query 1: Daily Spend by Channel
```sql
SELECT
  date,
  channel,
  SUM(estimated_cost) as total_spend,
  SUM(estimated_impressions) as total_impressions
FROM `looker-studio-pro-452620.repo_mart.adif__tv_digital_unioned`
WHERE advertiser = 'MASS'
GROUP BY 1, 2
ORDER BY 1, 2
```

### Query 2: Actual vs Planned Performance
```sql
SELECT
  package_name,
  SUM(planned_cost) as planned_spend,
  SUM(actual_cost) as actual_spend,
  SUM(actual_cost) / NULLIF(SUM(planned_cost), 0) - 1 as spend_variance_pct
FROM `looker-studio-pro-452620.repo_mart.adif__tv_digital_unioned`
WHERE type = 'digital'
  AND actual_cost IS NOT NULL
GROUP BY 1
HAVING SUM(planned_cost) > 0
ORDER BY 4 DESC
```

### Query 3: Data Source Coverage
```sql
SELECT
  date,
  COUNT(*) as total_packages,
  COUNTIF(actual_cost IS NOT NULL) as packages_with_actuals,
  COUNTIF(fpd_cost IS NOT NULL) as packages_with_fpd,
  COUNTIF(actual_cost IS NULL AND planned_cost IS NOT NULL) as planned_only
FROM `looker-studio-pro-452620.repo_mart.adif__tv_digital_unioned`
WHERE type = 'digital'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30
```

---

## ADIF Social Layer (Cross-Platform Source)

A lightweight staging layer is available to isolate ADIF social rows from the shared cross-platform delivery table.

### Source

`looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`

### Staging View Definition

`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/stg__adif__social_crossplatform.sql`

### Default Output View

`looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform`

### Inclusion Rules

- `account_name` must be one of:
  - `ADIF USA`
  - `A Diamond is Forever - US`
  - `A Diamond is Forever`
  - `De Beers Group`
- `campaign_name` must contain literal `WP_`.

### Notes

- Preserves source grain (daily ad-level rows) and all source metrics.
- Keeps lineage helpers (`layer_source_table`, `layer_row_key`) plus filter flags (`is_adif`, `is_social`).
- Designed as a staging layer while the final mart/table target remains configurable.

---

## Maintenance

### Daily Checks
1. Monitor R script execution logs for FPD and TV ingestion
2. Check for new package IDs requiring normalization mapping
3. Validate row counts in staging views

### Weekly Reviews
1. Review data quality metrics (NULL rates, orphan days)
2. Verify actual vs planned variance trends
3. Check for new Google Sheets requiring ingestion

### Monthly Tasks
1. Archive phase checkpoint CSVs
2. Update package ID normalization mappings if needed
3. Review and optimize BigQuery query costs

---

## Contact

For questions or issues with this pipeline, contact the data engineering team.

**Related Documentation**:
- [CLAUDE.md](../CLAUDE.md) - CodeViz research context
- [Google Sheets FPD Source](https://drive.google.com/drive/folders/1EyN93JE7v4OXjMMQREVuZ4ZN7xEed5WB)
- [TV Media Plan Sheet](https://docs.google.com/spreadsheets/d/1FLn_mge8Tz4wKqEmrIPQG_B2_MecS-9kNiQThQaXe_8)

**Last Updated**: January 9, 2026
