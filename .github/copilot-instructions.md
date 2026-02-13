# Copilot Instructions for looker_personal

## Project Overview

This is a **media analytics data warehouse** combining multiple ad platforms (Basis, DCM, TikTok, Olipop) with UTM tracking and cost modeling, built on **BigQuery + Looker**. The codebase uses **SQL (BigQuery) for data transformation** and **R for data ingestion/automation**.

## Architecture: Data Flow Layers

The project follows a **staged data transformation architecture**:

```
Raw Data Sources (Google Sheets, APIs, DCM)
         ↓
Ingestion Layer (R scripts: util_collect_*.r)
         ↓ 
Landing BigQuery Tables (looker-studio-pro-452620.landing.*)
         ↓
Staging Layer (sql/base/stg__*.sql) - Views parsing UTMs, deduplication
         ↓
Staging Layer 2 (stg2__*.sql) - Joins & enrichment
         ↓
Marts Layer (sql/marts/delivery/, mft/, olipop/) - Business-ready aggregates
         ↓
Looker Models (omni/bigquery_connection_v2/) - Metrics & dashboards
```

### Key Components

- **`adif/util_collect_fpd.r`**: Master ingestion script for FPD (First Party Data) from Google Sheets. Handles multi-sheet parsing, date range expansion, metric validation, BigQuery upload.
- **`sql/base/basis/`**: Staging views for Basis data - UTM parsing, delivery data deduplication, union logic.
- **`sql/base/dcm/`**: DCM staging & cost models (version-tracked: v3, v5).
- **`sql/marts/`**: Business-ready aggregate tables - delivery metrics joined with UTM parameters and cost data.
- **`omni/bigquery_connection_v2/`**: Looker LookerML models exposing BigQuery tables as dimensions/measures.
- **`util/`**: Helper R functions, deduplication logic, data loaders, and exploratory utilities.

## Data Ingestion Workflows

### Basis FPD Collection (Primary Workflow)

**File**: `adif/util_collect_fpd.r`

**What it does**:
1. Scans Google Drive folder for sheets matching pattern "De Beers | Partner Data"
2. **Auto-detects header row** (looks for keywords: impressions, spend, clicks)
3. Cleans data: trim whitespace, remove $ and commas, convert dates (multiple formats supported)
4. Extracts package ID from package names using regex: `P[A-Za-z0-9]{6}`
5. **Date range expansion**: If sheet has `start_date`/`end_date`, divides metrics by `days_in_range` then unnests to individual dates
6. **Validation**: Compares pre- and post-transformation totals to ensure metrics aren't lost during division
7. Writes two tables to BigQuery:
   - `landing.adif_fpd_data` (raw with expanded dates)
   - `landing.adif_fpd_data_ranged` (original date ranges)
8. Also saves CSV backup and RDS cache

**Critical patterns**:
- Uses `find_header_row()` function with multi-keyword heuristics (metric > primary > secondary)
- Caches raw data to `.rds` file to avoid re-reading Google Sheets (expensive API calls)
- **Date handling is complex**: supports `date`, `week` (first Sunday), `start_date`/`end_date` columns with priority order
- Metrics validated include: spend, impressions, clicks, sends, opens, pageviews, views, completed_views

**Configuration** (top of script):
```r
gdrive_folder_id <- "1EyN93JE7v4OXjMMQREVuZ4ZN7xEed5WB"
pattern <- "De Beers | Partner Data"  # Filter pattern for sheet names
refresh_drive <- TRUE  # Set FALSE to use cached data
```

### Other R Ingestion Scripts

- **`util/mft_download_0519.py`**: Python script for downloading Media Flight Tracker data
- **`util/mft_bq-to-gcs.r`**: Exports MFT data from BigQuery to Google Cloud Storage
- **`util/mft_gsheet_update.r`**: Updates Google Sheet from BigQuery MFT data
- **`util/extract_creative_names.r`**: Extracts and cleans creative names from BigQuery

## SQL Transformation Patterns

### Staging Layer: UTM Parsing

**File**: `sql/base/basis/stg__basis__utms.sql`

**Pattern**: Parse URLs and extract/clean UTM parameters

```sql
LOWER(REPLACE(REGEXP_EXTRACT(name, r'^(?:\d+_)?([^_]+.*?)(?:\d+x\d+.*)?$'), ' ', ''))
  AS cleaned_creative_name
```

Removes numeric prefixes, size suffixes (e.g., _300x250), spaces, and lowercases. This cleaned name is the **join key** across components.

### Staging Layer 2: Complex Joins

**File**: `sql/base/basis/stg2__basis__plus_utms.sql`

Joins delivery data to parsed UTM data using:
- Creative name match (cleaned)
- Placement ID match
- Filters: meaningful traffic (impressions + clicks > 0), excludes GE campaigns

### Deduplication Pattern

**File**: `sql/base/dcm/mart__delivery__join_dcm_utms_prisma.sql`

Uses CTEs to progressively refine data:
1. Source raw data with metadata
2. Apply business logic filters
3. Join multiple platforms (DCM + Basis + Prisma)
4. Select final columns

## Project-Specific Conventions

### Naming Conventions

- **Tables**: `{stage}__{component}__{entity}` (e.g., `stg__basis__utms`, `mart__delivery__unified`)
- **Views**: Suffixed with `_view` or `_v{number}` for versions (e.g., `basis_utms_stg_view_2507`)
- **CTEs in SQL**: Use `as` (not `with...as`) and descriptive abbreviations (`del` for delivery, `utm` for utm)
- **R functions**: Snake_case (e.g., `find_header_row`, `write_to_bq`)

### SQL Versioning & Comments

Include header comments with:
- View/Table name and full project path
- Layer (STG, STG2, MART)
- Purpose and transformation logic
- Source tables and target usage
- Join keys and business filters
- Last edit date

Example:
```sql
/*=======================================================================================
  View:    looker-studio-pro-452620.repo_stg.basis_utms_stg_view_2507
  Layer:   STAGING
  Purpose: Parse UTM parameters from URLs into structured columns
  Source:  looker-studio-pro-452620.landing.basis_utms_unioned
  Join Key: id + cleaned_creative_name
  Last Edit: 2025-07-16
=======================================================================================*/
```

### R Coding Patterns

- **Error handling**: Wrap risky operations (API calls, BigQuery writes, file reads) in `tryCatch()`
- **Status messages**: Use `cat()` with flush to show progress; prefix with `✓` (success) or `✗` (error) or `⚠` (warning)
- **Data validation**: After major transformations, print row counts and metric totals
- **Comments**: Use `#### Section Name ####` for major code blocks

Example:
```r
tryCatch({
  result <- read_sheet(...)
  cat("✓ Successfully read", nrow(result), "rows\n")
  result
}, error = function(e) {
  cat("✗ ERROR reading sheet:", e$message, "\n")
  NULL
})
```

### BigQuery Project & Dataset Structure

- **Project**: `looker-studio-pro-452620`
- **Datasets**:
  - `landing`: Raw ingested data
  - `repo_stg`: Staging views (stg__*, stg2__*, etc.)
  - `data_model_2025`: Looker-exposed views (basis_gsheet_table, dcm_test2, etc.)
  - `DCM`: DCM-specific cost models and analyses
  - `repo_tiktok`: TikTok data

## Development Workflows

### Adding a New Data Source

1. **Create R ingestion script** in `adif/` or `util/` following the pattern in `util_collect_fpd.r`:
   - Source data from API or Google Sheets
   - Clean and validate data locally
   - Write to `landing.*` BigQuery table

2. **Create staging view** in `sql/base/{component}/stg__{component}__{entity}.sql`:
   - Parse/extract key fields (e.g., cleaned creative names)
   - Deduplicate if needed
   - Add header comments

3. **Create stage-2 join** if linking to existing data:
   - Reference `stg2__basis__plus_utms.sql` pattern
   - Use cleaned names as join keys

4. **Create mart table** in `sql/marts/{domain}/` for final aggregation

5. **Update Looker models** in `omni/bigquery_connection_v2/`:
   - Create `.view.yaml` files exposing dimensions/measures
   - Link to BigQuery table

### Running Scripts

- **R scripts**: Execute in RStudio or terminal via `Rscript /path/script.r`
- **SQL views**: Deployed to BigQuery on file save or via manual execution
- **Scheduling**: Cron jobs configured in `util/R_functions/add_cron_job.R` (daily at 09:00 UTC)

### Testing & Validation

- **Unit tests**: R script `sql/base/basis/test__basis__duplicateDetector.r` detects duplicate records
- **Row count validation**: Compare input vs. output rows after major transformations (see `util_collect_fpd.r`)
- **Metric totals**: When expanding date ranges, validate that sum(daily_metrics) = original metric
- **Data quality checks**: Look for unexpected NAs, negative values, or outliers in key metrics

## Key Files Reference

| File | Purpose |
|------|---------|
| `adif/util_collect_fpd.r` | Master Basis FPD ingestion |
| `sql/base/basis/stg__basis__utms.sql` | Parse UTM parameters from URLs |
| `sql/base/basis/stg2__basis__plus_utms.sql` | Join delivery + UTM data |
| `sql/base/dcm/mart__delivery__join_dcm_utms_prisma.sql` | Multi-platform mart (DCM, Basis, Prisma) |
| `omni/bigquery_connection_v2/model.yaml` | Looker model configuration |
| `util/util__deduplication_audit.sql` | Check for duplicates in staging |
| `util/R_functions/` | Reusable R helper functions |

## Common Tasks & Commands

**Pull data from Google Sheets to BigQuery**:
```r
source("adif/util_collect_fpd.r")  # Runs entire workflow
```

**Test staging views** (run in BigQuery console):
```sql
SELECT COUNT(*) FROM `looker-studio-pro-452620.repo_stg.basis_utms_stg_view_2507`;
```

**Check for duplicates**:
```sql
SELECT package_name, COUNT(*) 
FROM `looker-studio-pro-452620.repo_stg.basis_delivery`
GROUP BY package_name
HAVING COUNT(*) > 1;
```

**Validate metric totals after date expansion**:
See validation logic in `adif/util_collect_fpd.r` lines 520-580 (compares input vs. output sums)

## Integration Points & External Dependencies

- **Google Sheets API**: Used via `googlesheets4` R package (auth via `.Renviron` or interactive OAuth)
- **Google Drive API**: Used via `googledrive` R package
- **BigQuery**: Connected via `bigrquery` R package; project ID hardcoded in scripts
- **Looker**: LookerML models in `omni/` reference BigQuery tables directly
- **Ad Platforms**: DCM, TikTok, Olipop data assumed pre-loaded in BigQuery

## Documentation References

- See `CLAUDE.md` for detailed component analyses (Basis, Google Sheets, Upstream Data Sources)
- Large Basis staging script has inline comments explaining header detection, date handling, validation
