# FPD Loader (First Party Data)

R-based ETL pipelines that collect partner marketing data from Google Sheets, normalize heterogeneous column schemas, and expand date-ranged records into daily granularity for BigQuery. Part of the ADIF (Adidas Data & Insight Framework) infrastructure.

This directory contains two pipelines:

| Script | Purpose | BigQuery Target |
|---|---|---|
| `adif/util_collect_fpd_v2.r` | Ingest raw partner data from ADIF sheets (from Google Drive folder) | `landing.adif_fpd_data_ranged` |
| `adif/util_collect_fpd_v3.r` | Ingest raw partner data from many sheets (from Google Drive Shortcuts folder) | `landing.fpd_data_ranged` |
| `manually_updated_data_loader.r` | Process manually corrected/updated FPD figures from a single sheet | `landing.manually_updated_fpd_daily` |

## Quick Start

```bash
# Install R packages (one-time)
Rscript -e 'install.packages(c("googledrive", "googlesheets4", "dplyr", "stringr", "readr", "lubridate", "janitor", "bigrquery", "tidyr"))'

# Run the main FPD collection pipeline
Rscript util_collect_fpd_v3.r

# Run the manually-updated data loader
Rscript manually_updated_data_loader.r
```

On first run, a browser window will open for Google OAuth. Subsequent runs use the cached token.

## How It Works

The pipeline runs 7 sequential phases. Each phase writes a checkpoint CSV to `output/`, so you can inspect intermediate results and re-run individual phases without starting from scratch.

```
Google Drive (partner sheets)
  Phase 1  ──>  Discover sheets
  Phase 2  ──>  Detect header rows
  Phase 3  ──>  Extract column names
  Phase 4  ──>  Build normalization mapping
  Phase 5  ──>  Ingest & combine all data
  Phase 6  ──>  Clean dates, extract IDs, filter
  Phase 6.1 ──>  Sheet-level summary with diagnostics
  Phase 7  ──>  Expand date ranges to daily rows
     │
     ▼
  BigQuery (landing.adif_fpd_data_ranged)
     │
     ▼
  util_process_updated_fpd.r (downstream processing)
```

## Configuration

Edit the top of `util_collect_fpd_v3.r` (lines 25-58):

| Variable | Purpose | Default |
|---|---|---|
| `gdrive_folder_id` | Google Drive folder ID containing partner sheets | `"1cMkgbplZ8sPIsDHluIuIEOmbam-BhZtY"` |
| `pattern` | Sheet name pattern to match during discovery | `"De Beers \| Partner Data"` |
| `output_dir` | Directory for checkpoint CSVs | `FPD_loader/output` |
| `use_saved_phases` | When `TRUE`, load cached CSVs for phases other than `current_phase` | `FALSE` |
| `current_phase` | The phase to actively compute (1-7) | `1` |
| `known_kpi_metrics` | Metric columns to treat as numeric and split across days in Phase 7 | See below |

### KPI Metrics List

```r
known_kpi_metrics <- c(
  "spend", "impressions", "sends", "opens", "views", "completed_views",
  "engagements", "clicks", "pageviews", "conversions", "benchmark_metric", "benchmark"
)
```

If a new partner includes a metric column not in this list, add it here. Only columns in this list are coerced to numeric and divided during daily expansion.

## Inputs

### Google Sheets

The pipeline reads from a Google Drive folder containing spreadsheets that match the `pattern` name. Each sheet must have:

- A tab named **`data`**
- A header row with **5+ non-empty columns** (auto-detected, does not need to be row 1)
- Data in columns **A through Y**

Partner sheets can use different column names — the pipeline normalizes them (see Phase 4).

### Authentication

- **Google Drive/Sheets**: OAuth via `googledrive` and `googlesheets4`. First run opens a browser for consent; token is cached for subsequent runs.
- **BigQuery**: Uses `bigrquery` with project `looker-studio-pro-452620`.

## Outputs

All checkpoint CSVs are written to `output/`.

### `phase1_discovered_files.csv`

**Purpose**: Inventory of all partner sheets found in the Drive folder.

**Key columns**: `sheet_id`, `sheet_name`, `sheet_url`, `last_modified_time`, `last_modified_by`

**How to use**: Review to confirm expected partner sheets are present. Sheets with "ARCHIVE" in the name are automatically excluded.

---

### `phase2_header_detection.csv`

**Purpose**: Records where the data table starts in each sheet (which row is the header).

**Key columns**: `sheet_name`, `header_row`, `status`

**How to use**: Check `status` column — should be `"success"` for all active sheets. If a sheet shows `"no_header_found"` or `"empty_sheet"`, the sheet may have changed format or been emptied. The `header_row` number tells you exactly which row the pipeline treats as column headers.

---

### `phase3_raw_headers.csv`

**Purpose**: Catalog of every raw column name found across all sheets, with frequency counts.

**Key columns**: `column_name_raw`, `sheet_name`, `frequency_across_sheets`

**How to use**: Check this when a new partner sheet is added. High-frequency column names (appearing in many sheets) are reliably normalized. Low-frequency names may need new normalization rules in Phase 4.

---

### `phase4_normalization_mapping.csv`

**Purpose**: The complete mapping from raw column names to normalized canonical names, including which rule matched.

**Key columns**: `column_name_raw`, `normalized_name`, `rule_applied`, `frequency_across_sheets`

**How to use**: This is the reference for how column names are translated. Review `rule_applied` to understand why a column was mapped a certain way. Rules include `match_spend`, `match_impressions`, `drop_autogenerated`, `fallback_janitor`, etc. If a column is mapped incorrectly, adjust the regex rules in the Phase 4 section of the script (lines ~460-510).

---

### `phase5_combined_master_data.csv`

**Purpose**: Raw combined data from all sheets after column normalization and type coercion, before any date logic or filtering.

**Key columns**: All normalized data columns plus metadata (`source_file`, `source_url`, `last_modified_time`, `last_modified_by`, `partner_sheet`)

**How to use**: This is the "truth" dataset before cleaning. Use it to:
- Verify metric totals per partner (cross-reference with original sheets)
- Debug issues where data seems missing after Phase 6 filtering
- Compare against Phase 7 during validation

---

### `phase6_cleaned_master_data.csv`

**Purpose**: Cleaned data with standardized dates, extracted package IDs, and diagnostic columns showing which date source was used per row.

**Key columns** (in addition to Phase 5 columns):
- `start_date_final`, `end_date_final` — the canonical date range for each row
- `start_date_source`, `end_date_source` — which input column was used (e.g., `"week"`, `"start_date"`, `"prisma_start_date"`)
- `package_id` — extracted from `package_name` when not provided directly

**Rows removed by Phase 6**:
1. Rows where `source_file` contains "archive" (case-insensitive)
2. Rows where the sum of all numeric columns is 0 or NA
3. Rows where `package_name` is NA or blank

**How to use**: Check `start_date_source` and `end_date_source` to understand how dates were resolved. If a partner's data is using `prisma_start_date` as the source, it means no granular date column was available — the data will expand over the full Prisma planning range rather than weekly/monthly.

**Date coalesce precedence** (Prisma dates are last resort):
- `start_date_final`: `start_date` > `week` > `month` > `date` > `end_date` > `prisma_start_date` > `prisma_end_date`
- `end_date_final`: `end_date` > `week+6` > `month_end` > `start_date` > `date` > `prisma_end_date` > `prisma_start_date`

---

### `phase6_1_sheet_summary.csv`

**Purpose**: One-row-per-sheet summary with aggregated metrics and date source diagnostics.

**Key columns**:
- `partner_name`, `source_file`, `source_url`, `last_modified_time`, `last_modified_by`
- `row_count`, `package_count`
- `date_range_start`, `date_range_end`
- `start_date_sources` — which date columns are driving `start_date_final` for this sheet (e.g., `"week"` or `"start_date"`)
- `end_date_sources` — same for `end_date_final`
- `week_start_dates_used` — if `week` is a date source, lists the distinct week start dates (NA otherwise)
- `week_day_range` — if `week` is a date source, shows the day-of-week range (e.g., `"Sun-Sat"`, `"Mon-Sun"`)
- `spend_sum`, `impressions_sum`, `clicks_sum`, etc. — total KPI metrics per sheet

**How to use**: This is the primary diagnostic file. Check it to:
- Verify row counts and metric totals per partner
- See which date column drives each partner's data (are they weekly? monthly? have explicit ranges?)
- Confirm `week_day_range` makes sense for weekly partners (e.g., `"Sun-Sat"` means the partner reports weeks starting Sunday)
- Spot partners whose dates fall back to Prisma (indicates missing granular dates)

---

### `phase7_daily_master_data.csv`

**Purpose**: Final output — every row represents a single day. Date-ranged records are expanded, and metrics are divided evenly across the days in the range.

**Key columns** (in addition to Phase 6 columns):
- `date_final` — the specific date this row represents
- `data_update_datetime` — timestamp of when this pipeline run was executed
- All KPI metric columns now contain **daily values** (original value / number of days in range)
- `impressions` and `clicks` are rounded to whole-number integers in final output files

**How to use**: This is what gets uploaded to BigQuery. You can query by `date_final` for time-series analysis without worrying about mixed granularity. Cross-reference metric totals with Phase 5 via `phase7_validation_table.csv` to see any differences and the associated Phase 6 filter reasons.

---

### `phase6_filter_audit.csv`

**Purpose**: Filter-level audit from Phase 6 showing which rows were removed by each filter rule and the metric impact of those removals.

**Key columns**:
- `source_file` — partner sheet name
- `filter_reason` — filter code (`archive_source_file`, `numeric_row_metric_sum_zero_or_na`, `missing_package_name`)
- `removed_rows` — number of rows removed by that filter for that source
- `removed_spend`, `removed_impressions`, `removed_clicks`, etc. — summed KPI impact for removed rows

**How to use**: Use this file to explain why per-sheet totals can differ between Phase 5 and Phase 7. This is the source used to populate the `filter_reason` column in `phase7_validation_table.csv`.

---

### `phase7_validation_table.csv`

**Purpose**: Per-sheet validation table comparing KPI totals between Phase 5 and Phase 7.

**Key columns**:
- `source_file`
- `*_diff` columns (e.g., `spend_diff`, `impressions_diff`, `clicks_diff`)
- `mismatch_any` — whether any KPI diff exceeds tolerance
- `filter_reason` — aggregated reason text derived from `phase6_filter_audit.csv` (for example: `missing_package_name(rows=99, impressions=7280286)`)

**How to use**: This is the primary reconciliation output. It is saved to `output/` and printed at the end of the pipeline run so the mismatch table appears after upload/completion logs.

## Running Individual Phases

To iterate on a specific phase without re-running the full pipeline:

```r
# At the top of the script, set:
use_saved_phases <- TRUE
current_phase <- 4        # Only Phase 4 will recompute; others load from saved CSVs
```

Then run the script. All phases except `current_phase` will load their cached CSV from `output/`.

This is useful for:
- Debugging a specific phase
- Adjusting normalization rules (Phase 4) and re-running from there
- Re-running date logic (Phase 6) after fixing coalesce precedence

## BigQuery Target

| Field | Value |
|---|---|
| Project | `looker-studio-pro-452620` |
| Dataset | `landing` |
| Table | `adif_fpd_data_ranged` |
| Write mode | `WRITE_TRUNCATE` (full replace each run) |

After upload, the script calls `source("util_process_updated_fpd.r")` for downstream processing with Prisma date ranges.

---

## Manually Updated Data Loader (`manually_updated_data_loader.r`)

A companion pipeline for processing **manually corrected or updated FPD figures**. While the main pipeline auto-discovers and ingests raw partner data from many sheets, this script reads from a single curated Google Sheet containing package-level totals that have been manually reviewed and adjusted.

### When to Use

Use this when:
- Partner data has been manually corrected after initial ingestion (e.g., spend adjustments, reconciliation)
- Updated impression or spend figures need to be loaded separately from the raw partner pipeline
- You need a clean, auditable path for manually-revised numbers

### How It Works

```
Google Sheet (manually updated FPD figures)
  Step 1  ──>  Read package-level records (package_id, impressions, spend)
  Step 2  ──>  Query Prisma in BigQuery for date ranges per package_id
  Step 3  ──>  Join packages with Prisma dates (left join)
  Step 4  ──>  Spread metrics evenly across Prisma date range → daily rows
  Step 5  ──>  Upload to BigQuery (landing.manually_updated_fpd_daily)
  Step 6  ──>  Print summary report
```

### Configuration

Edit the top of `manually_updated_data_loader.r`:

| Variable | Purpose | Default |
|---|---|---|
| `sheet_id` | Google Sheet with manually updated FPD data | `"1kUD8gVrHAAaZbULtFgDZl1hgGU-7Ut8fSdNJDgsZwfE"` |
| `target_gid` | Specific tab within the sheet | `"1894007924"` |
| `output_dir` | Directory for checkpoint CSVs | `util/data_loaders/FPD_loader/MUD_output` |
| `bq_table` | BigQuery table name | `"manually_updated_fpd_daily"` |

### Input Google Sheet

The sheet must contain these columns:
- `package_id` — must match Prisma package IDs for date range lookup
- `supplier_name`, `initiative`, `package_name` — dimension columns
- `updated_FPD_IMPRESSIONS` — corrected impression totals (package-level)
- `updated_FPD_SPEND` — corrected spend totals (package-level)

Rows with no `package_id` or where both metrics are NA are filtered out.

### Date Ranges

Unlike the main pipeline (which derives dates from partner-provided columns), this script **always uses Prisma dates**. It queries `prisma_expanded_full` in BigQuery for `MIN(date)` / `MAX(date)` per `package_id`, filtered to `advertiser_name = 'Forevermark US'` and `package_type != 'Child'`.

Packages without a matching Prisma date range are skipped with a warning.

### Outputs (`MUD_output/`)

| File | Purpose |
|---|---|
| `updated_fpd_packages.csv` | Package-level data as read from the sheet (after filtering) |
| `prisma_package_dates.csv` | Date ranges retrieved from Prisma per package |
| `updated_fpd_with_dates.csv` | Joined result — packages with their Prisma date ranges |
| `updated_fpd_daily.csv` | Final daily rows (metrics ÷ days in range) |
| `manually_updated_fpd_package_summary.csv` | Summary by package sorted by spend |

### BigQuery Target

| Field | Value |
|---|---|
| Project | `looker-studio-pro-452620` |
| Dataset | `landing` |
| Table | `manually_updated_fpd_daily` |
| Write mode | `WRITE_TRUNCATE` (full replace each run) |

### Differences from Main Pipeline

| | `util_collect_fpd_v3.r` | `manually_updated_data_loader.r` |
|---|---|---|
| **Input** | Many partner sheets (auto-discovered from Drive) | One curated Google Sheet |
| **Column handling** | Auto-detects headers, normalizes column names | Fixed schema (`package_id`, `updated_FPD_IMPRESSIONS`, `updated_FPD_SPEND`) |
| **Date source** | Coalesced from partner-provided dates (week, start_date, etc.) | Always from Prisma (`prisma_expanded_full`) |
| **Metrics** | Many (spend, impressions, clicks, views, etc.) | Two (`updated_FPD_IMPRESSIONS`, `updated_FPD_SPEND`) |
| **Phases** | 7 phases with checkpoint/resume | 5 linear steps |
| **BQ table** | `landing.adif_fpd_data_ranged` | `landing.manually_updated_fpd_daily` |

---

## Troubleshooting

### Google Auth Fails

**Symptom**: `Error in gargle::token_fetch()` or browser doesn't open.

**Fix**: Delete the cached token and re-authenticate:
```r
googledrive::drive_deauth()
googlesheets4::gs4_deauth()
# Then re-run — a new browser auth window will appear
```

### "No header found" for a Sheet

**Symptom**: Phase 2 reports `no_header_found` for a partner sheet.

**Cause**: The sheet's header row has fewer than 5 non-empty columns in columns A:G, or the tab is not named `data`.

**Fix**: Open the sheet and verify:
1. The tab is named exactly `data` (case-sensitive)
2. The header row has at least 5 columns with text in columns A through G
3. There aren't blank rows above the header pushing it past row 500

### Metric Totals Don't Match Between Phase 5 and Phase 7

**Symptom**: Phase 7 validation prints `"Detected differences in per-sheet KPI totals"`.

**Cause**: Rows were filtered out in Phase 6 (archive, zero-metric, or missing package_name) that were included in Phase 5.

**Where to debug quickly**:
- `output/phase7_validation_table.csv` — shows per-sheet diffs and `filter_reason`
- `output/phase6_filter_audit.csv` — shows row/metric impact per filter reason

**Fix**: This is expected when Phase 6 filters remove rows. The validation compares Phase 5 (pre-filter) with Phase 7 (post-filter). To verify the totals are correct, compare Phase 6 totals with Phase 7 instead:
```r
phase6 <- readr::read_csv("output/phase6_cleaned_master_data.csv")
phase7 <- readr::read_csv("output/phase7_daily_master_data.csv")
sum(phase6$spend, na.rm = TRUE)
sum(phase7$spend, na.rm = TRUE)
# These should match
```

### A Column Is Mapped to the Wrong Normalized Name

**Symptom**: A partner's "Budget" column is not being treated as `spend`, or a column name collides with another.

**Fix**: Check `phase4_normalization_mapping.csv` to see which rule matched. Then edit the Phase 4 regex rules in the script (~lines 460-510). Rules are checked in priority order — add specific matches above general ones.

### Dates Falling Back to Prisma

**Symptom**: `phase6_1_sheet_summary.csv` shows `start_date_sources = "prisma_start_date"` for a partner.

**Cause**: That partner's sheet has no `start_date`, `week`, `month`, or `date` column — only Prisma planning dates.

**Impact**: The data expands over the entire Prisma range (could be months), dividing metrics across many days. This may undercount daily values compared to what the partner actually delivered per week.

**Fix**: Ask the partner to add a `week` or `date` column to their reporting sheet. Alternatively, check if the data has a date column with an unrecognized name — if so, add a normalization rule in Phase 4.

### BigQuery Upload Fails

**Symptom**: `"ERROR writing to BigQuery"` in console output.

**Possible causes**:
- Authentication expired — run `bigrquery::bq_auth()` to refresh
- Schema mismatch — the table schema changed (new columns or type changes). The `WRITE_TRUNCATE` mode recreates the table, but the preceding `bq_table_delete` might fail on permissions
- Network timeout — retry the run

## Possible Improvements

- **Parameterize the script via CLI args** — currently `gdrive_folder_id`, `pattern`, `current_phase`, and `use_saved_phases` require editing the script. These could be `commandArgs()` or an `optparse` setup.
- **Week alignment** — the `week` column is used as-is (the raw date from the partner). If partners report inconsistent week-start days (some Sunday, some Monday), consider snapping to ISO week start with `lubridate::floor_date(week, "week", week_start = 1)`.
- **Parallel sheet processing** — Phases 2, 3, and 5 loop through sheets sequentially. Each iteration is an API call. `furrr::future_map()` or `parallel::mclapply()` could speed this up.
- **Incremental loading** — the pipeline currently does a full `WRITE_TRUNCATE` to BigQuery. An incremental approach (only process sheets modified since last run, using `last_modified_time`) would reduce API calls and runtime.
- **Weighted daily expansion** — Phase 7 divides metrics evenly across days. Some metrics (e.g., TV ad spend) may follow day-of-week patterns. A weighted split (heavier on weekdays, lighter on weekends) could improve accuracy.
- **Unit tests** — no test suite exists. Key functions to test: date parsing (`parse_any_date`), column normalization (Phase 4 rules), and daily expansion math (Phase 7).
- **Config file** — move `known_kpi_metrics`, `gdrive_folder_id`, and other settings to a YAML or JSON config file so the script doesn't need to be edited directly.
- **TODO: Create a Codex skill for input standardization** — package the Excel-to-standard-input conversion workflow (header detection, weekly row extraction, canonical column mapping, and output validation) into a reusable skill for manual partner report ingestion.
