# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FPD (First Party Data) Loader — an R-based ETL pipeline that discovers partner marketing data from Google Sheets, normalizes heterogeneous column schemas, expands date-ranged records into daily granularity, and uploads to BigQuery for downstream analytics. Part of the ADIF (Adidas Data & Insight Framework) infrastructure.

## Commands

```bash
# Run the main FPD collection pipeline (all 7 phases + BigQuery upload)
Rscript util_collect_fpd_v3.r

# Run a specific phase only (edit lines 30-32 in the script):
#   use_saved_phases <- TRUE
#   current_phase <- 3        # Set to desired phase number
# Then run the script — it will load cached CSVs for other phases

# Run the manually-updated data loader (separate pipeline)
Rscript manually_updated_data_loader.r
```

### Dependencies

R packages: `googledrive`, `googlesheets4`, `dplyr`, `stringr`, `readr`, `lubridate`, `janitor`, `bigrquery`

### Authentication

- Google Drive/Sheets: OAuth via `googledrive` and `googlesheets4` (browser-based first run, then cached)
- BigQuery: Uses `bigrquery` with project `looker-studio-pro-452620`

## Architecture

Single-file linear pipeline (`util_collect_fpd_v3.r`, ~1,200 lines) with 7 sequential phases. Each phase writes a checkpoint CSV to `output/` enabling selective re-execution.

### Phase Flow

```
Google Drive folder (De Beers | Partner Data)
  → Phase 1: Discover sheets (recursive Drive scan, extract metadata)
  → Phase 2: Detect header rows (read A:G, find row with most non-empty cells ≥5)
  → Phase 3: Extract column names (read A:Y from header row, compute cross-sheet frequency)
  → Phase 4: Normalize column names (heuristic regex rules → canonical names)
  → Phase 5: Ingest & combine data (apply mapping, coerce types, filter zero-metric rows)
  → Phase 6: Clean & finalize dates (extract package_id, coalesce date fields, filter archives)
  → Phase 6.1: Sheet-level summary (aggregate metrics per source sheet)
  → Phase 7: Expand to daily rows (divide metrics evenly across date range days)
  → BigQuery upload (WRITE_TRUNCATE to landing.adif_fpd_data_ranged)
  → source() downstream script: util_process_updated_fpd.r
```

### Phase Checkpoint/Resume System

Controlled by two variables at the top of the script:
- `use_saved_phases` (TRUE/FALSE): when TRUE, loads cached CSV for phases other than `current_phase`
- `current_phase` (1-7): the phase to actively compute (all others use saved results)

This allows iterating on a single phase without re-running the full pipeline.

### Column Normalization Strategy (Phase 4)

The core challenge this pipeline solves is that each partner sheet uses different column names for the same data. Phase 4 applies a priority-ordered chain of regex rules:

1. **Drop** auto-generated columns (starting with `...`)
2. **Exact/strong matches** — `package_id`, `package/placement`, `placement`, `creative`, `package_name`
3. **Date fields** — `prisma_start_date`, `start_date`, `prisma_end_date`, `end_date`, `week`, `date`
4. **Metric fields** — `spend|cost|budget` → `spend`, `impress*` → `impressions`, `click*` → `clicks`, etc.
5. **Fallback** — `janitor::make_clean_names()` for anything unmatched

Order matters: more specific patterns (e.g., `completed_views`) are checked before general ones (e.g., `views`).

### Date Coalescing Logic (Phase 6)

Since sheets provide dates in different columns, the script derives canonical dates via coalesce chains. Prisma dates are planning-level (package-wide) and only used as a last resort when no granular date is available:
- `start_date_final` ← `start_date` → `week` → `month` → `date` → `end_date` → `prisma_start_date` → `prisma_end_date`
- `end_date_final` ← `end_date` → `week+6` → `month_end` → `start_date` → `date` → `prisma_end_date` → `prisma_start_date`

Each row also records `start_date_source` and `end_date_source` indicating which column won the coalesce. Phase 6.1 summarizes this per sheet, including `week_day_range` (e.g., "Sun-Sat") when the `week` column is the date source.

### Known KPI Metrics

The `known_kpi_metrics` vector (line 36) controls which columns are treated as numeric and split across days in Phase 7:

```
spend, impressions, sends, opens, views, completed_views,
engagements, clicks, pageviews, conversions, benchmark_metric, benchmark
```

Adding a new metric type requires updating this list. Dimension columns (non-metrics) are never split.

### Validation

Phase 7 includes a built-in cross-check: per-sheet KPI totals from Phase 5 are compared against Phase 7 expanded totals (tolerance: 1e-6) to verify no data loss during daily expansion.

## Key Configuration (lines 25-58)

| Variable | Purpose |
|----------|---------|
| `gdrive_folder_id` | Google Drive folder containing partner sheets |
| `pattern` | Sheet name pattern to match during discovery |
| `output_dir` | Path for checkpoint CSVs |
| `known_kpi_metrics` | Metric columns to treat as numeric and split across days |

## BigQuery Target

- Project: `looker-studio-pro-452620`
- Dataset: `landing`
- Table: `adif_fpd_data_ranged`
- Disposition: `WRITE_TRUNCATE` (full replace each run)

## Manually Updated Data Loader (`manually_updated_data_loader.r`)

Companion pipeline for manually corrected FPD figures. Reads package-level totals (`updated_FPD_IMPRESSIONS`, `updated_FPD_SPEND`) from a single Google Sheet, joins with Prisma date ranges, spreads metrics to daily rows, and uploads to `landing.manually_updated_fpd_daily`. Checkpoints written to `MUD_output/`. Uses Prisma dates exclusively (no partner date columns).

## Downstream

After BigQuery upload, `util_collect_fpd_v3.r` calls `source("util_process_updated_fpd.r")` (located at `/looker_personal/adif/util_process_updated_fpd.r`) which joins FPD data with Prisma date ranges and uploads to `adif_updated_fpd_daily`.
