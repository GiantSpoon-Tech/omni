# Simplified FPD Ingestion Script Rewrite

## Overview
Rewrite `util_collect_fpd.r` from scratch as a straightforward, linear script with minimal abstractions. Build step-by-step toward 4-phase goal: discover files → identify data tables → collect column metadata → combine all data.

**Format**: Detailed section headers + inline comments. No helper functions (use base R / dplyr operations directly). Each major section writes intermediate checkpoint files (CSV) for inspection.

---

## Phase 1: Google Drive Discovery

**Goal**: Find all spreadsheets matching "De Beers | Partner Data" pattern in Google Drive folder.

**What happens**:
- Use `drive_ls()` to recursively search folder `1EyN93JE7v4OXjMMQREVuZ4ZN7xEed5WB`
- Pattern: `"De Beers | Partner Data"` (case-sensitive partial match on sheet name)
- Type: spreadsheet files only
- Result: Dataframe with columns `id`, `name`, `path` (sheet metadata)

**Checkpoint output**: Save to CSV with discovered files, sheet names, URLs.

---

## Phase 2: Header Row Detection (Per-File)

**Goal**: For each discovered file, identify where the data table actually starts (header row may vary across files).

**What happens**:
1. Read 'data' tab from each sheet (columns A:G only, for scanning)
2. Find the **first row with any text** — this is the header row
3. Once header row is identified, re-read the 'data' tab from that row starting at column A extending to column S (full table width)

**Simple logic**:
- Loop through rows in A:G scan
- First row with ≥1 non-empty cell = header row
- Return header row number

**Checkpoint output**: CSV with sheet name, URL, detected header row number.

---

## Phase 3: Column Header Ingestion & Metadata Collection

**Goal**: Extract all column headers from the 'data' tab of each file, store raw headers + sheet context (no normalization yet).

**What happens**:
1. For each file discovered in Phase 1:
   - Read the 'data' tab using the header row detected in Phase 2
   - Extract column names as-is (raw, without cleaning)
   - Create record: `sheet_name`, `sheet_url`, `column_name`, `column_position`
2. Combine all headers into single metadata dataframe
3. Identify unique column names and their frequency across sheets (helps spot naming variants)

**Checkpoint output**: CSV with all collected headers (`sheet_name`, `sheet_url`, `column_name`, `column_position`, `frequency_across_sheets`).

**Notes**:
- This phase is **informational** — prepares data for Phase 4 normalization
- Look for variants like: `package_name` vs `package`, date column aliases (`week`, `week_first_sunday`, `start_date`, `end_date`, `date`), metric naming variations

---

## Phase 4: Column Normalization Mapping

**Goal**: Build a normalization guide to standardize column names across files.

**What happens**:
1. Review Phase 3 checkpoint CSV to identify naming patterns
2. Build **explicit mapping** from raw column names → normalized names:
   - **Package names**: `package_placement_please_use_gs_placement_package_names_see_col_y` → `package_name`; `package` → `package_name`
   - **Package IDs**: Extract via regex `"\\|\\s*(P[A-Za-z0-9]{6})_"` from `package_name` → new column `package_id`
   - **Date columns**: Standardize all date variants to: `date_final` (single date or date range end), `start_date`, `end_date`
   - **Metrics**: Standardize `spend`, `impressions`, `clicks`, `sends`, `opens`, `pageviews`, `views`, `completed_views` (handle any case/underscore variants)
3. Store mapping as a list or dataframe (e.g., `norm_map` with columns `raw_name`, `normalized_name`, `sheet_name`)

**Checkpoint output**: CSV with normalization mapping (raw → normalized column names per sheet).

---

## Phase 5: Data Ingestion & Combination

**Goal**: Loop through each file, read data starting at detected header row, apply column normalization, combine into single master dataframe.

**What happens**:
1. For each file in Phase 1 discovery:
   - Read 'data' tab starting at header row (detected in Phase 2)
   - Apply column normalization mapping from Phase 4
   - Add metadata columns: `source_file`, `source_url`, `row_import_order`
   - Clean numeric columns: remove `$`, `,`, trim whitespace, convert to numeric
   - Clean date columns: convert to `Date` class (multiple format support)
   - Remove completely empty rows
2. Combine all files into single master dataframe via `bind_rows()`

**Checkpoint output**: CSV with combined master data (all rows, all normalized columns).

---

## Implementation Notes

### Range Specification
- **Current script**: Uses hardcoded `"e61:Q"` (row 61 start, columns E-Q)
- **Rewrite approach**: Start with `"A:Z"` (broader) and let header detection handle variable row positions
- **Question**: Keep hardcoded range or switch to broader auto-detection?

### Caching Strategy
- **Current script**: Maintains RDS cache (`adif_fpd_raw.rds`) to avoid expensive re-reads
- **Rewrite approach for v1**: Omit caching (add as optimization later with `refresh_drive` flag)
- **Question**: Skip caching for initial rewrite, or maintain it?

### Error Handling
- **Verbosity**: Keep current try-catch messages (file name + error description) or simplify to just skip failed files?
- **Approach**: Log failures to console + continue with remaining files (fail gracefully)

### Metrics & Validation
- **Defer to Phase 6** (future work): Date range expansion, metric validation (input vs. output totals), BigQuery upload
- **Focus for v1**: Just ingest and combine data

---

## File Outputs (Checkpoints)

1. **phase1_discovered_files.csv** — Sheet names, URLs, IDs
2. **phase2_header_detection.csv** — Detected header row per file
3. **phase3_raw_headers.csv** — All raw column names + metadata
4. **phase4_normalization_mapping.csv** — Raw → normalized column mapping
5. **phase5_combined_master_data.csv** — Final combined dataframe

---

## Future Phases (Not Included in This Rewrite)

- **Phase 6**: Date range expansion (divide metrics by days_in_range, unnest dates)
- **Phase 7**: Metric validation (input vs. output totals)
- **Phase 8**: BigQuery upload (landing.adif_fpd_data, landing.adif_fpd_data_ranged)
- **Optimization**: Add caching (RDS) to avoid re-reading Google Sheets
