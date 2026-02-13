# MFT Data Pipeline

Complete documentation of the MFT (MassMutual Full-funnel Tracking) data pipeline that unifies DCM and Basis programmatic advertising data with UTM parameter enrichment.

The final reporting endpoint is `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`.

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              BASE DATA SOURCES                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│ DCM: DCM.20250505_costModel_v5                                                 │
│ BASIS: giant-spoon-299605.data_model_2025.basis_master2                       │
│ UTM: mm_utms_snapshot, b_sup_pivt_unioned_tab, dcm_plus_utms_upload           │
└─────────────────────────────────────┬───────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            STAGING LAYER (Views)                               │
├─────────────────────────────────┬───────────────────────────────────────────────┤
│ DCM branch                      │ Basis branch                                  │
│ final_views.dcm                 │ repo_stg.basis_delivery                       │
│ final_views.utms_view           │ repo_stg.basis_plus_utms_v4_PnS_table         │
│ repo_stg.dcm_plus_utms          │                                               │
└────────────────┬────────────────┴────────────────┬───────────────────────────────┘
                 │                                  │
                 └──────────────┬───────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              MART LAYER                                        │
│                    repo_mart.mft_view (unioned mart view)                      │
└─────────────────────────────────────┬───────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    SCHEDULED QUERY SCRIPT (2-STEP BUILD)                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Step 1: CREATE OR REPLACE mass_mutual_mft_ext.mft_data_current                 │
│         FROM repo_mart.mft_view (aggregated by date/campaign/UTMs/placement)   │
│                                                                                 │
│ Step 2: CREATE OR REPLACE mass_mutual_mft_ext.mft_data                         │
│         = mft_data_current                                                      │
│           UNION ALL                                                             │
│           historical backfill from landing.mft (pre-cutover dates)             │
└─────────────────────────────────────┬───────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       FINAL REPORTING ENDPOINT                                 │
│             looker-studio-pro-452620.mass_mutual_mft_ext.mft_data              │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Mermaid Diagram (Full Pipeline)

```mermaid
flowchart TD
    %% Sources
    subgraph S["0) Base Data Sources"]
        DCM0["DCM.20250505_costModel_v5<br/>Raw DCM delivery + cost model fields"]
        BASIS0["giant-spoon-299605.data_model_2025.basis_master2<br/>Raw Basis delivery"]
        UTM1["mm_utms_snapshot<br/>Primary DCM UTM reference"]
        UTM2["utm_scrap.b_sup_pivt_unioned_tab<br/>Basis trafficking-sheet UTM extracts"]
        UTM3["repo_stg.dcm_plus_utms_upload<br/>Manual UTM corrections"]
        HIST0["landing.mft<br/>Historical pre-cutover data"]
    end

    %% DCM staging path
    subgraph D["1) DCM Staging Branch"]
        D1["Step 1.1: final_views.dcm<br/>Pass-through view of DCM cost model"]
        D2["Step 1.2: final_views.utms_view<br/>Filtered UTM snapshot (excludes invalid values)"]
        D3["Step 1.3: repo_stg.dcm_plus_utms<br/>LEFT JOIN DCM to UTMs on ad/ad_name<br/>Exact-row dedupe with ROW_NUMBER"]
    end

    %% Basis staging path
    subgraph B["2) Basis Staging Branch"]
        B1["Step 2.1: repo_stg.basis_delivery<br/>Extract CP id, normalize creative name, build del_key"]
        B2["Step 2.2: basis UTM prep<br/>Parse URL UTMs + normalize creative to match delivery"]
        B3["Step 2.3: combined UTM set<br/>UNION DISTINCT of trafficking-sheet UTMs + manual uploads"]
        B4["Step 2.4: repo_stg.basis_plus_utms_v4_PnS_table<br/>FULL JOIN delivery to UTM set on composite key<br/>Deduplicate by date + master_key"]
    end

    %% Mart
    subgraph M["3) Mart Layer"]
        M1["Step 3.1: repo_mart.mft_view<br/>UNION ALL of DCM and Basis branches<br/>Applies campaign/date/impression filters"]
    end

    %% Scheduled endpoint build
    subgraph Q["4) Scheduled Query Script (DTS Run)"]
        Q1["Step 4.1: mass_mutual_mft_ext.mft_data_current<br/>CREATE OR REPLACE from repo_mart.mft_view<br/>Aggregate metrics by date/campaign/partner/placement/UTMs"]
        Q2["Step 4.2: mass_mutual_mft_ext.mft_data<br/>CREATE OR REPLACE final endpoint<br/>Union current table with historical landing.mft slice"]
    end

    %% Consumption
    subgraph C["5) Consumption"]
        C1["Looker Studio and downstream reporting"]
    end

    %% Edges: source -> dcm
    DCM0 --> D1
    UTM1 --> D2
    D1 --> D3
    D2 --> D3

    %% Edges: source -> basis
    BASIS0 --> B1
    UTM2 --> B2
    UTM3 --> B3
    B2 --> B3
    B1 --> B4
    B3 --> B4

    %% Edges: branch -> mart -> endpoint
    D3 --> M1
    B4 --> M1
    M1 --> Q1
    HIST0 --> Q2
    Q1 --> Q2
    Q2 --> C1
```

## Table of Contents

- [Data Sources](#data-sources)
- [Staging Layer](#staging-layer)
- [Mart and Endpoint Layer](#mart-and-endpoint-layer)
- [UTM Processing Scripts](#utm-processing-scripts)
- [Key Concepts](#key-concepts)
- [Current State](#current-state)
- [Usage Examples](#usage-examples)
- [Safe Query Guardrails](#safe-query-guardrails)
- [Offline Sheet Daily Sync](#offline-sheet-daily-sync)
- [Changelog Policy](#changelog-policy)

---

## Data Sources

### DCM Data

#### DCM Cost Model
**Table**: `looker-studio-pro-452620.DCM.20250505_costModel_v5`

| Attribute | Value |
|-----------|-------|
| **Rows** | 129,733 |
| **Purpose** | DoubleClick Campaign Manager delivery data with cost modeling |
| **Update Frequency** | Daily |
| **Shared With** | ADIF pipeline |

**Key Fields**:
- `date` - Delivery date
- `campaign` - Campaign name
- `package_roadblock` - Package/roadblock identifier
- `placement_id` - Unique placement identifier
- `ad` - Ad name (used for UTM join)
- `creative` - Creative name
- `impressions` - Ad impressions delivered
- `clicks` - Click-through events
- `media_cost` - Raw media cost
- `daily_recalculated_cost` - Normalized daily cost (used in final output)

---

### Basis Data

#### Basis Master (Delivery)
**Table**: `giant-spoon-299605.data_model_2025.basis_master2`

| Attribute | Value |
|-----------|-------|
| **Rows** | 132,915 |
| **Purpose** | Programmatic ad delivery from Basis DSP |
| **Update Frequency** | Daily |
| **Last Updated** | Jan 12, 2026 (12:01) |

**Key Fields**:
- `date` - Delivery date
- `campaign` - Campaign name
- `package_roadblock` - Package identifier
- `tactic` - Media tactic
- `placement` - Placement name (includes CP_XXXXX ID)
- `creative_name` - Creative asset name
- `impressions` - Ad impressions
- `clicks` - Click events
- `media_cost` - Spend

**Note**: Also accessible via `looker-studio-pro-452620.landing.basis_master` (160,638 rows - includes additional campaigns)

---

### UTM Metadata Sources

#### 1. MM UTMs Snapshot (Primary DCM UTMs)
**Table**: `giant-spoon-299605.data_model_2025.mm_utms_snapshot`

| Attribute | Value |
|-----------|-------|
| **Rows** | 11,373 |
| **Purpose** | Primary UTM reference for DCM placements |
| **Last Updated** | Jan 12, 2026 (12:04) |

**Key Fields**:
- `Campaign` - Campaign name
- `Site_Name` - Publisher/site
- `Package_Name` - Package identifier
- `Placement_Name` - Placement name
- `Ad_Name` - Ad name (join key to DCM)
- `_UTM_Source` - Traffic source tag
- `_UTM_Medium` - Marketing medium tag
- `_UTM_Campaign` - Campaign tag
- `_UTM_Content` - Content variation tag
- `_UTM_Term` - Search term tag

---

#### 2. Basis UTM Pivot Table
**Table**: `looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab`

| Attribute | Value |
|-----------|-------|
| **Rows** | 1,616 |
| **Purpose** | UTM mappings extracted from Basis trafficking sheets |
| **Source** | Excel trafficking sheets via R script |

**Key Fields**:
- `tag_placement` - Placement tag name
- `name` - Creative name
- `url` - Full URL with UTM parameters
- Extracted UTM parameters via regex

**URL Parsing Logic**:
```sql
REGEXP_EXTRACT(url, 'utm_source=(.*?)&')  AS utm_source,
REGEXP_EXTRACT(url, 'utm_medium=(.*?)&')  AS utm_medium,
REGEXP_EXTRACT(url, 'utm_campaign=(.*?)&') AS utm_campaign,
REGEXP_EXTRACT(url, 'utm_term=(.*)')      AS utm_term,
REGEXP_EXTRACT(url, r'[?&]utm_content=([^&#]*)') AS utm_content
```

---

#### 3. DCM Plus UTMs Upload (Manual Corrections)
**Table**: `looker-studio-pro-452620.repo_stg.dcm_plus_utms_upload`

| Attribute | Value |
|-----------|-------|
| **Rows** | 97 |
| **Purpose** | Manual UTM corrections and additions |
| **Update** | As needed |

Used for edge cases where automated matching fails.

---

## Staging Layer

### DCM Branch

#### View: `final_views.dcm`
**Definition**: Simple pass-through from cost model
```sql
SELECT * FROM looker-studio-pro-452620.DCM.20250505_costModel_v5
```

#### View: `final_views.utms_view`
**Definition**: Filtered UTM snapshot
```sql
SELECT * FROM `looker-studio-pro-452620.landing.adswerve_utms`
```

#### View: `repo_stg.dcm_plus_utms`
**Purpose**: Enriches DCM delivery data with UTM parameters
**Local deploy SQL**: `scripts/sql/repo_stg__dcm_plus_utms.sql`

**Key Features**:
- Exact-key join first: `placement_id + creative_assignment`.
- Normalized fallback join when exact key misses:
  - scoped to campaigns `MassMutual20252026Media` and `MassMutualLVGP2025`
  - case-insensitive `campaign` + `placement_id`
  - creative normalization removes `px` suffix marker and lowercases before matching
- UTM fields use exact-first fallback (`COALESCE(exact, normalized)`).
- `TO_JSON_STRING`-based deduplication preserves one row per fully identical output record.

---

### Basis Branch

#### View: `repo_stg.basis_delivery`
**Location**: Defined in `sql/base/basis/stg__basis__delivery.sql`
**Purpose**: Adds join helper fields to basis_master2

**Key Transformations**:

1. **Placement ID Extraction**:
```sql
REGEXP_EXTRACT(placement, r'CP_(\d+)') AS id
```

2. **Creative Name Normalization** (for UTM matching):
```sql
LOWER(
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      LOWER(
        REPLACE(
          REGEXP_EXTRACT(
            creative_name,
            r'^(?:\d+_)?([^_]+.*?)(?:_\d+x\d+.*)?$'  -- strip prefix & size
          ),
          ' ', ''  -- remove spaces
        )
      ),
      r'(^peacock_|_peacock$)', ''  -- drop "peacock"
    ),
    r'[^a-zA-Z0-9]', ''  -- keep only alphanumeric
  )
) AS cleaned_creative_name
```

3. **Composite Join Key**:
```sql
CONCAT(
  LOWER(placement),
  " || ",
  cleaned_creative_name
) AS del_key
```

**Example Transformation**:
| Input | Output |
|-------|--------|
| `123_PEACOCK_Stay Ready_300x250_v2` | `stayready` |
| `CP_45678_MassMutual_Display` | `massmutual_display` → key: `cp_45678_massmutual_display \|\| stayready` |

---

#### View: `repo_stg.basis_plus_utms_v4_PnS_table`
**Purpose**: Joins Basis delivery with UTM parameters from multiple sources

**Full SQL Structure**:
```sql
WITH
-- 1. Delivery data (filtered)
del AS (
  SELECT *
  FROM `looker-studio-pro-452620.repo_stg.basis_delivery`
  WHERE campaign NOT LIKE '%GE%'
    AND campaign NOT LIKE 'Ritual%'
),

-- 2. UTM source from dcm_plus_utms_upload
utm4 AS (
  SELECT
    placement,
    -- Normalized creative name (same regex as basis_delivery)
    REGEXP_REPLACE(...) AS cleaned_creative_name_2,
    utm_source, utm_medium, utm_campaign, utm_content, utm_term,
    CONCAT(LOWER(placement), " || ", cleaned_creative_name_2) AS utm_utm_key
  FROM looker-studio-pro-452620.repo_stg.dcm_plus_utms_upload
),

-- 3. UTM source from trafficking sheets
utm1 AS (
  SELECT
    tag_placement AS placement,
    name AS creative_name,
    -- Extract UTMs from URL
    REGEXP_EXTRACT(url, 'utm_source=(.*?)&') AS utm_source,
    REGEXP_EXTRACT(url, 'utm_medium=(.*?)&') AS utm_medium,
    REGEXP_EXTRACT(url, 'utm_campaign=(.*?)&') AS utm_campaign,
    REGEXP_EXTRACT(url, 'utm_term=(.*)') AS utm_term,
    REGEXP_EXTRACT(url, r'[?&]utm_content=([^&#]*)') AS utm_content,
    -- Normalized creative + composite key
    ...
  FROM `looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab`
),

-- 4. Combined UTM reference (deduplicated)
utm AS (
  SELECT DISTINCT
    placement, cleaned_creative_name_2,
    utm_source, utm_medium, utm_campaign, utm_content, utm_term,
    CONCAT(LOWER(placement), " || ", cleaned_creative_name_2) AS utm_utm_key
  FROM utm1
  UNION DISTINCT
  SELECT * FROM utm4
),

-- 5. Full join delivery + UTMs
joined AS (
  SELECT
    del.*,
    utm.placement AS placement__utms,
    utm_source, utm_medium, utm_campaign, utm_term, utm_content,
    utm.utm_utm_key AS utm_key,
    COALESCE(del.del_key, utm_utm_key) AS master_key
  FROM del
  FULL JOIN utm ON del_key = utm.utm_utm_key
  WHERE campaign NOT LIKE '%GE%'
    AND campaign NOT LIKE 'Ritual%'
),

-- 6. Deduplicate by date + master_key
ranked AS (
  SELECT
    joined.*,
    ROW_NUMBER() OVER (
      PARTITION BY date, master_key
      ORDER BY placement
    ) AS rn
  FROM joined
)

SELECT * EXCEPT(rn, meta_data_date_pull, package, gmail_dt, meta_data_date_range)
FROM ranked
WHERE rn = 1
ORDER BY date DESC NULLS FIRST
```

**Key Features**:
- FULL JOIN captures UTM-only records (for validation)
- UNION DISTINCT combines multiple UTM sources
- Per-day deduplication ensures unique records
- Campaign filtering removes non-MassMutual data

---

## Mart and Endpoint Layer

### Final Endpoint: `mass_mutual_mft_ext.mft_data`

**Location**: `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`

| Attribute | Value |
|-----------|-------|
| **Total Rows** | 304,632 |
| **Date Range** | 2022-01-01 to 2026-02-09 |
| **Last Modified (UTC)** | 2026-02-11 10:00:31 |
| **Primary Use** | Final endpoint consumed by Looker Studio/reporting |

**Refresh Pattern**:
1. Scheduled query script rebuilds `mass_mutual_mft_ext.mft_data_current`.
2. Script then rebuilds `mass_mutual_mft_ext.mft_data` for final consumption.

**Current-table build step**:
```sql
CREATE OR REPLACE TABLE `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data_current` AS
SELECT
  date,
  campaign,
  COALESCE(REGEXP_EXTRACT(placement_name, r'\(\s*([A-Za-z]+)'), utm_source) AS partner,
  placement_name,
  utm_source,
  utm_medium,
  utm_campaign,
  utm_content,
  utm_term,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(video_audio_plays) AS video_audio_plays,
  SUM(video_audio_fully_played) AS video_audio_fully_played
FROM `looker-studio-pro-452620.repo_mart.mft_view`
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY date;
```

---

### Upstream Mart View: `repo_mart.mft_view`

**Location**: `looker-studio-pro-452620.repo_mart.mft_view`

This mart view remains the unioned transformation layer and feeds the endpoint build above.

---

### Final Endpoint Schema (`mass_mutual_mft_ext.mft_data`)

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Delivery date |
| `campaign` | STRING | Campaign name |
| `partner` | STRING | Partner/publisher extracted from placement or fallback to `utm_source` |
| `placement_name` | STRING | Placement identifier |
| `utm_source` | STRING | Traffic source |
| `utm_medium` | STRING | Marketing medium |
| `utm_campaign` | STRING | Campaign tag for analytics tracking |
| `utm_content` | STRING | Content variation identifier |
| `utm_term` | STRING | Search/targeting term |
| `cost` | FLOAT | Spend amount |
| `impressions` | INTEGER | Ad impressions |
| `clicks` | INTEGER | Click-through events |
| `video_audio_plays` | INTEGER | Video/audio starts |
| `video_audio_fully_played` | INTEGER | Video/audio completions |

---

## UTM Processing Scripts

### R Script: `util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r`

**Purpose**: Processes Basis trafficking sheets to extract UTM parameters

#### Input Files
```r
file_path1 <- 'Flight 1_Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx'
file_path2 <- 'Flight 2_Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx'
file_path3 <- 'Flight 3 Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx'

sheet_name1 <- "Q1 Tsheet"
sheet_name2 <- "Flight 2_Updated"
sheet_name3 <- "Flight 3"
```

#### Processing Steps

**1. Auto-detect Header Row**
```r
# Read first 20 rows to find header
temp_df <- read_excel(file_path, sheet = sheet_name, n_max = 20, col_names = FALSE)

# Find row containing "Property" in first column
header_row <- which(temp_df[[1]] == "Property")
skip_rows <- header_row[1] - 1
```

**2. Read and Clean Data**
```r
new_df <- read_excel(file_path, sheet = sheet_name, skip = skip_rows) %>%
  clean_names()
```

**3. Normalize Column Names**

Trafficking sheets have wide format with multiple creatives:
```
creative_1_name, creative_1_url, creative_2_name, creative_2_url, ...
```

Script normalizes to:
```
creative_1_name, creative_1_url, creative_2_name, creative_2_url, ...
```

**4. Pivot to Long Format**

Converts wide creative columns to rows for easier processing.

**5. Extract UTM Parameters**

Uses regex to parse URLs and extract individual UTM values.

**6. Upload to BigQuery**

Final processed data uploaded to `utm_scrap.b_sup_pivt_unioned_tab`.

---

### Related Files

| File | Purpose |
|------|---------|
| `util/basis_utms/essential/util__basis__utm_pivot_longer_loop.r` | Primary batch UTM extraction script |
| `util/basis_utms/essential/util_b_utm_validation.r` | BigQuery validation helper |
| `util/basis_utms/essential/stg3_b_plus_utms_PnS.sql` | Basis + UTM staging query |
| `util/basis_utms/archive/util__basis__utm_pivot_longer.r` | Legacy exploratory version (archived) |
| `util/basis_utms/archive/util__basis__utm_pivot_longer_clean.r` | Legacy single-flight version (archived) |
| `util/basis_utms/archive/union_basis_utms.ipynb` | Legacy notebook union workflow (archived) |
| `util/basis_utms/archive/b_utms_diagram.md` | Legacy diagram (archived) |
| `sql/base/basis/stg__basis__delivery.sql` | Basis delivery staging view |
| `util/basis_utms/essential/load_basis_utms_union.sql` | Critical union build for `landing.basis_utms_unioned` |
| `util/basis_utms/essential/stg__basis__utms.sql` | Basis UTM staging |
| `sql/base/basis/stg2__basis__plus_utms.sql` | Combined Basis + UTMs |

---

## Key Concepts

### 1. Creative Name Normalization

**Problem**: Same creative has different names across systems.

| System | Example Name |
|--------|--------------|
| DCM | `MassMutual_StayReady_300x250_v2` |
| Basis | `123_PEACOCK_Stay Ready_300x250` |
| UTM Sheet | `stay-ready-creative` |

**Solution**: Multi-step regex normalization:

```sql
-- Step-by-step transformation
'123_PEACOCK_Stay Ready_300x250_v2'
  → '123_PEACOCK_Stay Ready'           -- Strip size suffix
  → 'peacock_stay ready'               -- Lowercase
  → 'stay ready'                       -- Remove "peacock"
  → 'stayready'                        -- Remove spaces
  → 'stayready'                        -- Keep alphanumeric only
```

**Impact**: Increases UTM match rate from ~40% to ~85%.

---

### 2. Composite Join Keys

**Structure**:
```
del_key = LOWER(placement) || " || " || cleaned_creative_name
```

**Example**:
```
cp_45678_massmutual_display || stayreadybrandv2
```

**Why**: Single creative can run on multiple placements. Composite key ensures unique matching.

---

### 3. Multi-Source UTM Strategy

**Priority Order**:
1. `mm_utms_snapshot` - Primary source (most complete)
2. `b_sup_pivt_unioned_tab` - Trafficking sheet extracts
3. `dcm_plus_utms_upload` - Manual corrections

**Combination Logic**:
```sql
utm AS (
  SELECT DISTINCT ... FROM utm1    -- Trafficking sheets
  UNION DISTINCT
  SELECT * FROM utm4               -- Manual uploads
)
```

---

### 4. Deduplication Strategies

**DCM Branch**: Full row deduplication
```sql
ROW_NUMBER() OVER (
  PARTITION BY TO_JSON_STRING(joined)  -- Hash all columns
  ORDER BY placement_id
)
```

**Basis Branch**: Date + Key deduplication
```sql
ROW_NUMBER() OVER (
  PARTITION BY date, master_key
  ORDER BY placement
)
```

---

### 5. Minimum Impression Threshold

**Filter**: `impressions > 10`

**Rationale**:
- Removes test impressions
- Filters data noise
- Reduces row count ~15%
- Improves query performance

---

## Current State

### Final Endpoint Snapshot

| Metric | Value |
|--------|-------|
| Endpoint Table | `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data` |
| Total Rows | 304,632 |
| Date Range | 2022-01-01 to 2026-02-09 |
| Last Refresh (UTC) | 2026-02-11 10:00:31 |

---

### Data Distribution

#### By Source
| Source | Records | Percentage |
|--------|---------|------------|
| Basis | 82,710 | 61.6% |
| DCM | 51,573 | 38.4% |
| **Total** | **134,283** | 100% |

#### By UTM Source
| utm_source | utm_medium | Records | % of Total |
|------------|------------|---------|------------|
| basis | ott | 50,474 | 37.6% |
| basis | audio | 11,604 | 8.6% |
| mindbodygreen | standard | 11,127 | 8.3% |
| basis | ctv | 8,776 | 6.5% |
| basis | display | 7,349 | 5.5% |
| investmentnews | standard | 3,922 | 2.9% |
| thenewyorktimes | standard | 3,285 | 2.4% |
| wallstreetjournal | standard | 3,179 | 2.4% |
| advisorperspectives | standard | 2,889 | 2.2% |
| financialtimes | standard | 2,441 | 1.8% |

#### By Campaign
| Campaign | Description |
|----------|-------------|
| MassMutualWealthManagement2025 | Wealth management focus |
| MassMutualLVGP2025 | LVGP initiative |
| Massachusetts Mutual Full Funnel Branding FY25 | Brand awareness |
| MassMutual20252026Media | General media |
| MassMutualStayReady2025 | Stay Ready campaign |
| MassMutualVolatility2025 | Market volatility messaging |

---

### Update Schedule

| Component | Frequency | Last Updated |
|-----------|-----------|--------------|
| MFT Endpoint (`mass_mutual_mft_ext.mft_data`) | Daily scheduled query | Feb 11, 2026 10:00 UTC |
| DCM Cost Model | Daily | Jan 9, 2026 |
| Basis Master | Daily | Jan 12, 2026 |
| MM UTMs Snapshot | Daily | Jan 12, 2026 |
| UTM Pivot Table | As needed | Jan 12, 2026 |

---

## Usage Examples

### Safe Query Guardrails

Use `scripts/bq-safe-query.sh` to reduce accidental token-heavy outputs and expensive scans.

What it enforces by default:
- Blocks `SELECT *` (unless `--allow-select-star` is passed)
- Auto-appends `LIMIT 50` to `SELECT` queries that do not already include a limit
- Runs a dry-run check first and aborts if bytes processed exceed `176000000000` (about `$1` at `$6.25/TiB`)
- Caps returned rows with `bq query -n 50` and `--max_rows_per_request=50`
- Supports `--schema-only` mode to list all table columns without querying table data
- When a guardrail blocks a query, it prints summary-first SQL suggestions for safer analysis
- Guardrail failures also print one-run and session-level commands to bypass limits when needed

Examples:

```bash
# List all columns safely (no data row scan/output)
./scripts/bq-safe-query.sh --schema-only \
"looker-studio-pro-452620.mass_mutual_mft_ext.mft_data"

# Dry-run only (no result rows returned)
./scripts/bq-safe-query.sh --dry-run-only --sql \
"SELECT date, campaign, SUM(cost) AS spend
 FROM `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`
 WHERE date >= '2026-01-01'
 GROUP BY 1,2"

# Run query with tighter limits
./scripts/bq-safe-query.sh --max-rows 25 --max-bytes 100000000 --sql \
"SELECT date, utm_source, impressions
 FROM `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`
 WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
 ORDER BY date DESC"

# If blocked by guardrails, the script now suggests summary SQL recipes
./scripts/bq-safe-query.sh --sql \
"SELECT * FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view` LIMIT 1010"

# Bypass limits for one run (use carefully)
./scripts/bq-safe-query.sh --allow-select-star --max-rows 1000000 \
  --max-bytes 999999999999999 --sql "<your query>"
```

Environment defaults you can set once:

```bash
export BQ_SAFE_PROJECT_ID="looker-studio-pro-452620"
export BQ_SAFE_LOCATION="US"
export BQ_SAFE_MAX_ROWS="50"
export BQ_SAFE_MAX_BYTES="176000000000"
export BQ_SAFE_SCHEMA_MAX_ROWS="10000"
```

MCP policy in this repo:
- Use the same guardrails for BigQuery MCP `run_query` calls (no `SELECT *`, small limits, summary-first).
- If a request is too large, summarize first (counts/date span/top dimensions), then drill down with explicit columns.

### Offline Sheet Daily Sync

Use:
- `scripts/sql/mft_offline_daily_sheet_sync.sql` as the local SQL template
- `scripts/setup-mft-offline-daily-sheet-sync.sh` to render that SQL and create or update the BigQuery scheduled query
- `scripts/sql/stg__mm__mft_offline_connected_gsheet.sql` for direct BigQuery paste/run of staging DDL (no shell wrapper)
- `scripts/sql/mft_offline_update_manual.sql` for direct BigQuery paste/run of output table refresh SQL (no shell wrapper)

Behavior:
- Final table is native BigQuery (`mass_mutual_mft_ext.mft_offline`).
- SQL lives in `scripts/sql/mft_offline_daily_sheet_sync.sql` and is parameterized by the shell script.
- Setup script creates/updates staging external table `repo_stg.stg__mm__mft_offline_connected_gsheet` from the Sheet range.
- Scheduled query runs a single `SELECT` from staging and writes native output to `mft_offline`.
- Output columns are standardized to lowercase and mapped to actual sheet names (`date`, `channel`, `business_unit`, `campaign`, `partner`, `placement`, `spend`, `impressions`, `cpm`, `data_type`, `month`, `quarter`, `year`, `key_simp`, `total_act_cost_key`, `total_est_cost_key`, `full_key`, `year_quarter`).
- Output excludes rows where `COALESCE(spend, 0) + COALESCE(impressions, 0) = 0`.
- Scheduled-query transfer params include `destination_table_name_template` (`mft_offline`) and `write_disposition` (default `WRITE_TRUNCATE`).
- Default schedule is `every day 06:00` (adjustable with `--schedule`).
- The scheduled query identity (user or service account) must have access to the source Google Sheet.

Create a new schedule:

```bash
./scripts/setup-mft-offline-daily-sheet-sync.sh
```

Update an existing schedule:

```bash
./scripts/setup-mft-offline-daily-sheet-sync.sh \
  --transfer-config-id "projects/671028410185/locations/us/transferConfigs/699421ab-0000-2129-a27e-883d24f0f1b8"
```

Optional overrides:

```bash
./scripts/setup-mft-offline-daily-sheet-sync.sh \
  --schedule "every day 08:00" \
  --sql-file "scripts/sql/mft_offline_daily_sheet_sync.sql" \
  --staging-dataset "repo_stg" \
  --staging-table "stg__mm__mft_offline_connected_gsheet" \
  --write-disposition "WRITE_TRUNCATE" \
  --sheet-range "'[NEW] INTERNAL | COMBINED DATA'!A:U" \
  --service-account "bq-scheduler@looker-studio-pro-452620.iam.gserviceaccount.com" \
  --print-sql
```

Verify after creation:

```bash
bq ls --transfer_config --transfer_location=US --project_id=looker-studio-pro-452620
bq head -n 5 looker-studio-pro-452620:mass_mutual_mft_ext.mft_offline
```

### Query 1: Daily Performance by Channel
```sql
SELECT
  date,
  utm_medium AS channel,
  SUM(cost) AS total_spend,
  SUM(impressions) AS total_impressions,
  SUM(clicks) AS total_clicks,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100 AS ctr_pct
FROM `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`
WHERE date >= '2025-01-01'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
```

### Query 2: Publisher Performance
```sql
SELECT
  utm_source AS publisher,
  COUNT(DISTINCT date) AS active_days,
  SUM(impressions) AS total_impressions,
  SUM(clicks) AS total_clicks,
  SUM(cost) AS total_spend,
  SAFE_DIVIDE(SUM(cost), SUM(impressions)) * 1000 AS cpm
FROM `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`
WHERE utm_medium = 'standard'  -- Direct publisher buys
GROUP BY 1
HAVING total_impressions > 10000
ORDER BY total_spend DESC
```

### Query 3: Partner Performance (Last 30 Days)
```sql
SELECT
  partner,
  campaign,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100 AS ctr_pct,
  SUM(cost) AS spend,
  SAFE_DIVIDE(SUM(video_audio_fully_played), SUM(video_audio_plays)) * 100 AS completion_rate_pct
FROM `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1, 2
HAVING impressions > 1000
ORDER BY spend DESC
LIMIT 20
```

### Query 4: UTM Match Rate Analysis
```sql
SELECT
  CASE
    WHEN utm_source IS NULL THEN 'No UTM Match'
    ELSE 'UTM Matched'
  END AS match_status,
  COUNT(*) AS records,
  SUM(impressions) AS impressions,
  SUM(cost) AS spend
FROM `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`
GROUP BY 1
```

### Query 5: Video Completion by Campaign
```sql
SELECT
  campaign,
  SUM(video_audio_plays) AS plays,
  SUM(video_audio_fully_played) AS fully_played,
  SAFE_DIVIDE(SUM(video_audio_fully_played), SUM(video_audio_plays)) * 100 AS completion_rate_pct,
  SUM(cost) AS spend
FROM `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`
GROUP BY 1
HAVING plays > 0
ORDER BY completion_rate_pct DESC
```

---

## Comparison with ADIF Pipeline

| Aspect | MFT Endpoint | ADIF Pipeline |
|--------|----------|---------------|
| **Primary Purpose** | UTM-enriched delivery tracking | Planned vs actual reconciliation |
| **Client Focus** | MassMutual | Forevermark US |
| **Shared Source** | DCM.20250505_costModel_v5 | DCM.20250505_costModel_v5 |
| **Unique Sources** | Basis DSP, UTM sheets | FPD, Prisma, TV estimates |
| **Join Strategy** | LEFT JOIN (preserve delivery) | FULL OUTER JOIN (capture gaps) |
| **Key Enrichment** | UTM parameters | First-party data, planned budgets |
| **Output Columns** | 14 | 18 |
| **Row Count** | 304,632 | 12,085 |

---

## Maintenance

### Daily Checks
1. Verify row counts haven't dropped unexpectedly
2. Check for new campaigns requiring UTM mapping
3. Monitor UTM match rates

### Weekly Tasks
1. Review unmatched UTM records
2. Update trafficking sheet extracts if new flights launched
3. Validate creative name normalization catching new patterns

### Monthly Tasks
1. Archive old UTM upload files
2. Review and optimize view performance
3. Update documentation for schema changes

## Changelog Policy

- Use `CHANGELOG.md` for concise daily essentials only.
- Use `CHANGELOG_EXTENDED.md` for session-level and implementation-level detail.
- Keep both files in sync when logging new work:
  - summarize only the key outcomes in `CHANGELOG.md`
  - preserve full technical context in `CHANGELOG_EXTENDED.md`
- Preserve unrelated history:
  - do not delete or rewrite unrelated prior entries unless explicitly requested
  - scope updates to current-thread work by default
- Keep `CHANGELOG.md` entries consistent and preview-friendly:
  - bold each primary item title
  - keep `What` and `Why` concise and outcome-focused
  - keep paths in a dedicated collapsible section using `<details>` and put each path on its own line as a clickable link
  - keep long URLs on separate lines for readability

---

## Troubleshooting

### Issue: Low UTM Match Rate

**Symptoms**: Many NULL values in utm_source/utm_medium

**Diagnosis**:
```sql
SELECT
  COUNT(*) AS total,
  COUNTIF(utm_source IS NULL) AS no_utm,
  COUNTIF(utm_source IS NULL) / COUNT(*) * 100 AS pct_unmatched
FROM `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`
```

**Solutions**:
1. Check if new placements need UTM mapping
2. Verify creative name normalization regex
3. Add manual mappings to `dcm_plus_utms_upload`

---

### Issue: Duplicate Records

**Symptoms**: Same date/placement appearing multiple times

**Diagnosis**:
```sql
SELECT date, campaign, partner, placement_name, COUNT(*) AS dupes
FROM `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data`
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
ORDER BY dupes DESC
```

**Solutions**:
1. Check deduplication logic in staging views
2. Verify join keys are unique
3. Review source data for duplicates

---

## Contact

For questions or issues with this pipeline, contact the data engineering team.

**Related Documentation**:
- [ADIF Pipeline README](../adif/README.md) - Sister pipeline for Forevermark
- [Basis UTMs Diagram](../util/basis_utms/archive/b_utms_diagram.md) - Legacy diagram
- [DCM Cost Model](../sql/base/dcm/) - Shared DCM processing

**Last Updated**: February 12, 2026
