# Project Summary: Updated FPD Integration

**Date:** January 23, 2026
**Status:** ✅ Ready for Deployment
**Impact:** +244M impressions, +$3.3M spend across 46 packages

---

## Overview

Successfully integrated updated first-party data (FPD) from a new Google Sheet source into the ADIF data pipeline. The new data provides package-level metrics that are spread evenly across date ranges and integrated into the existing staging view.

## What Was Built

### 1. Data Processing Pipeline

**Script:** `util_process_updated_fpd.r`

**What it does:**
1. Reads 46 package-level records from Google Sheet
2. Joins with Prisma to get start/end dates for each package
3. Spreads metrics evenly across daily rows (1,297 total rows)
4. Validates that totals match (no data loss)
5. Uploads to BigQuery: `landing.adif_updated_fpd_daily`

**Runtime:** ~7 seconds

**Key Metrics:**
- Input: 46 packages with aggregated metrics
- Output: 1,297 daily rows (Oct 26, 2025 - Jan 12, 2026)
- Total impressions: 244,251,156
- Total spend: $3,265,977

### 2. Integration SQL (v3 - Final)

**File:** `sql/stg__adif__updated_fpd_integrated_v3.sql`

**Key Features:**
- Preserves ALL 109 existing columns from original staging view
- Adds new FPD breakdown columns:
  - `fpd_orig_impressions`, `fpd_orig_spend` (original partner sheet data)
  - `fpd_updated_impressions`, `fpd_updated_spend` (new Google Sheet data)
  - `fpd_impressions`, `fpd_spend` (COMBINED: original + updated)
- Recalculates `final_*` metrics with priority: Combined FPD → DCM → Planned
- Adds package-level rollup columns for all FPD sources

**Schema Impact:**
- ✅ All original columns preserved
- ✅ 11 new columns added for FPD breakdown
- ✅ 4 columns recalculated: `final_impressions`, `final_spend`, `pkg_act_imps`, `pkg_act_spend`

### 3. Validation Tools

#### R Validation Script
**File:** `util_validate_updated_fpd_impact.r`

Compares existing vs. new data sources at package level:
- Common packages (46) - in both sources
- Only in existing (184) - no updated FPD
- Only in updated (0) - all matched!

**Outputs:**
- `validation_common_packages.csv` - Side-by-side comparison
- `validation_only_existing_packages.csv` - Packages without updated FPD
- `validation_only_updated_packages.csv` - Orphaned packages (0 found)
- `validation_daily_comparison_sample.csv` - Daily detail for top 5 packages
- `validation_summary.csv` - Overall statistics

#### SQL Validation Query
**File:** `validate_updated_fpd_detailed_v2.sql`

Provides detailed breakdown by data source:

| Source | Original View | New View | Delta |
|--------|---------------|----------|-------|
| DCM | 402M imps, $9.7M | 402M imps, $9.7M | No change |
| Original FPD | 89M imps, $2.1M | 89M imps, $2.1M | No change |
| Updated FPD | 0 | 244M imps, $3.3M | **+244M, +$3.3M** |
| **Combined FPD** | 89M imps, $2.1M | 333M imps, $5.4M | **+244M, +$3.3M** |
| **TOTAL (final_*)** | 453M imps, $11.2M | 697M imps, $14.5M | **+244M, +$3.3M** |

### 4. Generalized Validation Framework

**Files:**
- `util_validate_two_sources.r` - Data-agnostic comparison tool
- `config_validation_fpd.yaml` - Example configuration

**Purpose:** Reusable framework to compare ANY two BigQuery tables/views

**Features:**
- YAML-based configuration
- Flexible column mapping
- Automatic overlap detection
- CSV outputs for analysis

### 5. Deployment Script

**File:** `deploy_updated_fpd_view.sql`

**Deployment Steps:**
1. **Test first:** Creates new view `adif__prisma_expanded_plus_dcm_updated_fpd_view_v3`
2. **After validation:** Uncomment section to replace original view

**Included:**
- Full SQL for view creation
- Post-deployment validation queries
- Optional replacement of original view

---

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────┐
│  GOOGLE SHEET: ADIF_Packages_w/o_delivery               │
│  46 packages with updated FPD (package-level)           │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
                 ┌───────────────┐
                 │  STEP 1: Read │
                 │  (R Script)   │
                 └───────┬───────┘
                         │
                         ▼
          ┌──────────────────────────┐
          │  STEP 2: Join w/ Prisma  │
          │  (Get date ranges)       │
          └──────────┬───────────────┘
                     │
                     ▼
          ┌─────────────────────────┐
          │  STEP 3: Spread Daily   │
          │  (Divide evenly)        │
          └──────────┬──────────────┘
                     │
                     ▼
          ┌─────────────────────────┐
          │  STEP 4: Upload to BQ   │
          │  landing.adif_updated_  │
          │  fpd_daily              │
          └──────────┬──────────────┘
                     │
                     ▼
          ┌─────────────────────────┐
          │  STEP 5: LEFT JOIN      │
          │  to staging view        │
          └──────────┬──────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────┐
│  FINAL VIEW: adif__prisma_expanded_plus_dcm_           │
│  updated_fpd_view_v3                                   │
│  - All 109 original columns                            │
│  - 11 new FPD breakdown columns                        │
│  - Updated final_* metrics                             │
└────────────────────────────────────────────────────────┘
```

---

## Key Decisions & Design Choices

### 1. Why Combine FPD Metrics?

**Decision:** `fpd_impressions` and `fpd_spend` now represent COMBINED totals (original + updated)

**Rationale:**
- Provides single source of truth for all FPD data
- Maintains backward compatibility with existing queries
- Allows granular analysis via `fpd_orig_*` and `fpd_updated_*` columns

**Alternative considered:** Keep metrics separate and rename original columns
- ❌ Would break existing queries referencing `fpd_impressions`
- ❌ Less intuitive for end users

### 2. Why Spread Package-Level Data?

**Decision:** Divide package totals evenly across date range

**Rationale:**
- Enables daily-level analysis and joining with other daily data
- Preserves package-level totals exactly (validated)
- Simple and transparent methodology

**Limitations:**
- Assumes even distribution (actuals may vary by day)
- Relies on Prisma date ranges being accurate

**Alternative considered:** Weight by day-of-week or existing delivery patterns
- ❌ Added complexity without clear benefit
- ✅ Future enhancement opportunity

### 3. Why LEFT JOIN (not FULL OUTER JOIN)?

**Decision:** Use LEFT JOIN when adding updated FPD to existing view

**Rationale:**
- All 46 packages in updated FPD matched existing packages
- No orphan records in updated FPD source
- Simpler logic, better performance

**Validation confirmed:** 0 packages only in updated FPD

### 4. Schema Preservation Strategy

**Decision:** Build on existing view rather than rebuild from scratch

**Rationale:**
- Minimizes risk of breaking existing logic
- Preserves all 109 original columns
- Easier to test and deploy incrementally

**Implementation:**
```sql
WITH existing_view AS (
  SELECT * FROM original_view  -- All 109 columns
),
updated_fpd AS (...),
combined AS (
  SELECT e.*, u.*  -- Add new columns
  FROM existing_view e
  LEFT JOIN updated_fpd u ...
)
```

---

## Validation Results

### Package Overlap Analysis

| Category | Count | Impressions | Spend |
|----------|-------|-------------|-------|
| **Common** (in both) | 46 | 244M | $3.3M |
| **Only in existing** | 184 | 453M | $11.2M |
| **Only in updated** | 0 | 0 | $0 |

✅ **Perfect match:** All updated FPD packages found in existing staging view

### Data Integrity Checks

✅ **No data loss:** Daily totals match package-level totals exactly
✅ **No orphans:** Zero packages in updated FPD without matches
✅ **Row count preserved:** 11,418 rows in both original and new views
✅ **DCM & original FPD unchanged:** All existing data intact

### Top 5 Packages by Updated FPD Spend

1. **MIQ DIGITAL** - YouTubeWrapDecember: $420K / 2.8M impressions
2. **MIQ DIGITAL** - YouTubeWrap: $400K / 22.4M impressions
3. **IHEART MEDIA** - Radio Andy: $400K / 28.7M impressions
4. **SIRIUS XM** - Radio Andy: $299K / 86M impressions
5. **NATIONAL CINEMEDIA** - Equitable Mix Pre-Trailer: $250K / 2.3M impressions

---

## Files Created

### R Scripts
| File | Purpose | Lines |
|------|---------|-------|
| `util_explore_new_fpd_sheet.r` | Exploration tool for Google Sheet structure | 127 |
| `util_process_updated_fpd.r` | Main processing pipeline | 234 |
| `util_validate_updated_fpd_impact.r` | Package-level validation | 387 |
| `util_validate_two_sources.r` | Generalized validation framework | 468 |

### SQL Scripts
| File | Purpose | Lines |
|------|---------|-------|
| `sql/stg__adif__updated_fpd_integrated_v3.sql` | Final integration SQL | 265 |
| `validate_updated_fpd_detailed_v2.sql` | Source breakdown validation | 320 |
| `deploy_updated_fpd_view.sql` | Deployment script with instructions | 402 |
| `test_updated_fpd_integration.sql` | Quick test query | 168 |

### Configuration & Documentation
| File | Purpose |
|------|---------|
| `config_validation_fpd.yaml` | Validation config example |
| `README_Updated_FPD_Integration.md` | Comprehensive integration guide |
| `PROJECT_SUMMARY_Updated_FPD_Integration.md` | This document |

### Data Outputs
| File | Records | Purpose |
|------|---------|---------|
| `data/updated_fpd_packages.csv` | 46 | Package-level raw data |
| `data/prisma_package_dates.csv` | 230 | Date ranges from Prisma |
| `data/updated_fpd_with_dates.csv` | 46 | Joined before spreading |
| `data/updated_fpd_daily.csv` | 1,297 | Final daily expanded data |
| `data/updated_fpd_package_summary.csv` | 46 | Summary by package |
| `data/validation_common_packages.csv` | 46 | Common package comparison |
| `data/validation_only_existing_packages.csv` | 184 | Packages without updated FPD |
| `data/validation_only_updated_packages.csv` | 0 | Orphaned packages |
| `data/validation_summary.csv` | 5 | Overall statistics |

### BigQuery Tables
| Table | Rows | Purpose |
|-------|------|---------|
| `landing.adif_updated_fpd_daily` | 1,297 | Updated FPD spread across days |

---

## Deployment Instructions

### Step 1: Verify Prerequisites

```bash
# Confirm BigQuery table exists
bq show looker-studio-pro-452620:landing.adif_updated_fpd_daily

# Expected output: 1,297 rows, 14 columns
```

### Step 2: Run Final Validation

```bash
cd /Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif

# SQL validation (compare original vs new)
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < validate_updated_fpd_detailed_v2.sql

# R validation (package-level comparison)
Rscript util_validate_updated_fpd_impact.r
```

**Expected results:**
- Delta: +244M impressions, +$3.3M spend
- 46 common packages, 0 orphans
- All validation checks pass ✅

### Step 3: Deploy New View (Test)

```bash
# Create new test view (non-destructive)
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < deploy_updated_fpd_view.sql
```

**Result:** Creates `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view_v3`

### Step 4: Test New View

```sql
-- Check row count (should be 11,418)
SELECT COUNT(*)
FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view_v3`;

-- Check data source distribution
SELECT data_source_primary, COUNT(*), SUM(final_impressions), SUM(final_spend)
FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view_v3`
GROUP BY data_source_primary;

-- Sample updated FPD rows
SELECT package_id_joined, date, fpd_orig_impressions, fpd_updated_impressions,
       fpd_impressions, final_impressions
FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view_v3`
WHERE fpd_updated_impressions > 0
LIMIT 10;
```

### Step 5: Update Downstream References (if needed)

If creating as new view:
```sql
-- Update mart layer to reference new view
UPDATE `repo_mart.adif__tv_digital_unioned`
SET source_view = 'adif__prisma_expanded_plus_dcm_updated_fpd_view_v3';
```

### Step 6: Replace Original View (Optional)

After thorough testing:
```bash
# Uncomment the REPLACE section in deploy_updated_fpd_view.sql
# Then execute to replace original view
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
  < deploy_updated_fpd_view.sql
```

⚠️ **Warning:** This replaces `adif__prisma_expanded_plus_dcm_view_v3_test` with new logic

---

## Maintenance

### Regular Updates

When new updated FPD data is available:

```bash
# Re-run processing pipeline
Rscript util_process_updated_fpd.r

# Verify upload
bq show looker-studio-pro-452620:landing.adif_updated_fpd_daily

# View auto-refreshes (no action needed)
```

### Monitoring

**Weekly:**
- Check for packages without Prisma date ranges
- Verify updated FPD row count trends
- Review validation summary

**Monthly:**
- Archive checkpoint CSVs
- Review package overlap (expect 100% match)
- Check for new packages in Google Sheet

### Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| Package has FPD but no dates | Not in Prisma or is "Child" type | Check package_id spelling or add manual dates |
| Totals don't match | Floating point rounding | Acceptable if difference < 1 unit |
| Google Sheet structure changed | Column names modified | Update script column references |

---

## Future Enhancements

### Short Term (1-3 months)
1. **Automated Scheduling:** Set up Cloud Scheduler to run R script daily
2. **Data Quality Alerts:** Slack/email notifications for validation failures
3. **Historical Tracking:** Archive previous versions of updated FPD

### Medium Term (3-6 months)
1. **Weighted Date Spreading:** Use day-of-week patterns from DCM data
2. **Date Range Validation:** Compare Prisma vs DCM delivery dates
3. **Package Matching Improvements:** Handle package ID variations automatically

### Long Term (6+ months)
1. **Real-time Integration:** Stream updated FPD instead of batch processing
2. **Machine Learning:** Predict daily distribution patterns
3. **Self-Service UI:** Web interface for uploading updated FPD

---

## Team Knowledge Transfer

### For Data Engineers
- **Where to find the code:** `/adif/` directory
- **Main script:** `util_process_updated_fpd.r`
- **Validation:** `util_validate_updated_fpd_impact.r`
- **BigQuery table:** `landing.adif_updated_fpd_daily`

### For Analysts
- **New columns available:**
  - `fpd_orig_*` - Original partner sheet data
  - `fpd_updated_*` - New Google Sheet data
  - `fpd_*` - Combined totals (use these!)
- **Priority:** Combined FPD → DCM → Planned
- **Coverage:** 46 packages, Oct-Jan 2026

### For Business Users
- **What changed:** We added more accurate FPD data from a new source
- **Impact:** +244M impressions, +$3.3M spend across 46 packages
- **Where to see it:** Final metrics in BI dashboards will reflect combined FPD data
- **Questions:** Review `validation_common_packages.csv` for package-by-package changes

---

## Conclusion

Successfully integrated updated FPD data into the ADIF pipeline with:

✅ **Zero data loss** - All metrics preserved exactly
✅ **Zero orphans** - All packages matched
✅ **Schema preserved** - All original columns intact
✅ **Validated thoroughly** - Multiple validation checks pass
✅ **Ready to deploy** - SQL tested and working
✅ **Well documented** - Comprehensive guides created

**Net Impact:** +244M impressions and +$3.3M spend now visible in reporting

**Recommendation:** Deploy to test view first, validate for 1 week, then replace original view.

---

## Contact & Support

**Created by:** Claude Code (Anthropic)
**Date:** January 23, 2026
**Related Documentation:**
- [Main Pipeline Documentation](README%20-%20ADIF%20TV%20&%20Digital%20Data%20Pipeline.md)
- [Updated FPD Integration Guide](README_Updated_FPD_Integration.md)
- [Original FPD Collection](util_collect_fpd_v2.r)

For questions or issues, contact the data engineering team.
