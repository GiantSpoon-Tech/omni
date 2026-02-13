# Deployment Checklist: Updated FPD Integration

**Date:** 2026-01-23
**Status:** Ready for deployment
**Estimated time:** 15 minutes

---

## Pre-Deployment Checklist

- [ ] **1. Verify BigQuery table exists**
  ```bash
  bq show looker-studio-pro-452620:landing.adif_updated_fpd_daily
  ```
  Expected: 1,297 rows

- [ ] **2. Run SQL validation**
  ```bash
  bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
    < validate_updated_fpd_detailed_v2.sql
  ```
  Expected: Delta shows +244M impressions, +$3.3M spend

- [ ] **3. Run R validation**
  ```bash
  Rscript util_validate_updated_fpd_impact.r
  ```
  Expected: 46 common packages, 0 orphans

- [ ] **4. Review validation CSVs**
  ```bash
  ls -lh data/validation_*.csv
  ```
  Check for anomalies in package comparisons

---

## Deployment Steps

### Option A: Create New View (Recommended First)

- [ ] **Step 1: Deploy as new view**
  ```bash
  bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
    < deploy_updated_fpd_view.sql
  ```

  Creates: `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view_v3`

- [ ] **Step 2: Verify new view**
  ```sql
  SELECT COUNT(*) FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view_v3`;
  -- Expected: 11,418 rows

  SELECT data_source_primary, COUNT(*)
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view_v3`
  GROUP BY data_source_primary;
  -- Expected: updated_fpd shows 1,297 rows
  ```

- [ ] **Step 3: Test final metrics**
  ```sql
  SELECT
    SUM(final_impressions) AS total_imps,
    SUM(final_spend) AS total_spend
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view_v3`;
  -- Expected: ~697M impressions, ~$14.5M spend
  ```

- [ ] **Step 4: Compare sample packages**
  ```sql
  SELECT
    package_id_joined,
    date,
    fpd_orig_impressions,
    fpd_updated_impressions,
    fpd_impressions,
    final_impressions
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view_v3`
  WHERE fpd_updated_impressions > 0
  LIMIT 10;
  ```

- [ ] **Step 5: Monitor for 1 week**
  - Check BI dashboard metrics
  - Verify no downstream errors
  - Review user feedback

### Option B: Replace Original View (After Testing)

⚠️ **Only after Option A has been tested for 1 week**

- [ ] **Step 1: Backup original view definition**
  ```bash
  bq show --format=prettyjson \
    looker-studio-pro-452620:repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test \
    > backup_original_view_definition.json
  ```

- [ ] **Step 2: Uncomment REPLACE section in deploy script**
  Edit `deploy_updated_fpd_view.sql` - uncomment the section at bottom

- [ ] **Step 3: Execute replacement**
  ```bash
  bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
    < deploy_updated_fpd_view.sql
  ```

- [ ] **Step 4: Verify replacement**
  ```sql
  SELECT COUNT(*) FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`;
  -- Expected: 11,418 rows (same as before)

  SELECT data_source_primary, COUNT(*)
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
  GROUP BY data_source_primary;
  -- Expected: now shows updated_fpd rows
  ```

---

## Post-Deployment Validation

- [ ] **1. Check BI dashboards**
  - Verify metrics look reasonable
  - Check for unexpected spikes/drops
  - Review date ranges

- [ ] **2. Run comparison query**
  ```sql
  -- Compare final totals
  WITH original AS (
    SELECT SUM(final_impressions) AS imps, SUM(final_spend) AS spend
    FROM backup_original_view  -- if you saved a backup
  ),
  current AS (
    SELECT SUM(final_impressions) AS imps, SUM(final_spend) AS spend
    FROM `repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
  )
  SELECT
    o.imps AS original_imps,
    c.imps AS current_imps,
    c.imps - o.imps AS delta_imps,
    o.spend AS original_spend,
    c.spend AS current_spend,
    c.spend - o.spend AS delta_spend
  FROM original o, current c;
  -- Expected: Delta = +244M imps, +$3.3M spend
  ```

- [ ] **3. Spot check specific packages**
  ```sql
  -- Check a known package with updated FPD
  SELECT
    date,
    fpd_orig_impressions,
    fpd_updated_impressions,
    fpd_impressions,
    final_impressions
  FROM `repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
  WHERE package_id_joined = 'P37JYWD'  -- YouTubeWrap package
  ORDER BY date;
  ```

- [ ] **4. Monitor query performance**
  - Check query execution times
  - Review bytes processed
  - Ensure no slowdowns

---

## Rollback Plan (If Needed)

If issues are discovered:

### Quick Rollback (within 24 hours)

```sql
-- Option 1: Restore from backup definition
CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test` AS
-- [paste original view SQL from backup_original_view_definition.json]
```

### Alternative: Point to old view

```sql
-- Option 2: Recreate with reference to backup
CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test` AS
SELECT * FROM `repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test_backup`;
-- (if you created a backup table)
```

---

## Success Criteria

✅ **Deployment is successful if:**

1. Row count remains 11,418
2. Delta metrics match validation: +244M imps, +$3.3M spend
3. No downstream query errors
4. BI dashboards show reasonable values
5. All validation queries pass
6. Query performance acceptable

❌ **Rollback if:**

1. Row count changes unexpectedly
2. Downstream queries fail
3. BI dashboards show anomalies
4. Query performance degrades >50%
5. Data integrity checks fail

---

## Communication Plan

### Before Deployment
- [ ] Notify BI team of upcoming changes
- [ ] Set expectations for metric increases
- [ ] Share validation results

### During Deployment
- [ ] Post status in team Slack channel
- [ ] Monitor for alerts/errors
- [ ] Be available for questions

### After Deployment
- [ ] Announce completion
- [ ] Share before/after comparison
- [ ] Update documentation links

---

## Troubleshooting

### Issue: View creation fails

**Check:**
- BigQuery permissions
- Source table exists: `landing.adif_updated_fpd_daily`
- SQL syntax is valid

**Solution:**
```bash
# Test SQL syntax only
bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false --dry_run \
  < sql/stg__adif__updated_fpd_integrated_v3.sql
```

### Issue: Metrics don't match expectations

**Check:**
- Updated FPD table has 1,297 rows
- Join keys are correct
- Aggregation logic is working

**Solution:**
```sql
-- Verify source table
SELECT COUNT(*), SUM(daily_fpd_impressions), SUM(daily_fpd_spend)
FROM `landing.adif_updated_fpd_daily`;
-- Expected: 1,297 rows, 244M imps, $3.3M spend
```

### Issue: Query timeout

**Check:**
- View complexity
- Table sizes
- Join conditions

**Solution:**
- Consider materializing intermediate CTEs
- Add appropriate WHERE clauses for date filtering
- Contact DBA for optimization help

---

## Sign-Off

- [ ] **Data Engineer:** Deployment complete
- [ ] **QA:** Validation passed
- [ ] **BI Lead:** Dashboards verified
- [ ] **Business Owner:** Metrics approved

**Deployment Date:** ________________

**Deployed By:** ________________

**Notes:**
_______________________________________________________________
_______________________________________________________________
_______________________________________________________________

---

## Quick Reference

**Key Files:**
- Integration SQL: `sql/stg__adif__updated_fpd_integrated_v3.sql`
- Deployment script: `deploy_updated_fpd_view.sql`
- Validation: `validate_updated_fpd_detailed_v2.sql`
- Processing pipeline: `util_process_updated_fpd.r`

**Key Tables:**
- Source: `landing.adif_updated_fpd_daily` (1,297 rows)
- Original view: `repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
- New view (test): `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view_v3`

**Key Metrics:**
- Packages with updated FPD: 46
- Daily rows added: 1,297
- Impressions added: 244,251,156
- Spend added: $3,265,977

**Support:**
- Documentation: `README_Updated_FPD_Integration.md`
- Project summary: `PROJECT_SUMMARY_Updated_FPD_Integration.md`
- Contact: Data Engineering Team
