# looker_personal

SQL- and BigQuery-first analytics workspace for campaign reporting pipelines across ADIF, Olipop, MFT, and shared utility workflows.

## Core Workflows

- ADIF TV and digital pipeline (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif`)
- ADIF updated FPD integration (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif`)
- Olipop cross-platform delivery + video joins (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/sql/marts/olipop`)
- MFT export and mart views (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft`)
- Basis UTM processing utilities (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/basis_utms`) with active scripts in `essential/` and legacy assets in `archive/`

## Cross-Brand Data Flow Diagram (Ingestion -> BigQuery -> dbt -> Dashboards)

- OLI (Olipop), MassMutual (MFT), and ADIF flow diagram:
  `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/DATA_FLOW_DIAGRAMS.md`
- This view maps each brand’s path from source ingestion to BigQuery datasets/tables, dbt/SQL model layer, and final BI dashboards.

## New Capability: ADIF Social Layer From Cross-Platform Raw

- Source table: `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`
- Layer SQL: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/stg__adif__social_crossplatform.sql`
- Default output view: `looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform`
- Current inclusion logic: keep rows where `account_name` is one of `ADIF USA`, `A Diamond is Forever - US`, `A Diamond is Forever`, or `De Beers Group`, and `campaign_name` contains literal `WP_`
- Social-layered ADIF table build SQL: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/build__adif__prisma_expanded_plus_dcm_with_social_tbl.sql`
- Scheduled-query payload SQL: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/query__adif__prisma_expanded_plus_dcm_with_social_tbl_sched.sql`
- Social-layered table target: `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_with_social_tbl`
- Base schema source for the layered table: `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`

## Notes

- The ADIF social layer keeps original grain (daily ad-level rows) and all source metrics.
- If you want a mart/table target, materialize from the staging view to your selected dataset.
- SQL QA guardrail: validate in isolated `_qa` objects first, share proof results, and only then apply live SQL patches after explicit approval.
