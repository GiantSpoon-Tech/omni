# Data Flow Diagram: OLI, MassMutual, ADIF

This diagram shows the end-to-end reporting flow for the three focus pipelines:
ingestion -> BigQuery -> dbt/SQL models -> dashboards.

```mermaid
flowchart LR
  classDef ingest fill:#FFF6D6,stroke:#8A6D00,color:#2B2100
  classDef bq fill:#E8F0FE,stroke:#1A73E8,color:#0B2B5B
  classDef dbt fill:#FDECEA,stroke:#C0392B,color:#5B1B14
  classDef dash fill:#EAF7EF,stroke:#1E8E3E,color:#0D3C1E

  subgraph OLI["OLI (Olipop)"]
    OLI_I["Ingestion<br/>Meta Ads + TikTok Ads + Google Ads + ad_reporting feed"] --> OLI_BQ["BigQuery<br/>repo_facebook / repo_tiktok / repo_google_ads / ad_reporting_transformed"]
    OLI_BQ --> OLI_DBT["dbt/SQL Models<br/>stg__olipop_videoviews_crossplatform<br/>mart__olipop__crossplatform<br/>stg__olipop__crossplatform_filtered_raw"]
    OLI_DBT --> OLI_DASH["Dashboards<br/>Looker Studio / BI"]
  end

  subgraph MFT["MassMutual (MFT)"]
    MFT_I["Ingestion<br/>DCM + Basis + UTM sheets/uploads"] --> MFT_BQ["BigQuery<br/>DCM cost model / basis_master / mm_utms_snapshot / utm_scrap"]
    MFT_BQ --> MFT_DBT["dbt/SQL Models<br/>final_views.dcm<br/>final_views.utms_view<br/>repo_stg.dcm_plus_utms<br/>repo_mart.mft_view"]
    MFT_DBT --> MFT_DASH["Dashboards<br/>Looker Studio / BI"]
  end

  subgraph ADIF["ADIF"]
    ADIF_I["Ingestion<br/>Google Sheets FPD + TV estimates + DCM + Prisma"] --> ADIF_BQ["BigQuery<br/>landing.adif_fpd_data_ranged<br/>landing.adif_updated_fpd_daily<br/>landing.tv_local_estimates / tv_national_estimates"]
    ADIF_BQ --> ADIF_DBT["dbt/SQL Models<br/>adif__prisma_expanded_plus_dcm_view_v3_test<br/>stg__adif__updated_fpd_integrated_v3<br/>adif__tv_digital_unioned"]
    ADIF_DBT --> ADIF_DASH["Dashboards<br/>Looker Studio / BI"]
  end

  class OLI_I,MFT_I,ADIF_I ingest
  class OLI_BQ,MFT_BQ,ADIF_BQ bq
  class OLI_DBT,MFT_DBT,ADIF_DBT dbt
  class OLI_DASH,MFT_DASH,ADIF_DASH dash
```

## Notes

- OLI model names are based on SQL files in `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/sql/stg` and `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/sql/marts/olipop`.
- MFT model and source names are based on `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft/README.md`.
- ADIF source/model names are based on `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/README - ADIF TV & Digital Data Pipeline.md` and `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/README_Updated_FPD_Integration.md`.
