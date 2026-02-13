# CodeViz Research Context

> **Note**: This file contains research context from CodeViz. Most recent contexts are at the bottom.

---

## Research Query

Investigate the "Basis" component in detail. Focus your analysis on these key files:
1. basis (/Users/eugenetsenter/Looker_clonedRepo/looker_personal/sql/base/basis)

Provide insights about the component's main responsibilities and functionality and how it integrates with other system components

*Session: e1cdba40626189ff0616a4b0465227ba | Generated: 8/5/2025, 11:36:52 AM*

### Analysis Summary

# Basis Component Analysis

The **Basis** component, primarily located in the [sql/base/basis](sql/base/basis) directory, is responsible for staging and processing core data related to UTMS (Urchin Tracking Modules) and delivery information. It acts as an initial data preparation layer, transforming raw data into a structured format suitable for further analysis and integration with other system components.

## Main Responsibilities and Functionality

The Basis component's core responsibilities include:

*   **Data Staging for UTMS:** It processes and stages UTM-related data, likely from various sources, preparing it for unioning and further analysis. This involves extracting, cleaning, and standardizing UTM parameters.
*   **Delivery Data Staging:** It handles the staging of delivery-related data, which could encompass information about ad deliveries, campaign performance, or other operational metrics.
*   **Data Loading and Integration:** It provides scripts for loading processed Basis data into master tables, facilitating its integration into a broader data model.
*   **Data Quality and Deduplication:** It includes utilities and tests to ensure data quality, specifically focusing on detecting and handling duplicate records.

### Key Files and Their Roles

The following files within the [sql/base/basis](sql/base/basis) directory define the functionality of the Basis component:

*   **[stg__basis__utms.sql](util/basis_utms/essential/stg__basis__utms.sql)**: This file is likely responsible for the initial staging of raw UTM data. It performs transformations and cleaning to prepare UTM information for further processing.
*   **[stg__basis__delivery.sql](sql/base/basis/stg__basis__delivery.sql)**: This script handles the staging of delivery-related data. It prepares raw delivery metrics or logs for integration.
*   **[stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql)**: This file suggests a second stage of processing, potentially joining or enriching the staged delivery data with the processed UTM information. This indicates a key integration point within the Basis component itself.
*   **[load_basis_utms_union.sql](util/basis_utms/essential/load_basis_utms_union.sql)**: This script is responsible for unioning various UTM-related datasets, consolidating them into a unified structure. This is crucial for comprehensive UTM analysis.
*   **[load__basis_gsheet_to_master2.sql](sql/base/basis/load__basis_gsheet_to_master2.sql)**: This file indicates a process for loading data, possibly from a Google Sheet source, into a master table. This suggests that external data sources are integrated into the Basis component's data flow.
*   **[test__basis__duplicateDetector.r](sql/base/basis/test__basis__duplicateDetector.r)**: This R script is a data quality utility. Its presence indicates that the Basis component has built-in mechanisms for identifying and potentially handling duplicate records, which is critical for data integrity.
*   **[utms_util/](sql/base/basis/utms_util/)**: This directory likely contains utility scripts or functions specifically designed to assist with UTM processing, such as parsing, normalization, or validation of UTM parameters.

## Integration with Other System Components

The Basis component serves as a foundational data layer, providing cleaned and structured data to downstream components.

*   **Data Consumers (Marts/Reports):** The staged and loaded data from the Basis component, particularly the unified UTM and delivery data, would be consumed by higher-level mart tables or reporting layers (e.g., `marts/delivery/mart__delivery__unified_dcm_basis.sql.sql` as seen in the directory structure). These marts would then build aggregated views or business-specific reports based on the prepared Basis data.
*   **Upstream Data Sources:** The Basis component integrates with various upstream data sources, as evidenced by `load__basis_gsheet_to_master2.sql`, which suggests integration with Google Sheets. Other potential upstream sources could include ad platforms, analytics tools, or internal databases.
*   **DCM Component:** Given the presence of `mart__delivery__join_dcm_utms_prisma.sql` in the `sql/base/dcm` directory, it's highly probable that the Basis component's processed UTM and delivery data is joined with data from a "DCM" (likely DoubleClick Campaign Manager or a similar ad server) component. This indicates a crucial integration for comprehensive marketing performance analysis.
*   **Prisma Component:** The mention of "Prisma" in `mart__delivery__join_dcm_utms_prisma.sql` suggests another integration point, where Basis data is combined with data from a "Prisma" component, possibly related to financial or media planning data.

---

## Research Query

Investigate the "Google Sheets" component in detail. Provide a comprehensive analysis of its architecture, responsibilities, and implementation details.

*Session: fa6549c0487035571081e4b0e645a72e | Generated: 8/5/2025, 11:39:43 AM*

### Analysis Summary

# Google Sheets Component Analysis

## High-Level Architecture

The "Google Sheets" component within this codebase primarily functions as a data source, integrated into the BigQuery data warehouse. It is represented by two key BigQuery views: **`basis_gsheet_table`** and **`basis_gsheet2`**. These views act as direct interfaces to data originating from Google Sheets, making it accessible for further processing and analysis within the BigQuery environment. The data from these sheets is then loaded into a master table for unified reporting and analysis.

## Mid-Level Component Interaction

The Google Sheets data, exposed via the BigQuery views, is consumed by SQL transformation scripts. Specifically, the [load__basis_gsheet_to_master2.sql](sql/base/basis/load__basis_gsheet_to_master2.sql) script is responsible for ingesting data from the `basis_gsheet2` view and integrating it into a `master2` table. This indicates a typical ETL (Extract, Transform, Load) pattern where Google Sheets serve as the "Extract" source.

## Implementation Details

### BigQuery Views for Google Sheets Data

Two primary LookerML view files define the structure and accessible fields for the Google Sheets data within BigQuery:

*   **`basis_gsheet_table`** ([basis_gsheet_table.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/basis_gsheet_table.view.yaml)):
    *   **Purpose:** This view provides a structured representation of Google Sheets data, likely for a specific reporting or analytical purpose.
    *   **Internal Parts:** It defines numerous dimensions and a single measure.
    *   **Dimensions:** `day`, `campaign_name`, `line_item_name`, `basis_tactic`, `placement`, `creative_name`, `creative_grouping_creative_grouping`, `basis_dsp_tactic_group`, `impressions`, `clicks`, `delivered_spend`, `video_audio_plays`, `video_views`, `video_audio_fully_played`, `viewable_impressions`, `meta_data_date_range`, `meta_data_date_pull`, `gmail_dt`, `latest_record`, `n`.
    *   **Measures:** `count` (aggregate type: count).
    *   **External Relationships:** References the BigQuery table `giant-spoon-299605.data_model_2025.basis_gsheet_table`.

*   **`basis_gsheet2`** ([basis_gsheet2.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/basis_gsheet2.view.yaml)):
    *   **Purpose:** Similar to `basis_gsheet_table`, this view also exposes Google Sheets data, potentially with slight variations in schema or intended use.
    *   **Internal Parts:** It defines a similar set of dimensions and a single measure.
    *   **Dimensions:** `campaign_name`, `line_item_name`, `basis_tactic`, `placement`, `creative_name`, `creative_grouping_creative_grouping`, `basis_dsp_tactic_group`, `impressions`, `clicks`, `delivered_spend`, `video_audio_plays`, `video_views`, `video_audio_fully_played`, `viewable_impressions`, `meta_data_date_range`, `meta_data_date_pull`, `gmail_dt`, `latest_record`, `n`, and `day` (with a description for date formatting).
    *   **Measures:** `count` (aggregate type: count).
    *   **External Relationships:** References the BigQuery table `giant-spoon-299605.data_model_2025.basis_gsheet2`.

### Data Loading and Transformation

The data from the Google Sheets BigQuery views is processed by the following SQL script:

*   **`load__basis_gsheet_to_master2.sql`** ([load__basis_gsheet_to_master2.sql](sql/base/basis/load__basis_gsheet_to_master2.sql)):
    *   **Purpose:** This script is responsible for loading and transforming data from the `basis_gsheet2` BigQuery view into a `master2` table. It performs data cleaning and standardization before insertion.
    *   **Implementation Details:**
        *   It selects various fields from `giant-spoon-299605.data_model_2025.basis_gsheet2` ([load__basis_gsheet_to_master2.sql:20](sql/base/basis/load__basis_gsheet_to_master2.sql:20)).
        *   It applies transformations such as `TRIM` to `campaign_name` and `line_item_name`.
        *   It casts `impressions`, `clicks`, `delivered_spend`, `video_audio_plays`, `video_views`, `video_audio_fully_played`, and `viewable_impressions` to `BIGNUMERIC`.
        *   It includes metadata fields like `meta_data_date_range`, `meta_data_date_pull`, `gmail_dt`, and `latest_record`.
        *   The script is designed to insert or update records into a destination table, ensuring data consistency and proper typing for downstream analysis.

---

## Research Query

Investigate the "Upstream Data Sources" component in detail. Provide a comprehensive analysis of its architecture, responsibilities, and implementation details.

*Session: 638379b3ba82a964925eaf53eb1938c6 | Generated: 8/5/2025, 11:40:02 AM*

### Analysis Summary

# Upstream Data Sources Analysis

The codebase defines and interacts with several upstream data sources, primarily through BigQuery connections and staging SQL transformations. These sources serve as the initial ingestion points for raw or semi-processed data, which is then transformed and modeled for various analytical purposes. The architecture emphasizes clear separation between raw data ingestion, staging, and subsequent data modeling.

## High-Level Architecture

Upstream data sources are conceptualized as the initial entry points of data into the system. They are primarily managed through:
*   **BigQuery Data Models:** Defined in YAML files, these represent the schema and structure of data directly accessible via BigQuery.
*   **Staging SQL Scripts:** These scripts ([`sql/base/`](sql/base/)) perform initial cleaning, deduplication, and basic transformations on raw data, preparing it for further modeling.

The relationship is hierarchical: raw data resides in BigQuery (defined by models), and staging scripts then process this raw data.

## Component: BigQuery Data Models

This component defines the structure and connection details for various BigQuery datasets and tables, acting as direct interfaces to raw or external data.

### Purpose
To provide a structured and version-controlled definition of external BigQuery tables and views, enabling their integration into the data modeling pipeline. These YAML files act as metadata layers over the actual BigQuery resources.

### Internal Parts
The primary internal parts are `.yaml` files that define topics, models, and relationships within the BigQuery context.

*   **[model.yaml](omni/bigquery_connection_v2/model.yaml)**: This file likely defines the overall BigQuery data model, potentially including project and dataset configurations.
*   **[dcm__dcm_linkedview2.topic.yaml](omni/bigquery_connection_v2/dcm__dcm_linkedview2.topic.yaml)**: Defines a "topic" related to DCM (DoubleClick Campaign Manager) linked views, indicating a specific data stream or logical grouping.
*   **[omni__query_copy.topic.yaml](omni/bigquery_connection_v2/omni__query_copy.topic.yaml)**: Another topic definition, possibly for a copied or temporary query output.
*   **[relationships.yaml](omni/bigquery_connection_v2/relationships.yaml)**: This file would define relationships between different entities or tables within the BigQuery data model, crucial for joining and understanding data flows.
*   **DCM Views ([omni/bigquery_connection_v2/DCM/](omni/bigquery_connection_v2/DCM/))**: This subdirectory contains specific view definitions for DCM data.
    *   **[20240424_dcmcostmodel_v3.view.yaml](omni/bigquery_connection_v2/DCM/20240424_dcmcostmodel_v3.view.yaml)**: A versioned view definition for a DCM cost model.
    *   **[20250423_costmodel_view_v2.view.yaml](omni/bigquery_connection_v2/DCM/20250423_costmodel_view_v2.view.yaml)**: Another versioned DCM cost model view.
    *   **[20250423_costmodel_view.view.yaml](omni/bigquery_connection_v2/DCM/20250423_costmodel_view.view.yaml)**: An earlier version of the DCM cost model view.
    *   **[dcm_linkedview.view.yaml](omni/bigquery_connection_v2/DCM/dcm_linkedview.view.yaml)**: A general DCM linked view definition.
    *   **[dcm_linkedview2.view.yaml](omni/bigquery_connection_v2/DCM/dcm_linkedview2.view.yaml)**: A second version of the DCM linked view.
    *   **[omni__query_copy.query.view.yaml](omni/bigquery_connection_v2/DCM/omni__query_copy.query.view.yaml)**: A view derived from a copied query.
    *   **[omni__query.query.view.yaml](omni/bigquery_connection_v2/DCM/omni__query.query.view.yaml)**: A view derived from a general query.
*   **giant-spoon-299605.data_model_2025 Views ([omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/))**: This directory contains view definitions for a specific BigQuery project and data model. These files define how various raw data sources (like basis sheets, DCM data, UTMs) are exposed as views.
    *   **[basis_gsheet_table.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/basis_gsheet_table.view.yaml)**: View for a basis Google Sheet table.
    *   **[basis_gsheet2.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/basis_gsheet2.view.yaml)**: Another basis Google Sheet view.
    *   **[basis_master.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/basis_master.view.yaml)**: View for a basis master table.
    *   **[basis_master2.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/basis_master2.view.yaml)**: Another basis master table view.
    *   **[basis_merge_log.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/basis_merge_log.view.yaml)**: View for a basis merge log.
    *   **[dcm_test2.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/dcm_test2.view.yaml)**: A test view for DCM data.
    *   **[latest_dcm_table.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/latest_dcm_table.view.yaml)**: View for the latest DCM table.
    *   **[md.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/md.view.yaml)**: A view likely related to master data.
    *   **[new_md.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/new_md.view.yaml)**: A new master data view.
    *   **[offlinedata_gsheet.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/offlinedata_gsheet.view.yaml)**: View for offline data from a Google Sheet.
    *   **[utms2.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/utms2.view.yaml)**: View for UTM data.
*   **Prisma Views ([omni/bigquery_connection_v2/Prisma/](omni/bigquery_connection_v2/Prisma/))**: Contains view definitions for Prisma data.
    *   **[prisma_processed_view.view.yaml](omni/bigquery_connection_v2/Prisma/prisma_processed_view.view.yaml)**: View for processed Prisma data.
    *   **[prisma_processed.view.yaml](omni/bigquery_connection_v2/Prisma/prisma_processed.view.yaml)**: View for raw Prisma data.

### External Relationships
These BigQuery data models serve as the direct upstream source for the **Staging SQL Scripts** component, which then queries and transforms the data defined by these views.

## Component: Staging SQL Scripts

This component consists of SQL scripts designed to perform initial transformations, cleaning, and standardization of raw data ingested from the BigQuery data models.

### Purpose
To create clean, consistent, and de-duplicated staging tables from raw upstream data, making it suitable for subsequent joins and analytical modeling. They act as an intermediate layer between raw data and refined data marts.

### Internal Parts
The internal parts are individual SQL files, each responsible for a specific staging process.

*   **Basis Staging ([sql/base/basis/](sql/base/basis/))**:
    *   **[load__basis_gsheet_to_master2.sql](sql/base/basis/load__basis_gsheet_to_master2.sql)**: Likely loads data from a basis Google Sheet into a master table.
    *   **[load_basis_utms_union.sql](util/basis_utms/essential/load_basis_utms_union.sql)**: Unions UTM data related to basis.
    *   **[stg__basis__delivery.sql](sql/base/basis/stg__basis__delivery.sql)**: Staging script for basis delivery data.
    *   **[stg__basis__utms.sql](util/basis_utms/essential/stg__basis__utms.sql)**: Staging script for basis UTM data.
    *   **[stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql)**: A second-stage staging script for basis data combined with UTMs.
*   **DCM Staging ([sql/base/dcm/](sql/base/dcm/))**:
    *   **[20250505_costModel_v5.sql](sql/base/dcm/20250505_costModel_v5.sql)**: A versioned SQL script for a DCM cost model.
    *   **[landing_dcm_updateMasterData.sql](sql/base/dcm/landing_dcm_updateMasterData.sql)**: Updates master data for DCM.
    *   **[mart__delivery__join_dcm_utms_prisma.sql](sql/base/dcm/mart__delivery__join_dcm_utms_prisma.sql)**: This script, despite being in `dcm/`, appears to be a mart-level join, indicating how DCM data is integrated with other sources.
    *   **[stg__dcm__utms.sql](sql/base/dcm/stg__dcm__utms.sql)**: Staging script for DCM UTM data.
*   **Olipop Staging ([olipop/](olipop/))**:
    *   **[stg__ad_group_history_deduped.sql](olipop/stg__ad_group_history_deduped.sql)**: Staging script for deduplicated ad group history.
    *   **[stg__olipop_vidlength.sql](olipop/stg__olipop_vidlength.sql)**: Staging script for Olipop video length data.
*   **General Staging ([sql/stg/](sql/stg/))**:
    *   **[stg__olipop_videoviews_crossplatform.sql](sql/stg/stg__olipop_videoviews_crossplatform.sql)**: Staging script for cross-platform video views.
    *   **[stg_oli_meta_daily_and_vv2_gt.sql](sql/stg/stg_oli_meta_daily_and_vv2_gt.sql)**: Staging script for daily metadata and video views.
*   **TikTok Staging ([sql/tiktok/](sql/tiktok/))**:
    *   **[int__tiktok_combined_history_dedupe.sql](sql/tiktok/int__tiktok_combined_history_dedupe.sql)**: Intermediate staging for deduplicated TikTok history.
    *   **[stg3__lifetime_unioned.sql](sql/tiktok/stg3__lifetime_unioned.sql)**: Third-stage staging for unioned lifetime data.

### External Relationships
The output of these staging scripts (the created staging tables) serves as the primary input for downstream data marts and analytical models, such as those found in the `sql/marts/` directory. They transform the raw data from the **BigQuery Data Models** into a more usable format.
