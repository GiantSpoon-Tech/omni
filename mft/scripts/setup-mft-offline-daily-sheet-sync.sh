#!/usr/bin/env bash
set -euo pipefail


# * SECTION [1]: CONFIGURATION
  # Description: Default values for the scheduled sync and SQL template.
  PROJECT_ID="${BQ_PROJECT_ID:-looker-studio-pro-452620}"
  DEST_DATASET_ID="${BQ_DEST_DATASET_ID:-${BQ_DATASET_ID:-mass_mutual_mft_ext}}"
  DEST_TABLE="${BQ_DEST_TABLE:-mft_offline}"
  STAGING_DATASET_ID="${BQ_STAGING_DATASET_ID:-repo_stg}"
  STAGING_TABLE="${BQ_STAGING_TABLE:-stg__mm__mft_offline_connected_gsheet}"
  SQL_TEMPLATE_FILE="${BQ_SQL_TEMPLATE_FILE:-scripts/sql/mft_offline_daily_sheet_sync.sql}"
  LOCATION="${BQ_LOCATION:-US}"
  DISPLAY_NAME="${BQ_TRANSFER_DISPLAY_NAME:-mft_offline_daily_sheet_sync}"
  SCHEDULE="${BQ_TRANSFER_SCHEDULE:-every day 06:00}"
  WRITE_DISPOSITION="${BQ_WRITE_DISPOSITION:-WRITE_TRUNCATE}"
  SHEET_URL="${BQ_SHEET_URL:-https://docs.google.com/spreadsheets/d/15DddW291w_O7WWv8F0AcOcumEMYJSdm9hWKU9vE5WPQ/edit?gid=1999096908#gid=1999096908}"
  SHEET_RANGE="${BQ_SHEET_RANGE:-'[NEW] INTERNAL | COMBINED DATA'!A:U}"
  SKIP_LEADING_ROWS="${BQ_SKIP_LEADING_ROWS:-1}"
  TRANSFER_CONFIG_ID="${BQ_TRANSFER_CONFIG_ID:-}"
  SERVICE_ACCOUNT_NAME="${BQ_SERVICE_ACCOUNT_NAME:-}"
  PRINT_RENDERED_SQL=0


# * SECTION [2]: HELPERS
  # Description: Utility functions for usage, validation, and SQL templating.

  # ? Print script usage
    usage() {
      cat <<'EOF'
Usage:
  setup-mft-offline-daily-sheet-sync.sh [options]

Options:
  --project <id>            GCP project id (default: looker-studio-pro-452620)
  --dataset <id>            Destination BigQuery dataset (default: mass_mutual_mft_ext)
  --table <name>            Destination native table (default: mft_offline)
  --staging-dataset <id>    Staging dataset for connected sheet external table
                            (default: repo_stg)
  --staging-table <name>    Staging external table name
                            (default: stg__mm__mft_offline_connected_gsheet)
  --sql-file <path>         SQL template file (default: scripts/sql/mft_offline_daily_sheet_sync.sql)
  --location <loc>          BigQuery location (default: US)
  --display-name <name>     Scheduled query display name
                            (default: mft_offline_daily_sheet_sync)
  --schedule <string>       Schedule expression (default: "every day 06:00")
  --write-disposition <v>   Scheduled query write mode:
                            WRITE_TRUNCATE | WRITE_APPEND | WRITE_EMPTY
                            (default: WRITE_TRUNCATE)
  --sheet-url <url>         Google Sheets URL
  --sheet-range <range>     Sheet range (default: "'[NEW] INTERNAL | COMBINED DATA'!A:U")
  --skip-leading-rows <n>   Rows to skip in sheet (default: 1)
  --transfer-config-id <id> Existing transfer config resource name to update
                            (example: projects/.../locations/us/transferConfigs/...)
  --service-account <email> Run scheduled query as this service account
  --print-sql               Print rendered SQL before create/update
  -h, --help                Show help

Examples:
  ./scripts/setup-mft-offline-daily-sheet-sync.sh

  ./scripts/setup-mft-offline-daily-sheet-sync.sh \
    --transfer-config-id "projects/671028410185/locations/us/transferConfigs/699421ab-0000-2129-a27e-883d24f0f1b8"

  ./scripts/setup-mft-offline-daily-sheet-sync.sh \
    --schedule "every day 08:00" \
    --service-account "bq-scheduler@looker-studio-pro-452620.iam.gserviceaccount.com"
EOF
    }

  # ? Escape a value for SQL single-quoted string literals
    sql_escape_single() {
      printf '%s' "$1" | sed "s/'/''/g"
    }

  # ? Escape a value for SQL double-quoted string literals
    sql_escape_double() {
      printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
    }

  # ? Escape replacement values used by sed
    sed_escape_replacement() {
      printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
    }

  # ? Fail fast with a helpful message
    die() {
      echo "Error: $*" >&2
      exit 1
    }


# * SECTION [3]: ARGUMENT PARSING
  # Description: Parse optional command-line overrides.

  # ? Parse flags
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --project)
          PROJECT_ID="${2:-}"
          shift 2
          ;;
        --dataset)
          DEST_DATASET_ID="${2:-}"
          shift 2
          ;;
        --staging-dataset)
          STAGING_DATASET_ID="${2:-}"
          shift 2
          ;;
        --table)
          DEST_TABLE="${2:-}"
          shift 2
          ;;
        --staging-table)
          STAGING_TABLE="${2:-}"
          shift 2
          ;;
        --sql-file)
          SQL_TEMPLATE_FILE="${2:-}"
          shift 2
          ;;
        --location)
          LOCATION="${2:-}"
          shift 2
          ;;
        --display-name)
          DISPLAY_NAME="${2:-}"
          shift 2
          ;;
        --schedule)
          SCHEDULE="${2:-}"
          shift 2
          ;;
        --write-disposition)
          WRITE_DISPOSITION="${2:-}"
          shift 2
          ;;
        --sheet-url)
          SHEET_URL="${2:-}"
          shift 2
          ;;
        --sheet-range)
          SHEET_RANGE="${2:-}"
          shift 2
          ;;
        --skip-leading-rows)
          SKIP_LEADING_ROWS="${2:-}"
          shift 2
          ;;
        --transfer-config-id)
          TRANSFER_CONFIG_ID="${2:-}"
          shift 2
          ;;
        --service-account)
          SERVICE_ACCOUNT_NAME="${2:-}"
          shift 2
          ;;
        --print-sql)
          PRINT_RENDERED_SQL=1
          shift
          ;;
        -h|--help)
          usage
          exit 0
          ;;
        *)
          die "Unknown option: $1"
          ;;
      esac
    done


# * SECTION [4]: VALIDATION
  # Description: Ensure all required tools and values are available before running.

  # ? Validate required CLI tool
    command -v bq >/dev/null 2>&1 || die "bq CLI is required but not found in PATH."
    command -v python3 >/dev/null 2>&1 || die "python3 is required but not found in PATH."

  # ? Validate required values
    [[ -n "$PROJECT_ID" ]] || die "--project cannot be empty."
    [[ -n "$DEST_DATASET_ID" ]] || die "--dataset cannot be empty."
    [[ -n "$DEST_TABLE" ]] || die "--table cannot be empty."
    [[ -n "$STAGING_DATASET_ID" ]] || die "--staging-dataset cannot be empty."
    [[ -n "$STAGING_TABLE" ]] || die "--staging-table cannot be empty."
    [[ -n "$SQL_TEMPLATE_FILE" ]] || die "--sql-file cannot be empty."
    [[ -f "$SQL_TEMPLATE_FILE" ]] || die "SQL template file not found: $SQL_TEMPLATE_FILE"
    [[ -n "$LOCATION" ]] || die "--location cannot be empty."
    [[ -n "$DISPLAY_NAME" ]] || die "--display-name cannot be empty."
    [[ -n "$SCHEDULE" ]] || die "--schedule cannot be empty."
    [[ "$WRITE_DISPOSITION" =~ ^WRITE_(TRUNCATE|APPEND|EMPTY)$ ]] || die "--write-disposition must be WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY."
    [[ -n "$SHEET_URL" ]] || die "--sheet-url cannot be empty."
    [[ -n "$SHEET_RANGE" ]] || die "--sheet-range cannot be empty."
    [[ "$SKIP_LEADING_ROWS" =~ ^[0-9]+$ ]] || die "--skip-leading-rows must be a non-negative integer."


# * SECTION [5]: BUILD SQL AND STAGING TABLE
  # Description: Render scheduled query SQL and ensure the staging external table exists.

  # ? Build fully-qualified table names
    FULL_STAGING_TABLE="${PROJECT_ID}.${STAGING_DATASET_ID}.${STAGING_TABLE}"
    FULL_DEST_TABLE="${PROJECT_ID}.${DEST_DATASET_ID}.${DEST_TABLE}"

  # ? Build escaped SQL literals
    SHEET_URL_SQL="$(sql_escape_single "$SHEET_URL")"
    SHEET_RANGE_SQL="$(sql_escape_double "$SHEET_RANGE")"

  # ? Escape template replacements for sed
    FULL_STAGING_TABLE_SED="$(sed_escape_replacement "$FULL_STAGING_TABLE")"
    FULL_DEST_TABLE_SED="$(sed_escape_replacement "$FULL_DEST_TABLE")"
    SHEET_URL_SQL_SED="$(sed_escape_replacement "$SHEET_URL_SQL")"
    SHEET_RANGE_SQL_SED="$(sed_escape_replacement "$SHEET_RANGE_SQL")"

  # ? Render template placeholders into executable SQL
    SCHEDULED_SQL="$(
      sed \
        -e "s|__FULL_STAGING_TABLE__|${FULL_STAGING_TABLE_SED}|g" \
        -e "s|__FULL_DEST_TABLE__|${FULL_DEST_TABLE_SED}|g" \
        -e "s|__SHEET_URL_SQL__|${SHEET_URL_SQL_SED}|g" \
        -e "s|__SHEET_RANGE_SQL__|${SHEET_RANGE_SQL_SED}|g" \
        -e "s|__SKIP_LEADING_ROWS__|${SKIP_LEADING_ROWS}|g" \
        "$SQL_TEMPLATE_FILE"
    )"

  # ? Build SQL that (re)creates the staging external table
    read -r -d '' STAGING_EXTERNAL_SQL <<EOF || true
CREATE OR REPLACE EXTERNAL TABLE \`${FULL_STAGING_TABLE}\`
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = ['${SHEET_URL_SQL}'],
  skip_leading_rows = ${SKIP_LEADING_ROWS},
  sheet_range = "${SHEET_RANGE_SQL}"
);
EOF

  # ? Ensure staging external table exists before scheduling query runs
    bq --location="${LOCATION}" --project_id="${PROJECT_ID}" query --use_legacy_sql=false "${STAGING_EXTERNAL_SQL}"

  # ? Optionally print rendered SQL for review/debugging
    if [[ "$PRINT_RENDERED_SQL" -eq 1 ]]; then
      cat <<EOF
----- BEGIN STAGING EXTERNAL SQL -----
${STAGING_EXTERNAL_SQL}
----- END STAGING EXTERNAL SQL -----
----- BEGIN RENDERED SQL -----
${SCHEDULED_SQL}
----- END RENDERED SQL -----
EOF
    fi

  # ? Encode query params for transfer config API
    PARAMS_JSON="$(
      QUERY_TEXT="$SCHEDULED_SQL" DEST_TABLE_TEMPLATE="$DEST_TABLE" WRITE_MODE="$WRITE_DISPOSITION" python3 - <<'PY'
import json
import os

print(json.dumps({
    "query": os.environ["QUERY_TEXT"],
    "destination_table_name_template": os.environ["DEST_TABLE_TEMPLATE"],
    "write_disposition": os.environ["WRITE_MODE"],
}))
PY
    )"


# * SECTION [6]: CREATE TRANSFER CONFIG
  # Description: Create a new transfer config or update an existing one.

  # ? Create new transfer when no transfer id is provided
    if [[ -z "$TRANSFER_CONFIG_ID" ]]; then
      BQ_CREATE_ARGS=(
        --location="${LOCATION}"
        mk
        --transfer_config
        --project_id="${PROJECT_ID}"
        --target_dataset="${DEST_DATASET_ID}"
        --display_name="${DISPLAY_NAME}"
        --schedule="${SCHEDULE}"
        --params="${PARAMS_JSON}"
        --data_source=scheduled_query
      )

      if [[ -n "$SERVICE_ACCOUNT_NAME" ]]; then
        BQ_CREATE_ARGS+=(--service_account_name="${SERVICE_ACCOUNT_NAME}")
      fi

      bq "${BQ_CREATE_ARGS[@]}"
      ACTION_TEXT="created"
      TARGET_TEXT="(new config with display name: ${DISPLAY_NAME})"
    else
      BQ_UPDATE_ARGS=(
        --location="${LOCATION}"
        update
        --transfer_config
        --project_id="${PROJECT_ID}"
        --target_dataset="${DEST_DATASET_ID}"
        --display_name="${DISPLAY_NAME}"
        --schedule="${SCHEDULE}"
        --params="${PARAMS_JSON}"
      )

      if [[ -n "$SERVICE_ACCOUNT_NAME" ]]; then
        BQ_UPDATE_ARGS+=(--service_account_name="${SERVICE_ACCOUNT_NAME}")
      fi

      bq "${BQ_UPDATE_ARGS[@]}" "${TRANSFER_CONFIG_ID}"
      ACTION_TEXT="updated"
      TARGET_TEXT="${TRANSFER_CONFIG_ID}"
    fi


# * SECTION [7]: OUTPUT
  # Description: Summarize what was created and how to verify it.

  # ? Print a concise success summary
    cat <<EOF
Scheduled query ${ACTION_TEXT}.
Project:      ${PROJECT_ID}
Dest dataset: ${DEST_DATASET_ID}
Dest table:   ${DEST_TABLE}
Stg dataset:  ${STAGING_DATASET_ID}
Stg table:    ${STAGING_TABLE}
SQL file:     ${SQL_TEMPLATE_FILE}
Sheet range:  ${SHEET_RANGE}
Schedule:     ${SCHEDULE}
Write mode:   ${WRITE_DISPOSITION}
Location:     ${LOCATION}
Target:       ${TARGET_TEXT}

Verification:
  bq ls --transfer_config --transfer_location=${LOCATION} --project_id=${PROJECT_ID}
  bq head -n 5 ${PROJECT_ID}:${DEST_DATASET_ID}.${DEST_TABLE}

Important:
  Ensure the scheduled query identity has Google Sheets access to the source file:
  ${SHEET_URL}
EOF
