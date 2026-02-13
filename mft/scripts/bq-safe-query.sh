#!/usr/bin/env bash
set -euo pipefail

MAX_ROWS="${BQ_SAFE_MAX_ROWS:-50}"
MAX_BYTES="${BQ_SAFE_MAX_BYTES:-176000000000}"
SCHEMA_MAX_ROWS="${BQ_SAFE_SCHEMA_MAX_ROWS:-10000}"
PROJECT_ID="${BQ_SAFE_PROJECT_ID:-}"
LOCATION="${BQ_SAFE_LOCATION:-US}"
FORMAT="${BQ_SAFE_FORMAT:-pretty}"
ALLOW_SELECT_STAR=0
DRY_RUN_ONLY=0
SCHEMA_ONLY_TABLE=""
SQL_INPUT=""
SQL_FILE=""
PARSED_TABLE_PROJECT=""
PARSED_TABLE_DATASET=""
PARSED_TABLE_NAME=""

parse_table_ref() {
  local raw_ref="$1"
  local table_ref=""

  table_ref="$(printf '%s' "$raw_ref" | tr -d '`' | tr -d '[:space:]')"
  PARSED_TABLE_PROJECT=""
  PARSED_TABLE_DATASET=""
  PARSED_TABLE_NAME=""

  if [[ "$table_ref" =~ ^([^:]+):([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)$ ]]; then
    PARSED_TABLE_PROJECT="${BASH_REMATCH[1]}"
    PARSED_TABLE_DATASET="${BASH_REMATCH[2]}"
    PARSED_TABLE_NAME="${BASH_REMATCH[3]}"
    return 0
  fi

  if [[ "$table_ref" =~ ^([^.]+)\.([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)$ ]]; then
    PARSED_TABLE_PROJECT="${BASH_REMATCH[1]}"
    PARSED_TABLE_DATASET="${BASH_REMATCH[2]}"
    PARSED_TABLE_NAME="${BASH_REMATCH[3]}"
    return 0
  fi

  if [[ "$table_ref" =~ ^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)$ ]]; then
    PARSED_TABLE_DATASET="${BASH_REMATCH[1]}"
    PARSED_TABLE_NAME="${BASH_REMATCH[2]}"
    return 0
  fi

  return 1
}

detect_table_ref_from_sql() {
  local sql_text="$1"
  local sql_flat=""
  local from_ref=""

  sql_flat="$(printf '%s' "$sql_text" | tr '\n' ' ')"
  from_ref="$(printf '%s\n' "$sql_flat" | sed -nE 's/.*[Ff][Rr][Oo][Mm][[:space:]]+`([^`]+)`.*/\1/p' | head -n 1)"

  if [[ -z "$from_ref" ]]; then
    from_ref="$(printf '%s\n' "$sql_flat" | sed -nE 's/.*[Ff][Rr][Oo][Mm][[:space:]]+([A-Za-z0-9_:-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+|[A-Za-z0-9_]+\.[A-Za-z0-9_]+).*/\1/p' | head -n 1)"
  fi

  printf '%s' "$from_ref"
}

print_summary_suggestions() {
  local reason="$1"
  local detected_table=""
  local table_expr="<project.dataset.table>"
  local schema_ref=""
  local script_path="./scripts/bq-safe-query.sh"

  if [[ -n "${0:-}" ]]; then
    script_path="$0"
  fi

  detected_table="$(detect_table_ref_from_sql "$SQL_INPUT")"
  if [[ -n "$detected_table" ]]; then
    table_expr="\`${detected_table}\`"
  fi

  if [[ -n "$detected_table" ]] && parse_table_ref "$detected_table"; then
    if [[ -n "$PARSED_TABLE_PROJECT" ]]; then
      schema_ref="\`${PARSED_TABLE_PROJECT}.${PARSED_TABLE_DATASET}.INFORMATION_SCHEMA.COLUMNS\`"
    elif [[ -n "$PARSED_TABLE_DATASET" ]]; then
      schema_ref="\`${PARSED_TABLE_DATASET}.INFORMATION_SCHEMA.COLUMNS\`"
    fi
  fi

  cat <<EOF >&2

Guardrail suggestion: ${reason}
Try summary-first queries:
  1) Row count:
     SELECT COUNT(*) AS row_count
     FROM ${table_expr};

  2) Date window summary (if table has a date field):
     SELECT MIN(date) AS min_date, MAX(date) AS max_date, COUNT(*) AS rows
     FROM ${table_expr};

  3) Top categories by volume:
     SELECT campaign, COUNT(*) AS rows
     FROM ${table_expr}
     GROUP BY 1
     ORDER BY rows DESC
     LIMIT 20;

  4) Narrow sample (explicit columns, small limit):
     SELECT date, campaign, partner
     FROM ${table_expr}
     ORDER BY date DESC
     LIMIT 50;
EOF

  if [[ -n "$schema_ref" && -n "$PARSED_TABLE_NAME" ]]; then
    cat <<EOF >&2

  5) Column list (schema-first exploration):
     SELECT ordinal_position, column_name, data_type
     FROM ${schema_ref}
     WHERE table_name = '${PARSED_TABLE_NAME}'
     ORDER BY ordinal_position;
EOF
  fi

  cat <<EOF >&2

How to turn off limits (use carefully):
  - One run, allow wide/large query:
    ${script_path} --allow-select-star --max-rows 1000000 --max-bytes 999999999999999 --sql "<your query>"

  - Keep defaults changed for your shell session:
    export BQ_SAFE_MAX_ROWS=1000000
    export BQ_SAFE_MAX_BYTES=999999999999999
EOF
}

usage() {
  cat <<'EOF'
Usage:
  bq-safe-query.sh [options]

Options:
  --sql "<query>"           SQL text to run.
  --file <path.sql>         SQL file to run.
  --max-rows <n>            Max rows returned to output (default: 50).
  --max-bytes <n>           Max bytes billed/processed (default: 176000000000).
  --project <project_id>    Override GCP project.
  --location <location>     BigQuery location (default: US).
  --format <fmt>            bq output format (default: pretty).
  --dry-run-only            Validate query + bytes estimate only.
  --schema-only <table>     Print all columns for a table and exit.
                            Accepted formats:
                            - project.dataset.table
                            - project:dataset.table
                            - dataset.table (uses --project or default project)
  --allow-select-star       Allow SELECT * (blocked by default).
  -h, --help                Show help.

Notes:
  - If SELECT has no LIMIT, LIMIT <max-rows> is appended automatically.
  - SELECT * is blocked by default to reduce oversized output.
  - --schema-only returns metadata rows and is capped by BQ_SAFE_SCHEMA_MAX_ROWS
    (default: 10000).
  - If a query is blocked by guardrails, summary-first query suggestions are printed.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sql)
      SQL_INPUT="${2:-}"
      shift 2
      ;;
    --file)
      SQL_FILE="${2:-}"
      shift 2
      ;;
    --max-rows)
      MAX_ROWS="${2:-}"
      shift 2
      ;;
    --max-bytes)
      MAX_BYTES="${2:-}"
      shift 2
      ;;
    --project)
      PROJECT_ID="${2:-}"
      shift 2
      ;;
    --location)
      LOCATION="${2:-}"
      shift 2
      ;;
    --format)
      FORMAT="${2:-}"
      shift 2
      ;;
    --dry-run-only)
      DRY_RUN_ONLY=1
      shift
      ;;
    --schema-only)
      SCHEMA_ONLY_TABLE="${2:-}"
      shift 2
      ;;
    --allow-select-star)
      ALLOW_SELECT_STAR=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! command -v bq >/dev/null 2>&1; then
  echo "Error: bq CLI is not installed or not in PATH." >&2
  exit 1
fi

if ! [[ "$SCHEMA_MAX_ROWS" =~ ^[0-9]+$ ]] || [[ "$SCHEMA_MAX_ROWS" -le 0 ]]; then
  echo "Error: BQ_SAFE_SCHEMA_MAX_ROWS must be a positive integer." >&2
  exit 1
fi

if [[ -n "$SQL_INPUT" && -n "$SQL_FILE" ]]; then
  echo "Error: use only one of --sql or --file." >&2
  exit 1
fi

if [[ -n "$SCHEMA_ONLY_TABLE" ]]; then
  if [[ -n "$SQL_INPUT" || -n "$SQL_FILE" ]]; then
    echo "Error: --schema-only cannot be combined with --sql or --file." >&2
    exit 1
  fi

  TABLE_REF="$(printf '%s' "$SCHEMA_ONLY_TABLE" | tr -d '`' | tr -d '[:space:]')"
  if ! parse_table_ref "$TABLE_REF"; then
    echo "Error: invalid --schema-only table reference: ${SCHEMA_ONLY_TABLE}" >&2
    echo "Use project.dataset.table, project:dataset.table, or dataset.table." >&2
    exit 1
  fi

  INFO_SCHEMA_REF=""
  RUN_PROJECT="${PROJECT_ID:-}"
  if [[ -n "$PARSED_TABLE_PROJECT" ]]; then
    INFO_SCHEMA_REF="${PARSED_TABLE_PROJECT}.${PARSED_TABLE_DATASET}.INFORMATION_SCHEMA.COLUMNS"
    if [[ -z "$RUN_PROJECT" ]]; then
      RUN_PROJECT="$PARSED_TABLE_PROJECT"
    fi
  else
    INFO_SCHEMA_REF="${PARSED_TABLE_DATASET}.INFORMATION_SCHEMA.COLUMNS"
  fi

  SCHEMA_SQL="SELECT ordinal_position, column_name, data_type, is_nullable
FROM \`${INFO_SCHEMA_REF}\`
WHERE table_name = '${PARSED_TABLE_NAME}'
ORDER BY ordinal_position"

  BQ_SCHEMA_ARGS=(--format="$FORMAT")
  if [[ -n "$RUN_PROJECT" ]]; then
    BQ_SCHEMA_ARGS+=(--project_id="$RUN_PROJECT")
  fi
  if [[ -n "$LOCATION" ]]; then
    BQ_SCHEMA_ARGS+=(--location="$LOCATION")
  fi
  BQ_SCHEMA_ARGS+=(query -n "$SCHEMA_MAX_ROWS" --use_legacy_sql=false "$SCHEMA_SQL")

  bq "${BQ_SCHEMA_ARGS[@]}"
  exit 0
fi

if [[ -n "$SQL_FILE" ]]; then
  if [[ ! -f "$SQL_FILE" ]]; then
    echo "Error: SQL file not found: $SQL_FILE" >&2
    exit 1
  fi
  SQL_INPUT="$(cat "$SQL_FILE")"
elif [[ -z "$SQL_INPUT" ]]; then
  if [[ ! -t 0 ]]; then
    SQL_INPUT="$(cat)"
  fi
fi

if [[ -z "${SQL_INPUT//[[:space:]]/}" ]]; then
  echo "Error: no SQL provided. Use --sql, --file, or stdin." >&2
  exit 1
fi

if ! [[ "$MAX_ROWS" =~ ^[0-9]+$ ]] || [[ "$MAX_ROWS" -le 0 ]]; then
  echo "Error: --max-rows must be a positive integer." >&2
  exit 1
fi

if ! [[ "$MAX_BYTES" =~ ^[0-9]+$ ]] || [[ "$MAX_BYTES" -le 0 ]]; then
  echo "Error: --max-bytes must be a positive integer." >&2
  exit 1
fi

SQL_FLAT="$(printf '%s' "$SQL_INPUT" | tr '\n' ' ')"

if [[ "$ALLOW_SELECT_STAR" -eq 0 ]] && printf '%s\n' "$SQL_FLAT" | grep -Eiq '^[[:space:]]*select[[:space:]]+\*'; then
  echo "Error: SELECT * is blocked. Specify explicit columns or use --allow-select-star." >&2
  print_summary_suggestions "SELECT * blocked"
  exit 1
fi

if printf '%s\n' "$SQL_FLAT" | grep -Eiq '^[[:space:]]*select[[:space:]]' && ! printf '%s\n' "$SQL_FLAT" | grep -Eiq '\blimit[[:space:]]+[0-9]+'; then
  SQL_INPUT="${SQL_INPUT%;}"
  SQL_INPUT="${SQL_INPUT}"$'\n'"LIMIT ${MAX_ROWS};"
fi

BQ_ARGS=(--format=prettyjson)
if [[ -n "$PROJECT_ID" ]]; then
  BQ_ARGS+=(--project_id="$PROJECT_ID")
fi
if [[ -n "$LOCATION" ]]; then
  BQ_ARGS+=(--location="$LOCATION")
fi
BQ_ARGS+=(query --dry_run --use_legacy_sql=false "$SQL_INPUT")

DRY_RUN_JSON="$(bq "${BQ_ARGS[@]}")"
TOTAL_BYTES="$(printf '%s\n' "$DRY_RUN_JSON" | sed -n 's/.*"totalBytesProcessed":[[:space:]]*"\{0,1\}\([0-9][0-9]*\)".*/\1/p' | head -n 1)"

if [[ -n "$TOTAL_BYTES" ]] && [[ "$TOTAL_BYTES" -gt "$MAX_BYTES" ]]; then
  echo "Error: query would process ${TOTAL_BYTES} bytes, which exceeds --max-bytes=${MAX_BYTES}." >&2
  print_summary_suggestions "Bytes cap exceeded (${TOTAL_BYTES} > ${MAX_BYTES})"
  exit 1
fi

echo "Dry run passed. Estimated bytes processed: ${TOTAL_BYTES:-unknown}"

if [[ "$DRY_RUN_ONLY" -eq 1 ]]; then
  exit 0
fi

BQ_RUN_ARGS=(--format="$FORMAT" --max_rows_per_request="$MAX_ROWS")
if [[ -n "$PROJECT_ID" ]]; then
  BQ_RUN_ARGS+=(--project_id="$PROJECT_ID")
fi
if [[ -n "$LOCATION" ]]; then
  BQ_RUN_ARGS+=(--location="$LOCATION")
fi
BQ_RUN_ARGS+=(query -n "$MAX_ROWS" --use_legacy_sql=false --maximum_bytes_billed="$MAX_BYTES" "$SQL_INPUT")

bq "${BQ_RUN_ARGS[@]}"
