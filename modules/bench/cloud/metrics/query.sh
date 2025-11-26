#!/bin/bash
# Render or run benchmark KQL queries against Azure Log Analytics
# Usage: ./query.sh <command> <query-type> <benchmark> [terraform.tfvars]
#   command:    render | run
#   query-type: anomaly | dashboard
#   benchmark:  tpch | yakbench | readings | auctionmark
#
# Examples:
#   ./query.sh render anomaly tpch
#   ./query.sh render dashboard yakbench
#   ./query.sh run anomaly readings

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMAND="${1:-}"
QUERY_TYPE="${2:-}"
BENCHMARK="${3:-}"
TFVARS_FILE="${4:-terraform.tfvars}"
MAIN_TF="${SCRIPT_DIR}/main.tf"

usage() {
  echo "Usage: $0 <command> <query-type> <benchmark> [terraform.tfvars]" >&2
  echo "  command:    render | run" >&2
  echo "  query-type: anomaly | dashboard" >&2
  echo "  benchmark:  tpch | yakbench | readings | auctionmark" >&2
  echo "" >&2
  echo "Examples:" >&2
  echo "  $0 render anomaly tpch       # Output the KQL query" >&2
  echo "  $0 run dashboard yakbench    # Run query against Azure" >&2
  echo "  $0 run anomaly readings      # Run query against Azure" >&2
  exit 1
}

# Validate parameters
if [[ -z "$COMMAND" || -z "$QUERY_TYPE" || -z "$BENCHMARK" ]]; then
  usage
fi

if [[ "$COMMAND" != "render" && "$COMMAND" != "run" ]]; then
  echo "Error: command must be 'render' or 'run'" >&2
  usage
fi

if [[ "$QUERY_TYPE" != "anomaly" && "$QUERY_TYPE" != "dashboard" ]]; then
  echo "Error: query-type must be 'anomaly' or 'dashboard'" >&2
  usage
fi

if [[ "$BENCHMARK" != "tpch" && "$BENCHMARK" != "yakbench" && "$BENCHMARK" != "readings" && "$BENCHMARK" != "auctionmark" ]]; then
  echo "Error: benchmark must be 'tpch', 'yakbench', 'readings', or 'auctionmark'" >&2
  usage
fi

if [[ ! -f "$TFVARS_FILE" ]]; then
  echo "Error: $TFVARS_FILE not found" >&2
  usage
fi

if [[ ! -f "$MAIN_TF" ]]; then
  echo "Error: $MAIN_TF not found" >&2
  exit 1
fi

# Parse variables from tfvars (handles basic cases)
get_var() {
  local var_name="$1"
  local default="$2"
  local value
  # Extract value, strip inline comments (# ...), quotes, and whitespace
  value=$(grep "^${var_name}\\s*=" "$TFVARS_FILE" 2>/dev/null | sed 's/.*=\s*//' | sed 's/\s*#.*//' | sed 's/^\s*"//' | sed 's/"\s*$//' | sed "s/^\s*'//" | sed "s/'\s*$//" | sed 's/\s*$//' || true)
  if [[ -z "$value" ]]; then
    echo "$default"
  else
    echo "$value"
  fi
}

# Get variables with defaults from variables.tf
ANOMALY_BASELINE_N=$(get_var "anomaly_baseline_n" "30")
ANOMALY_SIGMA=$(get_var "anomaly_sigma" "2")
ANOMALY_NEW_NORMAL_RELATIVE_THRESHOLD=$(get_var "anomaly_new_normal_relative_threshold" "0.05")

# Benchmark-specific configuration (mirrors local.benchmarks in main.tf)
case "$BENCHMARK" in
  tpch)
    BENCH_NAME="TPC-H (OLAP)"
    PARAM_NAME="scale-factor"
    PARAM_PATH="parameters['scale-factor']"
    PARAM_VALUE=$(get_var "tpch_anomaly_scale_factor" "1.0")
    PARAM_VAR="tpch_anomaly_scale_factor"
    PARAM_IS_STRING=false
    METRIC_PATH="'time-taken-ms'"
    METRIC_NAME="duration_minutes"
    ;;
  yakbench)
    BENCH_NAME="Yakbench"
    PARAM_NAME="scale-factor"
    PARAM_PATH="parameters['scale-factor']"
    PARAM_VALUE=$(get_var "yakbench_anomaly_scale_factor" "1.0")
    PARAM_VAR="yakbench_anomaly_scale_factor"
    PARAM_IS_STRING=false
    METRIC_PATH="'time-taken-ms'"
    METRIC_NAME="duration_minutes"
    ;;
  readings)
    BENCH_NAME="Readings benchmarks"
    PARAM_NAME="devices"
    PARAM_PATH="parameters['devices']"
    PARAM_VALUE=$(get_var "readings_anomaly_devices" "10000")
    PARAM_VAR="readings_anomaly_devices"
    PARAM_IS_STRING=false
    METRIC_PATH="'time-taken-ms'"
    METRIC_NAME="duration_minutes"
    ;;
  auctionmark)
    BENCH_NAME="Auction Mark OLTP"
    PARAM_NAME="duration"
    PARAM_PATH="parameters['duration']"
    PARAM_VALUE=$(get_var "auctionmark_anomaly_duration" "PT30M")
    PARAM_VAR="auctionmark_anomaly_duration"
    PARAM_IS_STRING=true
    METRIC_PATH="'throughput'"
    METRIC_NAME="throughput"
    ;;
esac

# Generate filter expression based on param type (mirrors local.param_filter_expr in main.tf)
if [[ "$PARAM_IS_STRING" == "true" ]]; then
  PARAM_FILTER_EXPR="filter_param = tostring(log.${PARAM_PATH})
                      | where benchmark == \"${BENCH_NAME}\" and filter_param == \"${PARAM_VALUE}\""
else
  PARAM_FILTER_EXPR="filter_param = todouble(log.${PARAM_PATH})
                      | where benchmark == \"${BENCH_NAME}\" and filter_param == ${PARAM_VALUE}"
fi

# Extract or generate query
extract_query() {
  if [[ "$QUERY_TYPE" == "anomaly" ]]; then
    # Extract the KQL query from the bench_anomaly resource (uses for_each)
    awk '
      /^resource "azapi_resource" "bench_anomaly"/ { in_resource = 1 }
      in_resource && /^ *"query" *= *<<-KQL$/ { in_query = 1; next }
      in_resource && in_query && /^ *KQL$/ { in_query = 0; in_resource = 0; next }
      in_query { print }
    ' "$MAIN_TF"
  else
    # Dashboard query - generate directly (mirrors local.dashboard_queries in main.tf)
    # auctionmark (param_is_string=true) uses string comparison (ISO duration), others use numeric comparison
    if [[ "$PARAM_IS_STRING" == "true" ]]; then
      cat <<KQL
ContainerLog
| where LogEntry startswith "{" and LogEntry has "benchmark"
| extend log = parse_json(LogEntry)
| where isnotnull(log.benchmark) and log.stage != "init"
| extend benchmark = tostring(log.benchmark),
         filter_param = tostring(log.\${each.value.param_path}),
         \${each.value.metric_name} = todouble(log[\${each.value.metric_path}])
| where benchmark == "\${each.value.name}" and filter_param == "\${each.value.param_value}"
| top \${var.anomaly_baseline_n} by TimeGenerated desc
| order by TimeGenerated asc
| project TimeGenerated, \${each.value.metric_name}
KQL
    else
      cat <<KQL
ContainerLog
| where LogEntry startswith "{" and LogEntry has "benchmark"
| extend log = parse_json(LogEntry)
| where isnotnull(log.benchmark) and log.stage != "init"
| extend benchmark = tostring(log.benchmark),
         filter_param = todouble(log.\${each.value.param_path}),
         duration_ms = todouble(log[\${each.value.metric_path}])
| where benchmark == "\${each.value.name}" and filter_param == \${each.value.param_value}
| top \${var.anomaly_baseline_n} by TimeGenerated desc
| extend duration_minutes = todouble(duration_ms) / 60000
| order by TimeGenerated asc
| project TimeGenerated, duration_minutes
KQL
    fi
  fi
}

QUERY_TEMPLATE=$(extract_query)

if [[ -z "$QUERY_TEMPLATE" ]]; then
  echo "Error: Could not extract $QUERY_TYPE query for $BENCHMARK from $MAIN_TF" >&2
  exit 1
fi

# Find the minimum leading whitespace (ignoring empty lines)
MIN_INDENT=$(echo "$QUERY_TEMPLATE" | grep -v '^[[:space:]]*$' | sed 's/^\([[:space:]]*\).*/\1/' | awk '{print length}' | sort -n | head -1)

# Substitute the Terraform variable references with actual values and strip leading whitespace
# Handle both for_each (each.value.*) and direct variable references
render_query() {
  # Escape special characters in PARAM_FILTER_EXPR for sed replacement
  # Replace newlines with literal \n, escape &, /, and \
  local ESCAPED_FILTER_EXPR
  ESCAPED_FILTER_EXPR=$(printf '%s' "$PARAM_FILTER_EXPR" | sed 's/[&/\]/\\&/g' | sed ':a;N;$!ba;s/\n/\\n/g')

  echo "$QUERY_TEMPLATE" | \
    sed "s/^.\{${MIN_INDENT}\}//" | \
    sed "s/\${var.anomaly_baseline_n}/${ANOMALY_BASELINE_N}/g" | \
    sed "s/\${var.anomaly_sigma}/${ANOMALY_SIGMA}/g" | \
    sed "s/\${var.anomaly_new_normal_relative_threshold}/${ANOMALY_NEW_NORMAL_RELATIVE_THRESHOLD}/g" | \
    sed "s/\${var.${PARAM_VAR}}/${PARAM_VALUE}/g" | \
    sed "s/\${each.value.param_name}/${PARAM_NAME}/g" | \
    sed "s/\${each.value.param_path}/${PARAM_PATH}/g" | \
    sed "s/\${each.value.param_value}/${PARAM_VALUE}/g" | \
    sed "s/\${each.value.metric_path}/${METRIC_PATH}/g" | \
    sed "s/\${each.value.metric_name}/${METRIC_NAME}/g" | \
    sed "s/\${each.value.name}/${BENCH_NAME}/g" | \
    sed "s/\${local.param_filter_expr\[each.key\]}/${ESCAPED_FILTER_EXPR}/g"
}

# Execute based on command
if [[ "$COMMAND" == "render" ]]; then
  render_query
else
  # run command - execute query against Azure
  if ! command -v az &> /dev/null; then
    echo "Error: az cli not found. Install with: brew install azure-cli" >&2
    exit 1
  fi

  if ! command -v jq &> /dev/null; then
    echo "Error: jq not found. Install with: brew install jq" >&2
    exit 1
  fi

  RESOURCE_GROUP=$(get_var "resource_group_name" "")
  WORKSPACE_NAME=$(get_var "cluster_law_name" "")
  TIMESPAN=$(get_var "anomaly_timespan" "P30D")

  if [[ -z "$RESOURCE_GROUP" ]]; then
    echo "Error: resource_group_name not found in $TFVARS_FILE" >&2
    exit 1
  fi

  if [[ -z "$WORKSPACE_NAME" ]]; then
    echo "Error: cluster_law_name not found in $TFVARS_FILE" >&2
    exit 1
  fi

  QUERY=$(render_query)

  echo "Query type: $QUERY_TYPE" >&2
  echo "Benchmark: $BENCHMARK" >&2
  echo "" >&2
  echo "$QUERY" >&2
  echo "" >&2
  echo "Running query against workspace: $WORKSPACE_NAME (resource group: $RESOURCE_GROUP)" >&2
  echo "Timespan: $TIMESPAN" >&2
  echo "" >&2

  # Get workspace ID
  WORKSPACE_ID=$(az monitor log-analytics workspace show \
    --workspace-name "$WORKSPACE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query customerId -o tsv)

  # Run the query
  az monitor log-analytics query \
    --workspace "$WORKSPACE_ID" \
    --analytics-query "$QUERY" \
    --timespan "$TIMESPAN" \
    | jq .
fi
