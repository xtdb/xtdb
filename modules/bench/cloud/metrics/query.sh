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
  echo "  query-type: anomaly | dashboard | queries-cold | queries-hot |" >&2
  echo "              profile-global | profile-max-user | profile-mean-user" >&2
  echo "  benchmark:  tpch | yakbench | readings | auctionmark" >&2
  echo "" >&2
  echo "Examples:" >&2
  echo "  $0 render anomaly tpch          # Output the KQL query" >&2
  echo "  $0 run dashboard yakbench       # Run query against Azure" >&2
  echo "  $0 run anomaly readings         # Run query against Azure" >&2
  echo "  $0 run queries-cold tpch        # TPC-H individual cold query times" >&2
  echo "  $0 run queries-hot tpch         # TPC-H individual hot query times" >&2
  echo "  $0 run profile-global yakbench  # Yakbench global profile queries" >&2
  echo "  $0 run profile-max-user yakbench    # Yakbench max-user queries" >&2
  echo "  $0 run profile-mean-user yakbench   # Yakbench mean-user queries" >&2
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

if [[ "$QUERY_TYPE" != "anomaly" && "$QUERY_TYPE" != "dashboard" && \
      "$QUERY_TYPE" != "queries-cold" && "$QUERY_TYPE" != "queries-hot" && \
      "$QUERY_TYPE" != "profile-global" && "$QUERY_TYPE" != "profile-max-user" && "$QUERY_TYPE" != "profile-mean-user" ]]; then
  echo "Error: query-type must be 'anomaly', 'dashboard', 'queries-cold', 'queries-hot'," >&2
  echo "       'profile-global', 'profile-max-user', or 'profile-mean-user'" >&2
  usage
fi

# queries-cold and queries-hot only work with tpch
if [[ "$QUERY_TYPE" == "queries-cold" || "$QUERY_TYPE" == "queries-hot" ]] && [[ "$BENCHMARK" != "tpch" ]]; then
  echo "Error: queries-cold and queries-hot only work with tpch benchmark" >&2
  usage
fi

# profile-* only work with yakbench
if [[ "$QUERY_TYPE" == "profile-global" || "$QUERY_TYPE" == "profile-max-user" || "$QUERY_TYPE" == "profile-mean-user" ]] && [[ "$BENCHMARK" != "yakbench" ]]; then
  echo "Error: profile-global, profile-max-user, profile-mean-user only work with yakbench benchmark" >&2
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
ANOMALY_BASELINE_N_TIMES_20=$((ANOMALY_BASELINE_N * 20))
ANOMALY_SIGMA=$(get_var "anomaly_sigma" "2")
ANOMALY_NEW_NORMAL_RELATIVE_THRESHOLD=$(get_var "anomaly_new_normal_relative_threshold" "0.05")
TPCH_ANOMALY_SCALE_FACTOR=$(get_var "tpch_anomaly_scale_factor" "1.0")

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
  elif [[ "$QUERY_TYPE" == "queries-cold" || "$QUERY_TYPE" == "queries-hot" ]]; then
    # TPC-H individual query breakdown - extract from local.tpch_query_kql in main.tf
    local QUERY_PREFIX
    if [[ "$QUERY_TYPE" == "queries-cold" ]]; then
      QUERY_PREFIX="cold"
    else
      QUERY_PREFIX="hot"
    fi
    # Extract the heredoc from tpch_query_kql (there's only one, used for both cold/hot via ${query_type})
    awk '
      /tpch_query_kql = \{/ { in_block = 1 }
      in_block && /<<-KQL$/ { in_query = 1; next }
      in_block && in_query && /^ *KQL$/ { in_query = 0; in_block = 0 }
      in_query { print }
    ' "$MAIN_TF" | sed "s/\${query_type}/${QUERY_PREFIX}/g"
  elif [[ "$QUERY_TYPE" == "profile-global" || "$QUERY_TYPE" == "profile-max-user" || "$QUERY_TYPE" == "profile-mean-user" ]]; then
    # Yakbench profile breakdown - extract from local.yakbench_query_kql in main.tf
    local PROFILE_TYPE
    PROFILE_TYPE="${QUERY_TYPE#profile-}"  # Strip "profile-" prefix
    # Extract the heredoc from yakbench_query_kql
    awk '
      /yakbench_query_kql = \{/ { in_block = 1 }
      in_block && /<<-KQL$/ { in_query = 1; next }
      in_block && in_query && /^ *KQL$/ { in_query = 0; in_block = 0 }
      in_query { print }
    ' "$MAIN_TF" | sed "s/\${profile_type}/${PROFILE_TYPE}/g"
  else
    # Dashboard query - extract from local.dashboard_queries in main.tf
    # There are two heredocs: first for string params, second for numeric params
    if [[ "$PARAM_IS_STRING" == "true" ]]; then
      # Extract the first heredoc (string param version)
      awk '
        /dashboard_queries = \{/ { in_block = 1 }
        in_block && /<<-KQL$/ && !found { in_query = 1; found = 1; next }
        in_block && in_query && /^ *KQL$/ { in_query = 0 }
        in_query { print }
      ' "$MAIN_TF"
    else
      # Extract the second heredoc (numeric param version)
      awk '
        /dashboard_queries = \{/ { in_block = 1 }
        in_block && /<<-KQL$/ { count++; if (count == 2) { in_query = 1 }; next }
        in_block && in_query && /^ *KQL$/ { in_query = 0; in_block = 0 }
        in_query { print }
      ' "$MAIN_TF"
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
    sed "s/\${var.anomaly_baseline_n \* 20}/${ANOMALY_BASELINE_N_TIMES_20}/g" | \
    sed "s/\${var.tpch_anomaly_scale_factor}/${TPCH_ANOMALY_SCALE_FACTOR}/g" | \
    sed "s/\${var.anomaly_sigma}/${ANOMALY_SIGMA}/g" | \
    sed "s/\${var.anomaly_new_normal_relative_threshold}/${ANOMALY_NEW_NORMAL_RELATIVE_THRESHOLD}/g" | \
    sed "s/\${var.${PARAM_VAR}}/${PARAM_VALUE}/g" | \
    sed "s/\${each.value.param_name}/${PARAM_NAME}/g" | \
    sed "s/\${each.value.param_path}/${PARAM_PATH}/g" | \
    sed "s/\${each.value.param_value}/${PARAM_VALUE}/g" | \
    sed "s/\${each.value.metric_path}/${METRIC_PATH}/g" | \
    sed "s/\${each.value.metric_name}/${METRIC_NAME}/g" | \
    sed "s/\${each.value.name}/${BENCH_NAME}/g" | \
    sed "s/\${bench.param_path}/${PARAM_PATH}/g" | \
    sed "s/\${bench.param_value}/${PARAM_VALUE}/g" | \
    sed "s/\${bench.metric_path}/${METRIC_PATH}/g" | \
    sed "s/\${bench.metric_name}/${METRIC_NAME}/g" | \
    sed "s/\${bench.name}/${BENCH_NAME}/g" | \
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
