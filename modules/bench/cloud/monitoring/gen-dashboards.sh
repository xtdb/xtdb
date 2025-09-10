#!/usr/bin/env bash
set -euo pipefail

# Generate monitoring-dashboards.yaml ConfigMap from JSON dashboard files.
# - Replaces DS_PROMETHEUS datasource placeholder with Prometheus
# - Embeds the JSON files as literal block scalars in the ConfigMap
#
# Usage:
#   ./gen-dashboards.sh [OUTPUT_PATH]
# Defaults:
#   INPUT_DIR = script directory
#   OUTPUT_PATH = INPUT_DIR/monitoring-dashboards.yaml

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
INPUT_DIR="${INPUT_DIR:-${SCRIPT_DIR}}"
OUT_PATH="${1:-${SCRIPT_DIR}/monitoring-dashboards.yaml}"

MON_JSON="${INPUT_DIR}/xtdb-monitoring.json"
NODE_JSON="${INPUT_DIR}/xtdb-node-debugging.json"

if [[ ! -f "${MON_JSON}" || ! -f "${NODE_JSON}" ]]; then
  echo "Error: Missing JSON dashboards. Expected files:" >&2
  echo "  - ${MON_JSON}" >&2
  echo "  - ${NODE_JSON}" >&2
  exit 1
fi

# Create a temp dir to stage processed JSON with datasource replacements
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

process_json() {
  local in_file="$1"; local out_file="$2"
  # Replace DS_PROMETHEUS -> Prometheus
  # Note: Use sed without in-place to support both GNU/BSD sed via explicit redirection
  sed 's/DS_PROMETHEUS/Prometheus/g' "${in_file}" > "${out_file}"
}

process_json "${MON_JSON}"  "${TMP_DIR}/xtdb-monitoring.json"
process_json "${NODE_JSON}" "${TMP_DIR}/xtdb-node-debugging.json"

indent_block() {
  # Indent every line by 4 spaces for YAML literal block
  sed 's/^/    /'
}

{
  echo "apiVersion: v1"
  echo "kind: ConfigMap"
  echo "metadata:"
  echo "  name: xtdb-bench-dashboards"
  echo "  namespace: monitoring"
  echo "  labels:"
  echo "    grafana_dashboard: \"1\""
  echo "data:"
  echo "  xtdb-monitoring.json: |"
  cat "${TMP_DIR}/xtdb-monitoring.json" | indent_block
  echo "  xtdb-node-debugging.json: |"
  cat "${TMP_DIR}/xtdb-node-debugging.json" | indent_block
} > "${OUT_PATH}"

echo "Generated ${OUT_PATH}"
