#!/usr/bin/env bash
set -euo pipefail

# Installs the monitoring stack (Prometheus Operator + Grafana) using kube-prometheus-stack
# and applies the XTDB dashboards.
#
# Requirements:
# - kubectl
# - helm
# - access to the target Kubernetes cluster context
#
# This script installs into the "monitoring" namespace with Helm release name "monitoring"
# to match selectors used elsewhere in the repo (e.g. ServiceMonitor labels).

RELEASE_NAME=${RELEASE_NAME:-monitoring}
NAMESPACE=${NAMESPACE:-monitoring}
CHART=${CHART:-prometheus-community/kube-prometheus-stack}

# Helpers
error() { echo "Error: $*" >&2; exit 1; }
log() { echo "[install-monitoring] $*"; }

# Resolve paths relative to this script directory
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
VALUES_FILE="${VALUES_FILE:-${SCRIPT_DIR}/values.yaml}"
DASHBOARDS_FILE="${DASHBOARDS_FILE:-${SCRIPT_DIR}/monitoring-dashboards.yaml}"

# Determine project root (prefer git, fall back to 4 dirs up from this script)
PROJECT_ROOT=${PROJECT_ROOT:-}
if [ -z "${PROJECT_ROOT}" ] && command -v git >/dev/null 2>&1; then
  PROJECT_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel 2>/dev/null || true)
fi
if [ -z "${PROJECT_ROOT}" ]; then
  PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../../../" && pwd)"
fi

# Dashboard JSON source directory in the repo
DASH_JSON_DIR="${DASH_JSON_DIR:-${PROJECT_ROOT}/dev/monitoring/dashboards}"

# Functions
check_prerequisites() {
  command -v kubectl >/dev/null 2>&1 || error "kubectl not found in PATH"
  command -v helm >/dev/null 2>&1 || error "helm not found in PATH"
}

ensure_namespace() {
  if ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    log "Creating namespace ${NAMESPACE}"
    kubectl create namespace "${NAMESPACE}"
  fi
}

add_helm_repo() {
  if ! helm repo list | awk '{print $1}' | grep -q '^prometheus-community$'; then
    log "Adding Helm repo prometheus-community"
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
  fi
  helm repo update
}

install_or_upgrade_stack() {
  log "Installing/Upgrading ${CHART} as release ${RELEASE_NAME} in namespace ${NAMESPACE}"
  local set_args=()
  if [ -n "${GRAFANA_ADMIN_PASSWORD:-}" ]; then
    set_args+=(--set "grafana.adminPassword=${GRAFANA_ADMIN_PASSWORD}")
  fi

  if [ -f "${VALUES_FILE}" ]; then
    helm upgrade --install "${RELEASE_NAME}" "${CHART}" \
      --namespace "${NAMESPACE}" \
      --create-namespace \
      -f "${VALUES_FILE}" \
      "${set_args[@]}"
  else
    echo "WARNING: values file not found at ${VALUES_FILE}. Proceeding with chart defaults." >&2
    helm upgrade --install "${RELEASE_NAME}" "${CHART}" \
      --namespace "${NAMESPACE}" \
      --create-namespace \
      "${set_args[@]}"
  fi
}

wait_for_components() {
  log "Waiting for core monitoring pods to become ready..."
  kubectl -n "${NAMESPACE}" rollout status deploy/"${RELEASE_NAME}"-kube-prometheus-operator --timeout=5m || true
  kubectl -n "${NAMESPACE}" rollout status statefulset/"${RELEASE_NAME}"-kube-prometheus-prometheus --timeout=10m || true
  kubectl -n "${NAMESPACE}" rollout status deploy/"${RELEASE_NAME}"-grafana --timeout=10m || true
}

generate_dashboards_if_present() {
  if [ -f "${DASH_JSON_DIR}/xtdb-monitoring.json" ] && [ -f "${DASH_JSON_DIR}/xtdb-node-debugging.json" ]; then
    log "Generating dashboards YAML from JSON sources at ${DASH_JSON_DIR}..."
    INPUT_DIR="${DASH_JSON_DIR}" bash "${SCRIPT_DIR}/gen-dashboards.sh" "${DASHBOARDS_FILE}"
  fi
}

apply_dashboards() {
  if [ -f "${DASHBOARDS_FILE}" ]; then
    log "Applying dashboards from ${DASHBOARDS_FILE}"
    kubectl apply -n "${NAMESPACE}" -f "${DASHBOARDS_FILE}"
  else
    echo "WARNING: dashboards file not found at ${DASHBOARDS_FILE}. Skipping dashboards apply." >&2
  fi
}

print_summary() {
  cat <<EOF

Monitoring stack installation complete.

Namespace:   ${NAMESPACE}
Release:     ${RELEASE_NAME}
Chart:       ${CHART}
Values:      ${VALUES_FILE}
Dashboards:  ${DASHBOARDS_FILE}

Next steps:
- Port-forward Grafana: kubectl -n ${NAMESPACE} port-forward svc/${RELEASE_NAME}-grafana 3000:80
  Login: admin / (password from values or secret)
- Check ServiceMonitors: kubectl -n ${NAMESPACE} get servicemonitors
- Check Prometheus targets for xtdb-benchmark metrics once the benchmark is running.
EOF
}

main() {
  check_prerequisites
  ensure_namespace
  add_helm_repo
  install_or_upgrade_stack
  wait_for_components
  generate_dashboards_if_present
  apply_dashboards
  print_summary
}

main "$@"
