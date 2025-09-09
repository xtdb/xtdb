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

# Resolve paths relative to this script directory
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
VALUES_FILE="${VALUES_FILE:-${SCRIPT_DIR}/values.yaml}"
DASHBOARDS_FILE="${DASHBOARDS_FILE:-${SCRIPT_DIR}/monitoring-dashboards.yaml}"

# Check prerequisites
command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found in PATH" >&2; exit 1; }
command -v helm >/dev/null 2>&1 || { echo "helm not found in PATH" >&2; exit 1; }

# Create namespace if it doesn't exist
if ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
  echo "Creating namespace ${NAMESPACE}"
  kubectl create namespace "${NAMESPACE}"
fi

# Add Helm repo if missing, then update
if ! helm repo list | awk '{print $1}' | grep -q '^prometheus-community$'; then
  echo "Adding Helm repo prometheus-community"
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
fi
helm repo update

# Install/Upgrade kube-prometheus-stack
echo "Installing/Upgrading ${CHART} as release ${RELEASE_NAME} in namespace ${NAMESPACE}"
if [ -f "${VALUES_FILE}" ]; then
  helm upgrade --install "${RELEASE_NAME}" "${CHART}" \
    --namespace "${NAMESPACE}" \
    --create-namespace \
    -f "${VALUES_FILE}"
else
  echo "WARNING: values file not found at ${VALUES_FILE}. Proceeding with chart defaults." >&2
  helm upgrade --install "${RELEASE_NAME}" "${CHART}" \
    --namespace "${NAMESPACE}" \
    --create-namespace
fi

# Wait for core components to be ready (Prometheus Operator, Prometheus, Grafana)
echo "Waiting for core monitoring pods to become ready..."
# Wait for operator
kubectl -n "${NAMESPACE}" rollout status deploy/"${RELEASE_NAME}"-kube-prometheus-operator --timeout=5m || true
# Wait for prometheus statefulset
kubectl -n "${NAMESPACE}" rollout status statefulset/"${RELEASE_NAME}"-kube-prometheus-prometheus --timeout=10m || true
# Wait for grafana
kubectl -n "${NAMESPACE}" rollout status deploy/"${RELEASE_NAME}"-grafana --timeout=10m || true

# Apply dashboards (Grafana sidecar will pick these up)
if [ -f "${DASHBOARDS_FILE}" ]; then
  echo "Applying dashboards from ${DASHBOARDS_FILE}"
  kubectl apply -n "${NAMESPACE}" -f "${DASHBOARDS_FILE}"
else
  echo "WARNING: dashboards file not found at ${DASHBOARDS_FILE}. Skipping dashboards apply." >&2
fi

cat <<EOF

Monitoring stack installation complete.

Namespace:   ${NAMESPACE}
Release:     ${RELEASE_NAME}
Chart:       ${CHART}
Values:      ${VALUES_FILE}
Dashboards:  ${DASHBOARDS_FILE}

Next steps:
- Port-forward Grafana: kubectl -n ${NAMESPACE} port-forward svc/${RELEASE_NAME}-grafana 3000:80
  Login: admin / admin (default, see values.yaml)
- Check ServiceMonitors: kubectl -n ${NAMESPACE} get servicemonitors
- Check Prometheus targets for xtdb-benchmark metrics once the benchmark is running.
EOF
