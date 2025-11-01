#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE_NAME="xtdb-benchmark"
NAMESPACE="cloud-benchmark"

CLOUD=""
BENCH_TYPE=""
USER_ARGS=()
CLOUD_ARGS=()
NO_CLEANUP=false

error() { echo "Error: $*" >&2; exit 1; }

usage() {
  echo "Usage: $0 <aws|azure|google-cloud> <tpch|readings|auctionmark> [helm args...] [--no-cleanup]"
  echo
  echo "Examples:"
  echo "  $0 azure tpch --set tpch.scaleFactor=0.01"
  echo "  $0 aws readings --set readings.devices=1000 --set readings.readings=5000"
  echo "  $0 google-cloud auctionmark --set auctionmark.scaleFactor=0.1 --set auctionmark.onlyLoad=true"
  echo "  $0 azure auctionmark --set auctionmark.threads=8 --set auctionmark.duration=PT10M --no-cleanup"
  exit 1
}

validate_inputs() {
  [[ "$CLOUD" =~ ^(aws|azure|google-cloud)$ ]] || error "Invalid cloud: $CLOUD"
  [[ "$BENCH_TYPE" =~ ^(tpch|readings|auctionmark|yakbench)$ ]] || error "Invalid bench type: $BENCH_TYPE"
}

get_cloud_config() {
  local config
  case "$CLOUD" in
    azure)
      config=$(terraform -chdir="cloud/$CLOUD" output -raw user_assigned_identity_client_id) || error "Azure config failed"
      [[ -n "$config" ]] || error "Azure client ID not found"
      CLOUD_ARGS+=(
        "--set" "providerConfig.env.AZURE_USER_MANAGED_IDENTITY_CLIENT_ID=$config"
        "--set" "providerConfig.serviceAccountAnnotations.azure\.workload\.identity/client-id=$config"
      )
      ;;
    aws)
      config=$(terraform -chdir="cloud/$CLOUD" output -raw service_account_role_arn) || error "AWS config failed"
      [[ -n "$config" ]] || error "AWS role ARN not found"
      CLOUD_ARGS+=(
        "--set" "providerConfig.serviceAccountAnnotations.eks\.amazonaws\.com/role-arn=$config"
      )
      ;;
    google-cloud)
      config=$(terraform -chdir="cloud/$CLOUD" output -raw service_account_email) || error "GCP config failed"
      [[ -n "$config" ]] || error "GCP service account not found"
      CLOUD_ARGS+=(
        "--set" "providerConfig.serviceAccountAnnotations.iam\.gke\.io/gcp-service-account=$config"
      )
      ;;
  esac
}

parse_args() {
  USER_ARGS=()
  NO_CLEANUP=false

  while [[ $# -gt 0 ]]; do
    case $1 in
      --no-cleanup) NO_CLEANUP=true; shift ;;
      *) USER_ARGS+=("$1"); shift ;;
    esac
  done
}

cleanup_previous_benchmark() {
  echo "Cleaning up previous benchmark job..."
  kubectl delete job "$BENCH_TYPE" --ignore-not-found --namespace "$NAMESPACE" || true

  if [[ "$NO_CLEANUP" != true ]]; then
    echo "Running full benchmark cleanup..."
    "${SCRIPT_DIR}/clear-bench.sh" "$CLOUD" || true
  fi
}

deploy_benchmark() {
  echo "Deploying benchmark with Helm..."
  helm dependency update "${SCRIPT_DIR}/helm"
  helm upgrade --install "$RELEASE_NAME" "${SCRIPT_DIR}/helm" \
    --namespace "$NAMESPACE" \
    --create-namespace \
    -f "${SCRIPT_DIR}/${CLOUD}/values.yaml" \
    --set "benchType=$BENCH_TYPE" \
    "${CLOUD_ARGS[@]}" \
    "${USER_ARGS[@]}"
}

main() {
  [[ $# -lt 2 ]] && usage
  CLOUD=$1; BENCH_TYPE=$2; shift 2

  validate_inputs
  get_cloud_config
  parse_args "$@"
  cleanup_previous_benchmark
  deploy_benchmark
}

main "$@"
