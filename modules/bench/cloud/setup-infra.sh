#!/usr/bin/env bash
set -euo pipefail

CLOUD="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

error() { echo "Error: $*" >&2; exit 1; }

usage() {
  echo "Usage: $0 <aws|azure|google-cloud>"
  exit 1
}

setup_infra() {
  echo "Setting up infrastructure for $CLOUD..."
  (
    cd "$SCRIPT_DIR/$CLOUD"
    terraform init
    terraform apply
  )
  echo "Infrastructure setup complete for $CLOUD"
}

configure_kubectl() {
  echo "Connecting kubectl to the xtdb-bench-cluster Kubernetes cluster..."
  case "$CLOUD" in
    aws)
      aws eks --profile xtdb-bench --region us-east-1 update-kubeconfig --name xtdb-bench-cluster
      ;;
    azure)
      az aks get-credentials --resource-group cloud-benchmark-resources --name xtdb-bench-cluster
      # Ensure contributor role for storage
      az role assignment create \
        --assignee "$(az account show --query user.name -o tsv)" \
        --role "Storage Blob Data Contributor" \
        --scope "$(az storage account show \
                    --name xtdbazurebenchmark \
                    --resource-group cloud-benchmark-resources \
                    --query id --output tsv)"
      ;;
    google-cloud)
      gcloud container clusters get-credentials xtdb-bench-cluster --region us-central1
      ;;
    *)
      error "Unknown cloud provider: $CLOUD"
      ;;
  esac
  echo "Kubernetes context set to xtdb-bench-cluster cluster in $CLOUD"
}

main() {
  [[ -z "$CLOUD" ]] && usage
  setup_infra
  configure_kubectl
}

main "$@"
