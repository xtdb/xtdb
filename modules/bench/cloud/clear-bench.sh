#!/usr/bin/env bash
set -euo pipefail

CLOUD="$1"
RELEASE_NAME="xtdb-benchmark"
NAMESPACE="cloud-benchmark"

error() { echo "Error: $*" >&2; exit 1; }

usage() {
  echo "Usage: $0 <aws|azure|google-cloud>"
  exit 1
}

clear_helm_release() {
  echo "Clearing previous Helm release..."
  helm uninstall "$RELEASE_NAME" --namespace "$NAMESPACE" || true
  echo "Previous Helm release cleared."
}

clear_kafka_pvcs() {
  echo "Clearing Kafka PVCs..."
  for i in 0 1 2; do
    kubectl delete pvc "data-$RELEASE_NAME-kafka-controller-$i" --namespace "$NAMESPACE" || true
  done
  echo "Kafka PVCs cleared."
}

clear_cloud_storage() {
  echo "Clearing storage for '$CLOUD'..."
  case "$CLOUD" in
    azure)
      echo "Clearing Azure Blob container: xtdbazurebenchmarkcontainer"
      if ! az storage blob delete-batch \
        --auth-mode login \
        --account-name xtdbazurebenchmark \
        --source xtdbazurebenchmarkcontainer; then
        error "Failed to delete Azure blob container contents.

You may need to assign the Storage Blob Data Contributor role. Run:
  az role assignment create \\
    --assignee \"\$(az account show --query user.name -o tsv)\" \\
    --role \"Storage Blob Data Contributor\" \\
    --scope \"\$(az storage account show \\
                  --name xtdbazurebenchmark \\
                  --resource-group cloud-benchmark-resources \\
                  --query id --output tsv)\""
      fi
      ;;
    aws)
      echo "Emptying S3 bucket: xtdb-bench-bucket"
      aws s3 --profile xtdb-bench rm s3://xtdb-bench-bucket --recursive
      ;;
    google-cloud)
      echo "Clearing Google Cloud Storage bucket: xtdb-gcp-benchmark-bucket"
      gcloud storage rm gs://xtdb-gcp-benchmark-bucket/** || true
      ;;
    *)
      error "Unknown cloud provider: $CLOUD"
      ;;
  esac
  echo "Storage cleared for '$CLOUD'"
}

main() {
  [[ -z "$CLOUD" ]] && usage
  clear_helm_release
  clear_kafka_pvcs
  clear_cloud_storage
}

main "$@"
