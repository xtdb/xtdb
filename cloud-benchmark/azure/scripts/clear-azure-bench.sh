#!/usr/bin/env bash

set -e

CLEAR_GRAFANA=false

# Parse arguments
for arg in "$@"; do
  if [ "$arg" == "--clear-grafana" ]; then
    CLEAR_GRAFANA=true
  fi
done

(
  echo Deleting XTDB Jobs...
  kubectl delete job xtdb-load-phase --namespace cloud-benchmark || true
  kubectl delete job xtdb-cluster-nodes --namespace cloud-benchmark || true
  echo Done

  echo Clearing Kafka...
  helm uninstall kafka --namespace cloud-benchmark || true
  kubectl delete pvc data-kafka-controller-0 --namespace cloud-benchmark || true
  kubectl delete pvc data-kafka-controller-1 --namespace cloud-benchmark || true
  kubectl delete pvc data-kafka-controller-2 --namespace cloud-benchmark || true
  echo Done

  echo Clearing Blob Store Container - xtdbazurebenchmarkcontainer ...
  az storage blob delete-batch --account-name xtdbazurebenchmark --source xtdbazurebenchmarkcontainer
  echo Done

  if [ "$CLEAR_GRAFANA" == "true" ]; then
    echo Clearing Grafana...
    kubectl delete deployment grafana-deployment --namespace cloud-benchmark || true
    kubectl delete pvc grafana-pvc --namespace cloud-benchmark || true
    kubectl delete pvc prometheus-pvc --namespace cloud-benchmark || true
    echo Done
  else
    echo "Skipping Grafana clearing (use --clear-grafana to include it)."
  fi
)
