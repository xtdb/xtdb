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
  minikube kubectl -- delete jobs xtdb-multi-node-auctionmark --namespace xtdb-benchmark || true
  minikube kubectl -- delete deployment kafka-app --namespace xtdb-benchmark || true 
  minikube kubectl -- delete pvc xtdb-pvc-local-storage --namespace xtdb-benchmark || true
  minikube kubectl -- delete pvc kafka-pvc --namespace xtdb-benchmark || true

  if [ "$CLEAR_GRAFANA" == "true" ]; then
    echo Clearing Grafana...
    minikube kubectl -- delete deployment grafana-deployment --namespace xtdb-benchmark || true 
    minikube kubectl -- delete pvc grafana-pvc --namespace xtdb-benchmark || true 
    minikube kubectl -- delete pvc prometheus-pvc --namespace xtdb-benchmark || true  
    echo Done
  else
    echo "Skipping Grafana clearing (use --clear-grafana to include it)."
  fi
)
