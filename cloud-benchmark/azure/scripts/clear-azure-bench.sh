#!/usr/bin/env bash

set -e
(
  kubectl delete jobs xtdb-single-node-auctionmark --namespace cloud-benchmark || true
  kubectl delete jobs xtdb-multi-node-auctionmark --namespace cloud-benchmark || true
  kubectl delete deployment kafka-app --namespace cloud-benchmark || true 

  echo Clearing Blob Store Container - xtdbazurebenchmarkcontainer ...
  az storage blob delete-batch --account-name xtdbazurebenchmark --source xtdbazurebenchmarkcontainer
  echo Done
  
  kubectl delete pvc xtdb-pvc-log --namespace cloud-benchmark || true
  kubectl delete pvc xtdb-pvc-local-cache-lp --namespace cloud-benchmark || true
  kubectl delete pvc xtdb-pvc-local-cache-1 --namespace cloud-benchmark || true
  kubectl delete pvc xtdb-pvc-local-cache-2 --namespace cloud-benchmark || true
  kubectl delete pvc xtdb-pvc-local-cache-3 --namespace cloud-benchmark || true
  kubectl delete pvc kafka-pvc --namespace cloud-benchmark || true
)

