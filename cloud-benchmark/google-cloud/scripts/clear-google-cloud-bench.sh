#!/usr/bin/env bash

set -e
(
  kubectl delete jobs xtdb-single-node-auctionmark || true
  kubectl delete jobs xtdb-multi-node-auctionmark || true
  gcloud storage rm gs://xtdb-am-bench-object-store/** || true
  kubectl delete pvc xtdb-pvc-log || true
  kubectl delete pvc xtdb-pvc-local-caches || true
)

