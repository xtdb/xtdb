#!/usr/bin/env bash

set -e
(
  gcloud deployment-manager deployments create google-cloud-auctionmark-benchmark --config deployment-manager/xtdb-google-cloud-bench.yaml
)
