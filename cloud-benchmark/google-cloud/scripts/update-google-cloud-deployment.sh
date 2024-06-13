#!/usr/bin/env bash

set -e
(
  gcloud deployment-manager deployments update google-cloud-auctionmark-benchmark --config deployment-manager/xtdb-google-cloud-bench.yaml
)
