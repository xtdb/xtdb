#!/usr/bin/env bash

# Syncs the mirrored food-prices Arrow (produced by mirror.py) up to
# s3://xtdb-datasets/food-prices/. Run occasionally, when refreshing the dataset.
# The benchmark pulls it back down itself — see xtdb.bench.delta-ingest/download-dataset.

set -xe

cd "$(dirname "$0")"

SRC="${1:-out}"

aws s3 sync "$SRC" s3://xtdb-datasets/food-prices/
