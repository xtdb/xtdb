#!/usr/bin/env bash

# Syncs the mirrored EDGAR transit (produced by xtdb.datasets.edgar.mirror/mirror!
# at the REPL — one <quarter>.transit.json.gz per quarter) up to
# s3://xtdb-datasets/edgar/. Run occasionally, when refreshing the dataset.
# Consumers pull it back down via download-dataset.sh --edgar.

set -xe

cd "$(dirname "$0")/.."

SRC="${1:-src/dev/resources/data/edgar}"

aws s3 sync --exclude 'tsv/*' "$SRC" s3://xtdb-datasets/edgar/
