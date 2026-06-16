#!/usr/bin/env bash

# Syncs the mirrored GLEIF transit (produced by xtdb.datasets.gleif.mirror/mirror!
# at the REPL) up to s3://xtdb-datasets/gleif/. Run occasionally, when refreshing
# the dataset. Consumers pull it back down via download-dataset.sh --gleif.

set -xe

cd "$(dirname "$0")/.."

SRC="${1:-src/test/resources/data/gleif}"

aws s3 sync "$SRC" s3://xtdb-datasets/gleif/
