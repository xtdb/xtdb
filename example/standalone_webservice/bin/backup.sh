#!/usr/bin/env bash

set -e

TEMP_DIR=$(mktemp -d)

tar -zcvf "${TEMP_DIR}/backup.tar.gz" -C "${CRUX_CHECKPOINT_DIRECTORY}" .
aws s3 cp "${TEMP_DIR}/backup.tar.gz" s3://crux-bench-results/backup.tar.gz
