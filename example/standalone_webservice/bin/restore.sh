#!/usr/bin/env bash

set -e

TEMP_DIR=$(mktemp -d)

aws s3 cp s3://crux-bench-results/backup.tar.gz "${TEMP_DIR}/backup.tar.gz" \
    || true # do not fail if there is no backup

if [ -f "${TEMP_DIR}/backup.tar.gz" ]; then
    tar -zvxf "${TEMP_DIR}/backup.tar.gz" --directory "${CRUX_CHECKPOINT_DIRECTORY}"
fi
