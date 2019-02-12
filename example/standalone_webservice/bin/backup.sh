#!/usr/bin/env bash

set -e

echo "GIVEN DIR: " ${CHECKPOINT_DIR}
echo "AWS_PROFILE: " ${AWS_PROFILE}

TEMP_DIR=$(mktemp -d)

tar -zcvf "${TEMP_DIR}/backup.tar.gz" "${CHECKPOINT_DIR}"
aws s3 cp "${TEMP_DIR}/backup.tar.gz" s3://crux-bench-results/backup.tar.gz
