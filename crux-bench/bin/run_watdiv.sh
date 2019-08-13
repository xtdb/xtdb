#!/usr/bin/env bash

set -e

(
    CRUX_WATDIV_NUM_QUERIES=2 \
    CRUX_WATDIV=true \
    CRUX_WATDIV_RUN_CRUX=true \
    lein test :only crux.watdiv-test
)

for f in target/watdiv_*; do
    aws s3 cp ${f} s3://crux-bench-results/ --acl public-read
done
