#!/usr/bin/env bash

set -e

cd ..

(
    CRUX_WATDIV_NUM_QUERIES=1 \
    CRUX_WATDIV=true \
    CRUX_WATDIV_RUN_CRUX=true \
    lein test :only crux.watdiv-test
)
