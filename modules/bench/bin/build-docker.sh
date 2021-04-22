#!/usr/bin/env bash

set -xe

(
    cd $(dirname $0)/../../../
    lein with-profile +bench uberjar
    docker build -t juxt.core2/bench:latest -f modules/bench/Dockerfile .
)
