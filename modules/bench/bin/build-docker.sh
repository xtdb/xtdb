#!/usr/bin/env bash

set -xe

(
    cd $(dirname $0)/../../..
    ./lein-sub install
)

(
    cd $(dirname $0)/..
    lein uberjar
    docker build -t juxt.core2/bench:latest -f Dockerfile .
)
