#!/usr/bin/env bash

set -xe

(
    cd $(dirname $0)/..
    ../../gradlew shadowJar
    docker build -t juxt.xtdb2/bench:latest -f Dockerfile .
)
