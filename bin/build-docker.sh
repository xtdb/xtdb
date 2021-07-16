#!/usr/bin/env bash

set -e
(
    cd $(dirname $0)/..

    if [ "$1" == "--clean" ] || ! [ -e target/core2-standalone.jar ]; then
        echo Building uberjar...
        ./lein-sub do clean, install
        lein uberjar
    fi

    echo Building Docker image...
    docker build -t juxt.crux-labs/core2:${CORE2_VERSION:-latest} .
    echo Done
)
