#!/usr/bin/env bash

set -e
(
    cd $(dirname $0)/..

    if [ "$1" == "--clean" ] || ! [ -e target/core2-standalone.jar ]; then
        ./bin/re-prep.sh
        clojure -Xuberjar
    fi

    echo Building Docker image ...
    docker build -t core2:latest .
    echo Done
)
