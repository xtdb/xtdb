#!/usr/bin/env bash

set -e
(
    # Ensure we're in the same directory as this script
    cd $(dirname $0)

    if [ "$1" == "--clean" ] || ! [ -e build/libs/http-proxy.jar ]; then
        echo Building jar ...
        ../../gradlew :lang:http-proxy:shadowJar
        echo Done
    fi

    echo Building Docker image ...
    docker build -t http-proxy:latest --output type=docker .
    echo Done
)
