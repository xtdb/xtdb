#!/usr/bin/env bash

set -e
(
    cd $(dirname $0)/..

    if [ "$1" == "--clean" ] || ! [ -e build/libs/core2-standalone.jar ]; then
        ../gradlew :docker:shadowJar
    fi

    sha=$(git rev-parse --short HEAD)

    echo Building Docker image ...
    docker build -t ghcr.io/xtdb/core2:latest --build-arg GIT_SHA="$sha" --build-arg CORE2_VERSION="${CORE2_VERSION:-dev-SNAPSHOT}" .
    echo Done
)
