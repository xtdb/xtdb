#!/usr/bin/env bash

set -e
(
    cd $(dirname $0)/..

    if [ "$1" == "--clean" ] || ! [ -e build/libs/xtdb-standalone.jar ]; then
        ../gradlew :docker:shadowJar
    fi

    sha=$(git rev-parse --short HEAD)

    echo Building Docker image ...
    docker build -t ghcr.io/xtdb/xtdb2:latest --build-arg GIT_SHA="$sha" --build-arg XTDB_VERSION="${XTDB_VERSION:-dev-SNAPSHOT}" .
    echo Done
)
