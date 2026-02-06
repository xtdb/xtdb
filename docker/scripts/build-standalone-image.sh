#!/usr/bin/env bash

set -e
(
    cd $(dirname $0)/../..

    if [ "$1" == "--clean" ] || ! [ -e docker/standalone/build/libs/xtdb-standalone.jar ]; then
        ./gradlew :docker:standalone:shadowJar
    fi

    sha=$(git rev-parse --short HEAD)

    echo Building Docker image ...
    docker build -t ghcr.io/xtdb/xtdb:dev --build-arg VARIANT=standalone --build-arg GIT_SHA="$sha" --build-arg XTDB_VERSION="${XTDB_VERSION:-2-SNAPSHOT}" --output type=docker -f docker/Dockerfile .
    echo Done
)
