#!/usr/bin/env bash

set -e
(
    cd $(dirname $0)/..

    ../gradlew :monitoring:docker-image:shadowJar

    sha=$(git rev-parse --short HEAD)

    echo Building Docker image ...
    docker build -t xtdb-monitoring --build-arg GIT_SHA="$sha" --build-arg XTDB_VERSION="${XTDB_VERSION:-2-SNAPSHOT}" --output type=docker docker-image
    echo Done
)
