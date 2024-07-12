#!/usr/bin/env bash

set -e
(
    ../../gradlew :cloud-benchmark:local:shadowJar

    sha=$(git rev-parse --short HEAD)

    echo Building Docker image ...
    docker build -t xtdb-local-auctionmark:latest --build-arg GIT_SHA="$sha" --build-arg XTDB_VERSION="${XTDB_VERSION:-dev-SNAPSHOT}" .
    echo Done
)
