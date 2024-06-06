#!/usr/bin/env bash

set -e
(
    ../../gradlew :cloud-benchmark:aws:shadowJar

    sha=$(git rev-parse --short HEAD)

    echo Building Docker image ...
    docker build -t xtdb-aws-bench:latest --build-arg GIT_SHA="$sha" --build-arg XTDB_VERSION="${XTDB_VERSION:-dev-SNAPSHOT}" --output type=docker .
    echo Done
)
