#!/usr/bin/env bash

set -e
(
    echo Building Google Cloud ShadowJar ...

    ../../gradlew :cloud-benchmark:google-cloud:shadowJar

    sha=$(git rev-parse --short HEAD)

    echo Building Docker image ...
    docker build -t xtdb-google-cloud-bench:latest --build-arg GIT_SHA="$sha" --build-arg XTDB_VERSION="${XTDB_VERSION:-dev-SNAPSHOT}" --output type=docker .
    echo Done
)
