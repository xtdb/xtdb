#!/usr/bin/env bash

set -e
(
    ../../gradlew :cloud-benchmark:azure:shadowJar

    sha=$(git rev-parse --short HEAD)

    echo Building Docker image ...
    docker buildx build --platform linux/arm64/v8,linux/amd64 --push -t cloudbenchmarkregistry.azurecr.io/xtdb-azure-bench:latest --platform=linux/amd64 --build-arg GIT_SHA="$sha" --build-arg XTDB_VERSION="${XTDB_VERSION:-dev-SNAPSHOT}" .
    echo Done
)

