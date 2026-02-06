#!/usr/bin/env bash

set -e
(
    cd $(dirname $0)/../..

    if [ "$1" == "--clean" ] || ! [ -e docker/aws/build/libs/xtdb-aws.jar ]; then
        ./gradlew :docker:aws:shadowJar
    fi

    sha=$(git rev-parse --short HEAD)

    echo Building Docker image ...
    docker build -t xtdb/xtdb-aws --build-arg VARIANT=aws --build-arg GIT_SHA="$sha" --build-arg XTDB_VERSION="${XTDB_VERSION:-2-SNAPSHOT}" --output type=docker -f docker/Dockerfile .
    echo Done
)
