#!/usr/bin/env bash
set -x

mvn package \
    -Dxtdb.crux-version=${XTDB_VERSION:-"crux-git-version"} \
    -Dxtdb.artifact-version=${XTDB_ARTIFACT_VERSION:-"crux-git-version"} \
    -Dxtdb.uberjar-name=${UBERJAR_NAME:-xtdb.jar} $@
