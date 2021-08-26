#!/usr/bin/env bash
set -x

mvn package \
    -Dcrux.crux-version=${XTDB_VERSION:-"crux-git-version"} \
    -Dcrux.artifact-version=${XTDB_ARTIFACT_VERSION:-"crux-git-version"} \
    -Dcrux.uberjar-name=${UBERJAR_NAME:-crux.jar} $@
