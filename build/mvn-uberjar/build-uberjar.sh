#!/usr/bin/env bash
set -x

mvn package \
    -Dcrux.crux-version=${CRUX_VERSION:-"crux-git-version"} \
    -Dcrux.artifact-version=${CRUX_ARTIFACT_VERSION:-"crux-git-version"} \
    -Dcrux.uberjar-name=${UBERJAR_NAME:-crux.jar} $@
