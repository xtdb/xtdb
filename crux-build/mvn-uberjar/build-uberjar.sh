#!/usr/bin/env bash
set -x

mvn package \
    -Dcrux.crux-version=${CRUX_VERSION:-"derived-from-git"} \
    -Dcrux.artifact-version=${CRUX_ARTIFACT_VERSION:-"derived-from-git"} \
    -Dcrux.uberjar-name=${UBERJAR_NAME:-crux.jar} $@
