#!/usr/bin/env bash
set -x

mvn package \
    -Dxtdb.xtdb-version=${XTDB_VERSION:-"xtdb-git-version"} \
    -Dxtdb.artifact-version=${XTDB_ARTIFACT_VERSION:-"xtdb-git-version"} \
    -Dxtdb.uberjar-name=${UBERJAR_NAME:-xtdb.jar} $@
