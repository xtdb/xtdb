#!/usr/bin/env bash
set -e

PLAYBOOK="antora-playbook.yml"

(
    cd $(dirname "$0")/..
    ANTORA="$(npm bin)/antora"
    OPTIONS=$@
    [[ -e "$ANTORA" ]] || npm install
    CI=true $ANTORA --clean --redirect-facility static --fetch $PLAYBOOK $OPTIONS
    echo "Built: file://$(pwd)/build/site/index.html"
)

