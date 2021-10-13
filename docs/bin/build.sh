#!/usr/bin/env bash
set -e

PLAYBOOK="antora-playbook.yml"

(
    cd $(dirname "$0")/..
    ANTORA="$(npm bin)/antora"
    [[ -e "$ANTORA" ]] || npm install
    CI=true $ANTORA --clean --redirect-facility static --fetch $PLAYBOOK
    echo "Built: file://$(pwd)/build/site/index.html"
)
