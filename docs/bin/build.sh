#!/usr/bin/env bash
set -e

PLAYBOOK="antora-playbook.yml"

(
    cd $(dirname "$0")/..
    CI=true npx antora --clean --redirect-facility static --fetch $PLAYBOOK
    echo "Built: file://$(pwd)/build/site/index.html"
)
