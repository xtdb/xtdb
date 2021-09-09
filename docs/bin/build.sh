#!/usr/bin/env bash
set -e

OPTS=
PLAYBOOK="antora-playbook.yml"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --with-local-bundle)
            OPTS+=" --ui-bundle-url=../../website-old/build/ui-bundle.zip"
            shift;;
        *) echo "Unknown parameter passed: $1"; exit 1;;
    esac
done

(
    cd $(dirname "$0")/..
    ANTORA="$(npm bin)/antora"
    [[ -e "$ANTORA" ]] || npm install
    $ANTORA --clean --redirect-facility static --fetch $OPTS $PLAYBOOK
    echo "Built: file://$(pwd)/build/site/index.html"
)
