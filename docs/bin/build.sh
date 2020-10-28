#!/usr/bin/env bash
set -x
set -e

OPTS=

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --with-local-bundle)
            OPTS+=" --ui-bundle-url=../../crux-site/build/ui-bundle.zip"
            shift;;
        *) echo "Unknown parameter passed: $1"; exit 1;;
    esac
done

(
    cd $(dirname "$0")/..
    antora --clean --fetch $OPTS antora-playbook.yml
)
