#!/usr/bin/env bash
set -x
set -e

cd $(dirname "$0")/..
antora --clean --pull antora-playbook.yml
cd -
