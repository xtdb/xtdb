#!/usr/bin/env bash
set -x
set -e

PREFIX=${1:-"/_$(whoami)"}
echo Deploying to https://opencrux.com$PREFIX

cd $(dirname "$0")/..
aws s3 sync --delete build/site s3://opencrux-docs$PREFIX --cache-control no-cache
cd -
