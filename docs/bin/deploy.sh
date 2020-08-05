#!/usr/bin/env bash
set -x
set -e

cd $(dirname "$0")/..
aws s3 sync --delete build/site s3://opencrux-docs --cache-control no-cache
cd -
