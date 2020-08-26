#!/usr/bin/env bash
set -x
set -e

prefix_user=$(whoami)

if [ "$prefix_user" == "james" ]; then
    prefix_user=jms
fi

PREFIX=${1:-"/${OPENCRUX_PREFIX:-_$prefix_user}"}
echo Deploying to https://opencrux.com$PREFIX

cd $(dirname "$0")/..
aws s3 sync --delete build/site s3://opencrux-docs$PREFIX --cache-control no-cache
cd -
