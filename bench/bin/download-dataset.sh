#!/usr/bin/env bash
set -x
set -e
cd $(dirname "$0")/..
mkdir -p data
[ -f data/devices.sql ] || curl -L https://assets.timescale.com/docs/downloads/devices_small.tar.gz | tar xz -C data
[ -f data/weather.sql ] || curl -L https://assets.timescale.com/docs/downloads/weather_small.tar.gz | tar xz -C data
[ -f data/watdiv.10M.nt ] || curl -L https://dsg.uwaterloo.ca/watdiv/watdiv.10M.tar.bz2 | tar xj -C data
[ -d data/watdiv-stress-100 ] || curl -L https://dsg.uwaterloo.ca/watdiv/stress-workloads.tar.gz | tar xz -C data
cd -
