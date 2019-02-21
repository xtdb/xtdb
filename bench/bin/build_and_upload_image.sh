#!/usr/bin/env bash

set -e

docker build -t crux-bench-base -f docker-files/Dockerfile.base .
docker build -t crux-bench -f docker-files/Dockerfile.production .
docker tag crux-bench juxt/crux-bench
docker push juxt/crux-bench
