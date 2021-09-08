#!/usr/bin/env bash
set -x
docker build -t ${IMAGE_NAME:-xtdb-custom}:${IMAGE_VERSION:-latest} .
