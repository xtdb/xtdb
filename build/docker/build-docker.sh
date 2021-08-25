#!/usr/bin/env bash
set -x
docker build -t ${IMAGE_NAME:-crux-custom}:${IMAGE_VERSION:-latest} .
