#!/usr/bin/env bash

set -xe

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ECR_URL="$ACCOUNT.dkr.ecr.eu-west-1.amazonaws.com"
TAG="$ECR_URL/core2-bench:latest"

docker tag ghcr.io/xtdb/core2-bench:latest $TAG

docker push $TAG
