#!/usr/bin/env bash

set -xe

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ECR_URL="$ACCOUNT.dkr.ecr.eu-west-1.amazonaws.com"
TAG="$ECR_URL/xtdb2-bench:latest"

docker tag juxt.xtdb2/bench:latest $TAG

docker push $TAG
