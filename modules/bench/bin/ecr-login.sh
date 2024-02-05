#!/usr/bin/env bash

set -xe

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ECR_URL="$ACCOUNT.dkr.ecr.eu-west-1.amazonaws.com"

aws ecr get-login-password \
    --region eu-west-1 \
    | docker login \
             --username AWS \
             --password-stdin "$ECR_URL"
