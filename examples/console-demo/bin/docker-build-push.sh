#!/usr/bin/env bash
set -xe

cd $(dirname "$0")/..
ECR=955308952094.dkr.ecr.eu-west-1.amazonaws.com/crux-console-demo
lein uberjar
docker build -t $ECR:latest .
docker push $ECR:latest
cd -
