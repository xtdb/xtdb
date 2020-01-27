#!/usr/bin/env bash
ECR=955308952094.dkr.ecr.eu-west-2.amazonaws.com/crux-bench
lein uberjar
docker build -t $ECR:latest .
docker push $ECR:latest
