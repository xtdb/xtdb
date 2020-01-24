#!/usr/bin/env bash
ECR=955308952094.dkr.ecr.eu-west-2.amazonaws.com/crux-bench
docker build -t $ECR:latest -f crux-bench/ .
docker push $ECR:latest
