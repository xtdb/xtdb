#!/usr/bin/env bash
aws ecs run-task\
 --task-definition time-series-bench\
 --cluster crux-bench\
 --launch-type FARGATE\
 --count 1\
 --network-configuration \
 "awsvpcConfiguration={subnets=[subnet-5140ba2b],assignPublicIp=\"ENABLED\"}"\
 --output json
