#! /bin/env bash
aws ecs run-task\
 --task-definition example-task:1\
 --cluster example-cluster\
 --launch-type FARGATE\
 --count 1\
 --network-configuration \
 "awsvpcConfiguration={subnets=[subnet-5140ba2b],assignPublicIp=\"ENABLED\"}"\
 --output json
