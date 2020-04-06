#!/usr/bin/env bash
aws ecs run-task\
 --task-definition crux-bench-dev\
 --cluster crux-bench\
 --overrides "{\"containerOverrides\": [{\"name\":\"bench-container\",\"command\":[\"crux.bench.watdiv-neo4j\"]}]}"\
 --launch-type FARGATE\
 --count 1\
 --network-configuration \
 "awsvpcConfiguration={subnets=[subnet-5140ba2b],assignPublicIp=\"ENABLED\"}"\
 --output json
