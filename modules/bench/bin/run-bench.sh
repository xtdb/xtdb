#!/usr/bin/env bash

set -e

TASKDEF_ARN=$(aws cloudformation describe-stack-resource \
                  --stack-name xtdb2-bench \
                  --logical-resource-id BenchTask \
                  --query 'StackResourceDetail.PhysicalResourceId' \
                  --output text)

SUBNET_ID=$(aws cloudformation describe-stack-resource \
                --stack-name xtdb2-bench \
                --logical-resource-id PublicSubnet1 \
                --query 'StackResourceDetail.PhysicalResourceId' \
                --output text)

COUNT=1

# COMMAND = '["xtdb.bench", "--scale-factor", "0.01"]'
COMMAND='['

if [[ "$#" -eq 0 ]]; then echo "No benchmark passed."; exit 1; fi

case $1 in
    tpch|ts-devices)
        COMMAND+='"'"xtdb.bench.$1"'"'
        shift 1;;
    *) echo "Unknown benchmark passed: $1"; exit 1;;
esac


while [[ "$#" -gt 0 ]]; do
    case $1 in
        --scale-factor|--size)
            COMMAND+=", \"$1\", \"$2\""
            shift 2;;
        --count)
            COUNT=$2
            shift 2;;
        *) echo "Unknown parameter passed: $1"; exit 1;;
    esac
done

COMMAND+="]"

set -x

aws ecs run-task \
    --task-definition "$TASKDEF_ARN" \
    --cluster xtdb2-bench \
    --launch-type FARGATE \
    --count "$COUNT" \
    --network-configuration "awsvpcConfiguration={subnets=[$SUBNET_ID],assignPublicIp=\"ENABLED\"}" \
    --overrides '{"containerOverrides": [{"name": "bench", "command": '"$COMMAND"'}]}' \
    --output json \
    | jq .failures
