#!/usr/bin/env bash

set -xe

REGION="eu-west-1"
STACK_NAME="xtdb2-bench"

echo "Checking if stack exists ..."
(
    cd $(dirname $0)/../

    if ! aws cloudformation describe-stacks --region $REGION --stack-name $STACK_NAME ; then

        echo -e "\nStack does not exist, creating ..."
        aws cloudformation create-stack \
            --region $REGION \
            --capabilities CAPABILITY_IAM \
            --template-body file://$(pwd)/cloudformation/bench.yml \
            --stack-name $STACK_NAME \
            --tags Key=juxt:team,Value=xtdb-core


        echo "Waiting for stack to be created ..."
        aws cloudformation wait stack-create-complete \
            --region $REGION \
            --stack-name $STACK_NAME

    else

        echo "\nUpdating existing stack ..."
        aws cloudformation update-stack \
            --region $REGION \
            --capabilities CAPABILITY_IAM \
            --template-body file://$(pwd)/cloudformation/bench.yml \
            --stack-name $STACK_NAME \
            --tags Key=juxt:team,Value=xtdb-core

    fi
)
