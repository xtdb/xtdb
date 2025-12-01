#!/usr/bin/env bash

set -xe

(
    cd $(dirname $0)/../

    aws cloudformation update-stack \
        --region eu-west-1 \
        --capabilities CAPABILITY_IAM \
        --template-body file://$(pwd)/cloudformation/bench.yml \
        --stack-name core2-bench \
        --tags Key=juxt:team,Value=xtdb-core
)
