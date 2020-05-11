#!/bin/bash
set -xe

aws cloudformation update-stack \
    --capabilities CAPABILITY_IAM \
    --template-body file://$(dirname $0)/console-demo.yml \
    --stack-name crux-console-demo \
    --tags Key=juxt:team,Value=crux-core
