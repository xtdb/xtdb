#!/bin/bash
set -e
set -x

aws s3 sync --delete --exclude '*.sh' $(dirname $0) s3://crux-cloudformation/crux-soak/

aws cloudformation update-stack \
    --capabilities CAPABILITY_IAM \
    --template-url https://crux-cloudformation.s3-eu-west-1.amazonaws.com/crux-soak/crux-soak.yml \
    --stack-name crux-soak \
    --tags Key=juxt:team,Value=crux-core
