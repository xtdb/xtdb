#!/bin/bash
set -e
set -x

aws s3 sync --delete --exclude '*.sh' $(dirname $0) s3://crux-cloudformation/crux-cloud/

aws cloudformation update-stack \
    --capabilities CAPABILITY_IAM \
    --template-url https://crux-cloudformation.s3-eu-west-1.amazonaws.com/crux-cloud/crux-cloud.yml \
    --stack-name crux-cloud \
    --tags Key=juxt:team,Value=crux-core
