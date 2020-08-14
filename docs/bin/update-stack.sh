#!/bin/bash
set -e
set -x

aws s3 sync --delete --exclude '*.sh' $(dirname $0)/../cloudformation/ s3://crux-cloudformation/opencrux-docs/

aws cloudformation update-stack \
    --capabilities CAPABILITY_IAM \
    --template-url https://crux-cloudformation.s3-eu-west-1.amazonaws.com/opencrux-docs/static-site.yml \
    --stack-name opencrux-docs \
    --tags Key=juxt:team,Value=crux-core
