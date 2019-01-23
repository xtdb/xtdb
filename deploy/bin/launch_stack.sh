#!/usr/bin/env bash

source "${BASH_SOURCE%/*}/shared.sh"

echo "deploying cloudformation stack"

set -Exeuo pipefail

with-common-env && \
    aws cloudformation deploy \
        --stack-name ${AWS_CLOUDFORMATION_STACK_NAME} \
        --template-file cloudformation/stack.yml \
        --capabilities CAPABILITY_NAMED_IAM \
        --parameter-overrides \
        SSHKeyPair="default-key" \
        AmiImage=${AWS_AMI}
