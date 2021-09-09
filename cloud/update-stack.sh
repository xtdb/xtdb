#!/bin/bash
set -e

if [ $# != 1 ]; then
    echo "expected: $(basename $0) <template-file>"
    exit 1
fi

TEMPLATE_FILE=$1
STACK_NAME=`basename $TEMPLATE_FILE`
STACK_NAME=${STACK_NAME%.yml}

OP=update-stack
aws cloudformation describe-stacks --stack-name $STACK_NAME 2>/dev/null >/dev/null || OP=create-stack

aws cloudformation $OP \
    --capabilities CAPABILITY_IAM \
    --template-body "file://$(realpath $TEMPLATE_FILE)" \
    --region eu-west-1 \
    --stack-name $STACK_NAME
