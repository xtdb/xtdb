#!/bin/bash

stack_name=CruxStack
ssh_key_name=juxt_keypair_london

function jsonValue() {
    KEY=$1
    awk -F"[,:}]" '{for(i=1;i<=NF;i++){if($i~/'$KEY'\042/){print $(i+1)}}}' | tr -d '"'
}


aws cloudformation create-stack \
    --stack-name $stack_name \
    --template-body file://aws-cf-template.json \
    --parameters \
    ParameterKey=SSHKeyName,ParameterValue=$ssh_key_name \
    --capabilities CAPABILITY_NAMED_IAM \
    >/dev/null

echo "Stack creation in progress..."

aws cloudformation wait stack-create-complete \
    --stack-name $stack_name

#TODO Catch stack creation failures.

echo "Stack creation finished."

cruxbox_id=$(aws cloudformation describe-stack-resource \
                 --stack-name $stack_name \
                 --logical-resource-id InstCrux \
                 | jsonValue PhysicalResourceId)

cruxbox=$(aws ec2 describe-instances \
              --instance-id $cruxbox_id \
              | jsonValue PublicIpAddress)

echo "In a minute, if all went well, Crux's HTTP server will be up at$cruxbox:3000"
