#!/bin/bash

ssh_key_name=juxt_keypair_london
stack_name=CruxStack
verbose=false

function jsonValue() {
    KEY=$1
    awk -F"[,:}]" '{for(i=1;i<=NF;i++){if($i~/'$KEY'\042/){print $(i+1)}}}' | tr -d '"'
}

function usage() {
    printf "
Options:\n\
  -h            Help\n\
  -k <string>   Name of an AWS keypair to use when SSHing to an instance\n\
  -n <string>   Name of the stack\n\
  -v            Verbose: prints details of the stack launch\n"
}

while getopts ":hk:n:v" opt; do
    case "${opt}" in
        h )
            usage
            exit 0
            ;;
        k )
            ssh_key_name=$OPTARG
            ;;
        n )
            stack_name=$OPTARG
            ;;
        v )
            verbose=true
            ;;
        \? )
            echo "Invalid option: -$OPTARG"
            echo "Use -h to list valid options"
            exit 1
            ;;
    esac
done

aws cloudformation create-stack \
    --stack-name $stack_name \
    --template-body file://aws-cf-template.json \
    --parameters \
    ParameterKey=SSHKeyName,ParameterValue=$ssh_key_name \
    --capabilities CAPABILITY_NAMED_IAM \
    >/dev/null

if [ $verbose == true ]; then
    echo "Creating the stack..."
    aws cloudformation wait stack-create-complete \
        --stack-name $stack_name \
        >/dev/null

    stack_status=$(aws cloudformation describe-stacks \
                       --stack-name $stack_name \
                       | jsonValue StackStatus)

    if [ $stack_status == "CREATE_COMPLETE" ]; then
        
        echo "Stack creation successful."

        cruxbox_id=$(aws cloudformation describe-stack-resource \
                         --stack-name $stack_name \
                         --logical-resource-id InstCrux1 \
                         | jsonValue PhysicalResourceId)

        cruxbox_ip=$(aws ec2 describe-instances \
                         --instance-id $cruxbox_id \
                         | jsonValue PublicIpAddress)

        echo "Setting up the Crux instance..."
        aws ec2 wait instance-status-ok \
            --instance-id $cruxbox_id \
            >/dev/null

        echo "Crux setup finished."
        echo "If all went well, Crux's HTTP server should be up at$cruxbox_ip:3000"
        
    else
        echo "Stack creation failed."
    fi
fi
