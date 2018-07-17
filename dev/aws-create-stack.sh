#!/bin/bash

stack_name=CruxStack
delete=false
verbose=false

function jsonValue() {
    KEY=$1
    awk -F"[,:}]" '{for(i=1;i<=NF;i++){if($i~/'$KEY'\042/){print $(i+1)}}}' | tr -d '"'
}

function usage() {
    printf "
Options:\n\
  -d            First, delete any existing stack with the same name
  -h            Help\n\
  -k <string>   Name of an AWS keypair to use when SSHing to an instance\n\
  -n <string>   Name of the stack\n\
  -v            Verbose: prints details of the stack launch\n"
}

while getopts ":dhk:n:v" opt; do
    case "${opt}" in
        d )
            delete=true
            ;;
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

if [ $delete == true ]; then
    if [ $verbose == true ]; then
        echo "Deleting any existing stack called $stack_name..."
    fi
    aws cloudformation delete-stack \
        --stack-name $stack_name \
        >/dev/null
    
    if [ $verbose == true ]; then
        aws cloudformation wait stack-delete-complete \
            --stack-name $stack_name \
            >/dev/null
        echo "Stack deletion complete"
    fi
fi

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
        
        echo "Stack creation successful"

        lb_dns=$(aws elbv2 describe-load-balancers \
                     --names CruxLB \
                     | jsonValue DNSName)

        echo "Setting up the Crux instance..."
        aws ec2 wait instance-status-ok \
            --instance-id $cruxbox_id \
            >/dev/null
        
        server_status=$(curl -s -o /dev/null -w "%{http_code}" $lb_dns:3000)
        if [ $server_status == "200" ]; then
            echo "Crux setup successful"
            echo "Crux HTTP server running on$lb_dns:3000"
        else
            echo "Something went wrong with Zookeeper, Kafka, or Crux"
            echo "Crux HTTP server is supposed to be running on$lb_dns:3000 but did not respond with HTTP status 200"
        fi
    else
        echo "Stack creation failed"
    fi
fi
