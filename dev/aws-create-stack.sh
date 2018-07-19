#!/bin/bash

region=eu-west-2
stack_name=CruxStack
delete=false
verbose=false

function usage() {
    printf "
Options:\n\
  -d            First, delete any existing stack with the same name
  -h            Help\n\
  -k <string>   Name of an AWS keypair to use when SSHing to an instance\n\
  -n <string>   Name of the stack\n\
  -v            Verbose: prints details of the stack launch\n"
}

############################################
# Utils
function jsonValue() {
    KEY=$1
    awk -F"[,:}]" '{for(i=1;i<=NF;i++){if($i~/'$KEY'\042/){print $(i+1)}}}' | tr -d '"'
}

function vecho() {
    if [ $verbose == true ]; then
        echo $1
    fi
}

############################################
# Steps of the stack creation
function createBucket() {
    vecho "Uploading files to setup bucket..."
    bucket=crux-bucket-$(uuidgen)
    aws s3api create-bucket \
        --bucket $bucket \
        --create-bucket-configuration \
        LocationConstraint=$region \
        --acl public-read \
        >/dev/null

    aws s3 cp aws-cft-crux.yaml s3://$bucket >/dev/null
    aws s3 cp aws-cft-kafka.yaml s3://$bucket >/dev/null
    aws s3 cp aws-cft-zoo.yaml s3://$bucket >/dev/null
    aws s3 cp aws-userdata-cruxbox.sh s3://$bucket >/dev/null
    aws s3 cp aws-userdata-kafkabox.sh s3://$bucket >/dev/null
    vecho "Done"
}

function deleteBucket() {
    vecho "Deleting setup bucket..."
    aws s3 rm s3://$bucket --recursive >/dev/null
    aws s3api delete-bucket --bucket $bucket >/dev/null
    vecho "Done"
}

function deleteOldStack() {
    vecho "Deleting any existing stack called $stack_name..."
    aws cloudformation delete-stack \
        --stack-name $stack_name \
        >/dev/null
    
    aws cloudformation wait stack-delete-complete \
        --stack-name $stack_name \
        >/dev/null
    vecho "Done"
}

function createStack() {
    vecho "Creating the stack..."
    aws cloudformation create-stack \
        --stack-name $stack_name \
        --template-body file://aws-cft-common.yaml \
        --parameters \
        ParameterKey=SSHKeyPair,ParameterValue=$ssh_key_name \
        ParameterKey=SetupBucketURL,ParameterValue=http://$bucket.s3.amazonaws.com \
        --capabilities CAPABILITY_NAMED_IAM \
        >/dev/null

    aws cloudformation wait stack-create-complete \
        --stack-name $stack_name \
        >/dev/null

    stack_status=$(aws cloudformation describe-stacks \
                       --stack-name $stack_name \
                       | jsonValue StackStatus)
    if [ $stack_status == "CREATE_COMPLETE" ]; then
        vecho "Done"
    else
        echo "Stack creation failed!"
    fi
}

function waitForCrux() {
    lb_dns=$(aws elbv2 describe-load-balancers \
                 --names CruxLB \
                 | jsonValue DNSName)
    cruxbox_id=$(aws cloudformation describe-stack-resource \
                     --stack-name $stack_name \
                     --logical-resource-id InstCrux1 \
                     | jsonValue PhysicalResourceId)

    vecho "Setting up the Crux cluster..."
    aws ec2 wait instance-status-ok \
        --instance-id $cruxbox_id \
        >/dev/null
    
    server_status=$(curl -s -o /dev/null -w "%{http_code}" $lb_dns:3000)
    if [ $server_status == "200" ]; then
        vecho "Done"
        echo "Crux HTTP server running on$lb_dns:3000"
    else
        echo "Crux setup failed!"
        vecho "Something went wrong with Zookeeper, Kafka, or Crux"
        vecho "Crux HTTP server is supposed to be running on$lb_dns:3000 but did not respond with HTTP status 200"
    fi
}

############################################
# Arguments parsing
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

############################################
# Main
if [ $delete == true ]; then
    deleteOldStack
fi

createBucket
createStack
deleteBucket

if [ $stack_status == "CREATE_COMPLETE" ]; then
    waitForCrux
fi
