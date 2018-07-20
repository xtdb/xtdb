#!/bin/bash

region=eu-west-2
stack_name=RootStack
stack_status=nil
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
        >/dev/null
    aws s3 cp aws-cft-crux.yaml s3://$bucket >/dev/null
    aws s3 cp aws-cft-kafka.yaml s3://$bucket >/dev/null
    aws s3 cp aws-cft-zoo.yaml s3://$bucket >/dev/null
    aws s3 cp aws-userdata-cruxbox.sh s3://$bucket \
        --acl public-read \
        >/dev/null
    aws s3 cp aws-userdata-kafkabox.sh s3://$bucket \
        --acl public-read \
        >/dev/null
    aws s3 cp aws-userdata-zoobox.sh s3://$bucket \
        --acl public-read \
        >/dev/null
}

function deleteBucket() {
    vecho "Deleting setup bucket..."
    aws s3 rm s3://$bucket --recursive >/dev/null
    aws s3api delete-bucket --bucket $bucket >/dev/null
}

function deleteOldStack() {
    vecho "Deleting any existing stack called $stack_name..."
    aws cloudformation delete-stack \
        --stack-name $stack_name \
        >/dev/null
    
    aws cloudformation wait stack-delete-complete \
        --stack-name $stack_name \
        >/dev/null
}

function createStack() {
    vecho "Creating the stack..."
    aws cloudformation create-stack \
        --stack-name $stack_name \
        --template-body file://aws-cft-root.yaml \
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
                       --query 'Stacks[0].StackStatus' \
                       --output text)
}

function waitForCrux() {
    vecho "Setting up the CRUX cluster..."
    cruxbox_ids=$(aws ec2 describe-instances \
                       --filters Name=instance-state-name,Values=running,Name=tag:Name,Values=CruxBox \
                       --query 'Reservations[*].Instances[0].InstanceId' \
                       --output text)
    aws ec2 wait instance-status-ok \
        --instance-ids $cruxbox_ids \
        >/dev/null
    # crux_stack_physid=$(aws cloudformation describe-stack-resources \
    #                       --stack-name $stack_name \
    #                       --logical-resource-id CruxStack \
    #                       --query 'StackResources[0].PhysicalResourceId' \
    #                       --output text)
    # crux_stack_name=$(aws cloudformation list-stacks \
    #                       --stack-status-filter CREATE_COMPLETE \
    #                       --query "StackSummaries[?StackId=='$crux_stack_physid'].StackName" \
    #                       --output text)

    vecho "Testing the CRUX HTTP server..."
    lb_dns=$(aws elbv2 describe-load-balancers \
                 --names CruxLB \
                 --query 'LoadBalancers[0].DNSName' \
                 --output text)
    server_status=$(curl -s -o /dev/null -w "%{http_code}" $lb_dns:3000)
    if [ $server_status == "200" ]; then
        echo "CRUX HTTP server running on$lb_dns:3000"
    else
        echo "CRUX setup failed!"
        vecho "Something went wrong with Zookeeper, Kafka, or CRUX"
        vecho "CRUX HTTP server is supposed to be running on $lb_dns:3000 but did not respond with HTTP status 200"
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

if [ $stack_status == "CREATE_COMPLETE" ]; then
    waitForCrux
else
    echo "Stack creation failed!"
fi

deleteBucket
