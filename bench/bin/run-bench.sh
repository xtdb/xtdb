#!/usr/bin/env bash
set -e

# COMMAND = '["xtdb.bench.main", "foo", "bar"]'
COMMAND='["xtdb.bench.main"'

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -r|--rev)
            REV="$2";
            shift 2;;
        --nodes|--tests|--tpch-query-count|--tpch-field-count|--repeat)
            COMMAND+=", \"$1\", \"$2\""
            shift 2;;
        *) echo "Unknown parameter passed: $1"; exit 1;;
    esac
done

COMMAND+="]"
REV=${REV:-HEAD}
SHA="$(git rev-parse ${REV})"

REGION=$(aws configure get region)
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
STACK_ARN=$(aws cloudformation describe-stacks --stack-name 'xtdb-bench' --query 'Stacks[0].StackId' --output text)
TASK_ROLE=$(aws cloudformation describe-stack-resource --logical-resource-id ECSTaskRole --stack-name "$STACK_ARN" --output text --query StackResourceDetail.PhysicalResourceId)
EXECUTION_ROLE=$(aws cloudformation describe-stack-resource --logical-resource-id ECSTaskExecutionRole --stack-name "$STACK_ARN" --output text --query StackResourceDetail.PhysicalResourceId)
ECR_REPO_URI=$(aws ecr describe-repositories --repository-names xtdb-bench --output text --query 'repositories[0].repositoryUri')

CONTAINER_DEFINITIONS='
[
  {
    "name":"postgres",
    "image":"postgres:13.2",
    "essential":true,
    "environment":[{"name":"POSTGRES_PASSWORD", "value":"postgres"}],
    "portMappings":[{"containerPort":5432}]
  }, {
    "name":"zookeeper-container",
    "image":"confluentinc/cp-zookeeper:6.1.1",
    "essential":true,
    "environment":[{"name":"ZOOKEEPER_CLIENT_PORT", "value":"2181"},{"name":"ZOOKEEPER_TICK_TIME", "value":"2000"}],
    "portMappings":[{"containerPort":2181}]
  }, {
    "name":"broker-container",
    "image":"confluentinc/cp-kafka:6.1.1",
    "dependsOn":[{"condition":"START","containerName":"zookeeper-container"}],
    "essential":true,
    "environment":[{"name":"KAFKA_BROKER_ID","value":"1"},
                   {"name":"KAFKA_ZOOKEEPER_CONNECT","value":"localhost:2181"},
                   {"name":"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR","value":"1"},
                   {"name":"KAFKA_ADVERTISED_LISTENERS","value":"PLAINTEXT://localhost:9092"}],
    "portMappings":[{"containerPort":9092}]
  },{
    "name":"bench-container",
    "cpu":2048,
    "memory":8192,
    "image":"'${ECR_REPO_URI}':commit-'${SHA}'",
    "dependsOn":[{"condition":"START","containerName":"broker-container"}],
    "essential":true,
    "secrets":[{"name":"BENCH_SECRETS","valueFrom":"/aws/reference/secretsmanager/xtdb-bench"}],
    "logConfiguration":{
      "logDriver":"awslogs",
      "options": {
        "awslogs-region":"'$REGION'",
        "awslogs-group":"xtdb-bench-dev",
        "awslogs-stream-prefix":"'$(whoami)'-'${COMMIT_SHA}'"
      }
    }
  }
]'

TASKDEF_ARN=$(aws ecs register-task-definition\
                  --family "xtdb-bench-dev" \
                  --cpu "4 vCPU" \
                  --memory "12GB" \
                  --task-role-arn "$TASK_ROLE" \
                  --execution-role-arn "$EXECUTION_ROLE" \
                  --network-mode "awsvpc" \
                  --container-definitions "$CONTAINER_DEFINITIONS"\
                  --output text --query taskDefinition.taskDefinitionArn)

VPC_STACK_ARN=$(aws cloudformation describe-stacks --stack-name 'xtdb-vpc' --query 'Stacks[0].StackId' --output text)
SUBNET=$(aws cloudformation describe-stack-resource --logical-resource-id PublicSubnetOne --stack-name "$VPC_STACK_ARN" --output text --query StackResourceDetail.PhysicalResourceId)

echo "Starting ECS task @ ${SHA:0:8}. Failures:"
aws ecs run-task \
    --task-definition "$TASKDEF_ARN" \
    --cluster xtdb-bench \
    --launch-type FARGATE \
    --count 1 \
    --network-configuration "awsvpcConfiguration={subnets=[$SUBNET],assignPublicIp=\"ENABLED\"}" \
    --overrides '{"containerOverrides": [{"name": "bench-container", "command": '"$COMMAND"'}]}' \
    --output json \
    | jq .failures
