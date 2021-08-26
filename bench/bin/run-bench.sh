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

TASKDEF_ARN=$(aws ecs register-task-definition\
                  --family "crux-bench-dev" \
                  --cpu "4 vCPU" \
                  --memory "12GB" \
                  --task-role-arn "arn:aws:iam::955308952094:role/crux-bench-ECSTaskRole-1QHM7XK4QT25X" \
                  --execution-role-arn "arn:aws:iam::955308952094:role/crux-bench-ECSTaskExecutionRole-14WW8A7NF1D2V" \
                  --network-mode "awsvpc" \
                  --container-definitions \
                  '[{
                      "name":"postgres",
                      "image":"postgres:13.2",
                      "essential":true,
                      "environment":[{"name":"POSTGRES_PASSWORD", "value":"postgres"}],
                      "portMappings":[{"containerPort":5432}]
                    }, {
                      "name":"zookeeper-container",
                      "image":"confluentinc/cp-zookeeper:5.3.1",
                      "essential":true,
                      "environment":[{"name":"ZOOKEEPER_CLIENT_PORT", "value":"2181"},{"name":"ZOOKEEPER_TICK_TIME", "value":"2000"}],
                      "portMappings":[{"containerPort":2181}]
                    }, {
                      "name":"broker-container",
                      "image":"confluentinc/cp-enterprise-kafka:5.3.1",
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
                      "image":"955308952094.dkr.ecr.eu-west-2.amazonaws.com/crux-bench:commit-'${SHA}'",
                      "dependsOn":[{"condition":"START","containerName":"broker-container"}],
                      "essential":true,
                      "secrets":[{"name":"SLACK_URL","valueFrom":"arn:aws:secretsmanager:eu-west-2:955308952094:secret:bench/slack-url-uumMHQ"}],
                      "logConfiguration":{
                        "logDriver":"awslogs",
                        "options": {
                          "awslogs-region":"eu-west-2",
                          "awslogs-group":"crux-bench-dev",
                          "awslogs-stream-prefix":"'$(whoami)'-'${COMMIT_SHA}'"
                        }
                      }
                    }]' \
        | jq -r .taskDefinition.taskDefinitionArn )

echo "Starting ECS task @ ${SHA:0:8}. Failures:"
aws ecs run-task \
    --task-definition "$TASKDEF_ARN" \
    --cluster crux-bench \
    --launch-type FARGATE \
    --count 1 \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-5140ba2b],assignPublicIp=\"ENABLED\"}" \
    --overrides '{"containerOverrides": [{"name": "bench-container", "command": '"$COMMAND"'}]}' \
    --output json \
    | jq .failures
