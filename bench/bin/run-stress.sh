#!/usr/bin/env bash
set -e

# COMMAND = '["xtdb.bench.main", "foo", "bar"]'
COMMAND='["xtdb.bench.main", "--tests", "tpch-stress"'

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -r|--rev)
            REV="$2";
            shift 2;;
        --nodes|--tpch-query-count|--tpch-field-count)
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
                  --cpu "2 vCPU" \
                  --memory "4GB" \
                  --task-role-arn "arn:aws:iam::955308952094:role/crux-bench-ECSTaskRole-1QHM7XK4QT25X" \
                  --execution-role-arn "arn:aws:iam::955308952094:role/crux-bench-ECSTaskExecutionRole-14WW8A7NF1D2V" \
                  --network-mode "awsvpc" \
                  --container-definitions \
                  '[{
                      "name":"bench-container",
                      "cpu":2048,
                      "memory":4092,
                      "image":"955308952094.dkr.ecr.eu-west-2.amazonaws.com/crux-bench:commit-'${SHA}'",
                      "essential":true,
                      "secrets":[{"name":"SLACK_URL","valueFrom":"arn:aws:secretsmanager:eu-west-2:955308952094:secret:bench/slack-url-uumMHQ"}],
		      "entryPoint":["java","-cp","xtdb-bench-standalone.jar","-Xms1g","-Xmx1g","clojure.main", "-m"],
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
    --cluster crux-bench-stress \
    --launch-type FARGATE \
    --count 1 \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-5140ba2b],assignPublicIp=\"ENABLED\"}" \
    --overrides '{"containerOverrides": [{"name": "bench-container", "command": '"$COMMAND"'}]}' \
    --output json \
    | jq .failures
