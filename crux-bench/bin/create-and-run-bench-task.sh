#!/usr/bin/env bash
COMMIT_SHA="$(git rev-parse ${COMMIT_ISH})"

aws ecs register-task-definition\
  --family "crux-bench-${COMMIT_SHA}"\
  --cpu "4 vCPU"\
  --memory "12GB"\
  --task-role-arn "arn:aws:iam::955308952094:role/crux-bench-ECSTaskRole-1QHM7XK4QT25X"\
  --execution-role-arn "arn:aws:iam::955308952094:role/crux-bench-ECSTaskExecutionRole-14WW8A7NF1D2V"\
  --network-mode "awsvpc"\
  --container-definitions \
  '[{"name":"zookeeper-container","cpu":1024,"memory":2048,"image":"confluentinc/cp-zookeeper:5.3.1","essential":true,"environment":[{"name":"ZOOKEEPER_CLIENT_PORT","value":"2181"},{"name":"ZOOKEEPER_TICK_TIME","value":"2000"}],"portMappings":[{"containerPort":2181}]},{"name":"broker-container","cpu":1024,"memory":2048,"image":"confluentinc/cp-enterprise-kafka:5.3.1","dependsOn":[{"condition":"START","containerName":"zookeeper-container"}],"essential":true,"environment":[{"name":"KAFKA_BROKER_ID","value":"1"},{"name":"KAFKA_ZOOKEEPER_CONNECT","value":"localhost:2181"},{"name":"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR","value":"1"},{"name":"KAFKA_ADVERTISED_LISTENERS","value":"PLAINTEXT://localhost:9092"}],"portMappings":[{"containerPort":9092}]},{"name":"bench-container","cpu":2048,"memory":8146,"image":"955308952094.dkr.ecr.eu-west-2.amazonaws.com/crux-bench:commit-'${COMMIT_SHA}'","dependsOn":[{"condition":"START","containerName":"broker-container"}],"essential":true,"secrets":[{"name":"SLACK_URL","valueFrom":"arn:aws:secretsmanager:eu-west-2:955308952094:secret:bench/slack-url-uumMHQ"}]}]'

aws ecs run-task\
 --task-definition "crux-bench-${COMMIT_SHA}"\
 --cluster crux-bench\
 --launch-type FARGATE\
 --count 1\
 --network-configuration \
 "awsvpcConfiguration={subnets=[subnet-5140ba2b],assignPublicIp=\"ENABLED\"}"\
 --output json
