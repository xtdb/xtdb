#!/usr/bin/env bash
aws ecs run-task\
 --task-definition xtdb-bench\
 --cluster xtdb-bench\
 --overrides "{\"containerOverrides\": [{\"name\":\"bench-container\",\"command\":[\"xtdb.bench.watdiv-rdf4j\"]}]}"\
 --launch-type EC2\
 --count 1\
 --output json
