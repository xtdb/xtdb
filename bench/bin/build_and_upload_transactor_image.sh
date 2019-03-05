#!/usr/bin/env bash

set -e

docker build -t datomic-transactor-free -f docker-files/datomic_transactor .
docker tag datomic-transactor-free juxt/datomic-transactor-free
docker push juxt/datomic-transactor-free
