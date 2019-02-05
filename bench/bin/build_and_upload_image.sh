#!/usr/bin/env bash

set -e

sudo docker build -t crux-bench -f bench/Dockerfile .
sudo docker tag crux-bench juxt/crux-bench
sudo docker push juxt/crux-bench
