#!/usr/bin/env bash

source "${BASH_SOURCE%/*}/shared.sh"

set -e

cd ..
sudo docker build -t crux .
sudo docker tag crux ${AWS_ECR_URI}
sudo docker push ${AWS_ECR_URI}
