#!/usr/bin/env bash

set -e

sudo docker build -t crux-standalone-webservice ../../. -f Dockerfile
sudo docker tag crux-standalone-webservice juxt/crux-standalone-webservice
sudo docker push juxt/crux-standalone-webservice
