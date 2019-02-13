#!/usr/bin/env bash

set -e

docker build -t crux-standalone-webservice .
docker tag crux-standalone-webservice juxt/crux-standalone-webservice
docker push juxt/crux-standalone-webservice
