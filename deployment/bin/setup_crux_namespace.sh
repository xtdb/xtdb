#!/usr/bin/env bash

source "${BASH_SOURCE%/*}/shared.sh"

kubectl create -f templates/crux-namespace.yml
