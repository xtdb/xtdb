#!/usr/bin/env bash

source "${BASH_SOURCE%/*}/shared.sh"

echo "creating new ami image"

with-common-env && \
    packer-io build -on-error=ask ami_packer_template.json
