#!/usr/bin/env bash

echo "creating new ami image"

export $(grep -v '^#' config/env | xargs -0) && \
    packer-io build -on-error=ask ami_packer_template.json
