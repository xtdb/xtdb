#!/usr/bin/env bash

source config/env

function with-common-env() {
    export $(grep -v '^#' config/env | xargs -0)
}
