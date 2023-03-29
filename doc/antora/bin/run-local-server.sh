#!/usr/bin/env bash
set -xe
(
    cd $(dirname "$0")/../build/site
    python3 -m http.server
)

