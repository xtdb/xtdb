#!/usr/bin/env bash

set -e
(
    echo Clearing Blob Store Container - xtdbazurebenchmarkcontainer ...
    az storage blob delete-batch --account-name xtdbazurebenchmark --source xtdbazurebenchmarkcontainer
    echo Done
)
