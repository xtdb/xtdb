#!/usr/bin/env bash

(
    cd $(dirname $0)/..

    for x in writes-log-file \
                 can-build-chunk-as-arrow-ipc-file-format \
             ; do

        echo copying $x...

        cp core/target/$x/objects/*.json \
           core/test-resources/$x/
    done

    cp core/target/can-submit-tpch-docs-0.001/objects/metadata*.json \
       core/test-resources/can-submit-tpch-docs-0.001/

    cp core/target/can-submit-tpch-docs-0.01/objects/metadata*.json \
       core/test-resources/can-submit-tpch-docs-0.01/
)
