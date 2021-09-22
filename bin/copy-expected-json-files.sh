#!/usr/bin/env bash

(
    for x in     writes-log-file \
                 can-build-chunk-as-arrow-ipc-file-format \
                 can-handle-dynamic-cols-in-same-block \
             ; do

        echo copying $x...

        cp target/$x/objects/*.json \
           $(dirname $0)/../core/test-resources/$x/
    done

    cp target/can-submit-tpch-docs-0.001/objects/metadata*.json \
       $(dirname $0)/../core/test-resources/can-submit-tpch-docs-0.001/

    cp target/can-submit-tpch-docs-0.01/objects/metadata*.json \
       $(dirname $0)/../core/test-resources/can-submit-tpch-docs-0.01/
)
