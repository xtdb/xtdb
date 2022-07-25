#!/usr/bin/env bash

(
    for x in writes-log-file \
             can-build-chunk-as-arrow-ipc-file-format \
             can-handle-dynamic-cols-in-same-block \
             can-index-sql-insert \
             multi-block-metadata \
             ; do

        echo "copying $x..."

        cp target/$x/objects/*.json \
           $(dirname $0)/../test-resources/$x/
    done

    for x in can-submit-tpch-docs-0.001 \
             can-submit-tpch-docs-0.01 \
             ; do

        echo "copying $x (metadata only)..."

        cp target/$x/objects/metadata*.json \
           $(dirname $0)/../test-resources/$x/
    done
)
