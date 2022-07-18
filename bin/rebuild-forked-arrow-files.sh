#!/usr/bin/env bash

set -e

if [[ "$#" -eq 0 ]]; then echo "Usage: $0 <path-to-arrow-fork>"; exit 1; fi

ARROW_PATH=$1
CORE2_PATH=$(realpath $(dirname $0)/..)

mkdir -p "$CORE2_PATH/core/src/org/apache/arrow/vector/complex"

(
    cd $1/java
    mvn -pl vector compile -am

    echo
    echo Copying files...

    cp vector/target/generated-sources/org/apache/arrow/vector/complex/DenseUnionVector.java "$CORE2_PATH/core/src/org/apache/arrow/vector/complex/"
    cp vector/src/main/java/org/apache/arrow/vector/complex/AbstractContainerVector.java "$CORE2_PATH/core/src/org/apache/arrow/vector/complex/"
    cp vector/src/main/java/org/apache/arrow/vector/ipc/JsonFileWriter.java "$CORE2_PATH/core/src/org/apache/arrow/vector/ipc/"
)
