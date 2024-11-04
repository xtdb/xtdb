#!/usr/bin/env bash

set -e

if [[ "$#" -eq 0 ]]; then echo "Usage: $0 <path-to-arrow-fork>"; exit 1; fi

ARROW_PATH=$1
XTDB_PATH=$(realpath $(dirname $0)/..)

mkdir -p "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/"{complex/impl,ipc}

(
    cd $1/java
    mvn -pl vector package -am

    echo
    echo Copying files...

    cp vector/target/generated-sources/fmpp/org/apache/arrow/vector/complex/DenseUnionVector.java "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/complex/"
    cp vector/target/generated-sources/fmpp/org/apache/arrow/vector/complex/UnionVector.java "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/complex/"
    cp vector/src/main/java/org/apache/arrow/vector/complex/AbstractContainerVector.java "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/complex/"
    cp vector/target/generated-sources/fmpp/org/apache/arrow/vector/complex/impl/NullableStructWriter.java "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/complex/impl/"
    cp vector/target/generated-sources/fmpp/org/apache/arrow/vector/complex/impl/UnionWriter.java "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/complex/impl/"
    cp vector/src/main/java/org/apache/arrow/vector/ipc/JsonFileWriter.java "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/ipc/"

    cp vector/target/generated-sources/fmpp/org/apache/arrow/vector/complex/impl/PromotableWriter.java "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/complex/impl/"
    cp vector/src/main/java/org/apache/arrow/vector/BaseLargeVariableWidthVector.java "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/"
    cp vector/src/main/java/org/apache/arrow/vector/BaseVariableWidthVector.java "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/"
    cp vector/src/main/java/org/apache/arrow/vector/complex/ListVector.java "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/complex/"
    cp vector/src/main/java/org/apache/arrow/vector/complex/FixedSizeListVector.java "$XTDB_PATH/core/src/main/java/org/apache/arrow/vector/complex/"
)
