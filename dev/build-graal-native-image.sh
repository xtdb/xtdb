#!/bin/sh -e

: "${GRAAL_HOME?GRAAL_HOME is required. Download from https://github.com/oracle/graal/releases}"

command -v lein >/dev/null 2>&1 || { echo >&2 "lein required on PATH"; exit 1; }

JAVA_HOME=$GRAAL_HOME
PATH=$JAVA_HOME/bin:$PATH

LEIN_TARGET_DIR=./target
GRAAL_TARGET_DIR=/dev/shm/crux-graal-build-target

echo "Deleting $LEIN_TARGET_DIR and linking to $GRAAL_TARGET_DIR"

rm -rf $GRAAL_TARGET_DIR $LEIN_TARGET_DIR
mkdir -p $GRAAL_TARGET_DIR
ln -fs $GRAAL_TARGET_DIR $LEIN_TARGET_DIR

export CRUX_DISABLE_LIBGCRYPT=true
export CRUX_DISABLE_LIBCRYPTO=true

lein do version, with-profile graal,uberjar uberjar

UBERJAR=$(ls $LEIN_TARGET_DIR/*-standalone.jar)
REFLECTION_JSON_RESOURCE=graal_reflectconfig.json

native-image --no-server \
             --enable-http \
             -H:+ReportExceptionStackTraces \
             -H:ReflectionConfigurationResources=$REFLECTION_JSON_RESOURCE \
             -H:IncludeResources='.*/.*properties$' \
             -H:IncludeResources='.*/.*so$' \
             -H:IncludeResources='.*/.*xml$' \
             -H:IncludeResources='.*/.*json$' \
             -H:Path=$LEIN_TARGET_DIR \
             -Dclojure.compiler.direct-linking=true \
             -jar $UBERJAR
