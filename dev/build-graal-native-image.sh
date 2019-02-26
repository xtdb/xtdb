#!/bin/sh -e

: "${GRAAL_HOME?GRAAL_HOME is required. Download from https://github.com/oracle/graal/releases}"

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

REFLECTION_JSON=./resources/graal_reflectconfig.json

native-image --no-server \
             -H:+ReportExceptionStackTraces \
             -H:+ReportUnsupportedElementsAtRuntime \
             -H:ReflectionConfigurationFiles=$REFLECTION_JSON \
             -H:EnableURLProtocols=http \
             -H:IncludeResources='.*/.*properties$' \
             -H:IncludeResources='.*/.*so$' \
             -H:IncludeResources='.*/.*xml$' \
             -H:Path=$LEIN_TARGET_DIR \
             -Dclojure.compiler.direct-linking=true \
             -jar $LEIN_TARGET_DIR/crux-*-standalone.jar
