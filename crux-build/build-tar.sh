#!/bin/bash
CRUX_VERSION_SUB="s/crux-git-version/${CRUX_VERSION:-$(git describe --tags)}/g"
DEPS_EDN="${DEPS_EDN:-deps.edn}"
CRUX_EDN="${CRUX_EDN:-crux.edn}"
LOGBACK_XML="${LOGBACK_XML:-logback.xml}"
rm -rf crux-builder/
mkdir -p crux-builder/{clj-uberjar,mvn-uberjar,docker}

mkdir -p crux-builder/clj-uberjar/resources
cp $LOGBACK_XML crux-builder/clj-uberjar/resources/logback.xml
cp clj-uberjar/$CRUX_EDN crux-builder/clj-uberjar/crux.edn
sed $DEPS_EDN -e $CRUX_VERSION_SUB > crux-builder/clj-uberjar/deps.edn
sed clj-uberjar/build-uberjar.sh -e $CRUX_VERSION_SUB > crux-builder/clj-uberjar/build-uberjar.sh
chmod +x crux-builder/clj-uberjar/build-uberjar.sh

mkdir -p crux-builder/docker/resources
cp $LOGBACK_XML crux-builder/docker/resources/logback.xml
cp docker/$CRUX_EDN crux-builder/docker/crux.edn
cp docker/build-docker.sh docker/Dockerfile crux-builder/docker/
sed $DEPS_EDN -e $CRUX_VERSION_SUB > crux-builder/docker/deps.edn

sed mvn-uberjar/pom.xml -e $CRUX_VERSION_SUB > crux-builder/mvn-uberjar/pom.xml
mkdir -p crux-builder/mvn-uberjar/src/main/resources/
cp $LOGBACK_XML crux-builder/mvn-uberjar/src/main/resources/logback.xml
cp mvn-uberjar/$CRUX_EDN crux-builder/mvn-uberjar/src/main/resources/crux.edn
sed mvn-uberjar/build-uberjar.sh -e $CRUX_VERSION_SUB > crux-builder/mvn-uberjar/build-uberjar.sh
chmod +x crux-builder/mvn-uberjar/build-uberjar.sh

tar -czf crux-builder.tar.gz crux-builder/
