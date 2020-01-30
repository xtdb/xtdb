#!/bin/bash
CRUX_VERSION_SUB="s/derived-from-git/${CRUX_VERSION:-$(git describe --tags)}/g"
DEPS_EDN="${DEPS_EDN:-deps.edn}"
rm -rf crux-builder/
mkdir -p crux-builder/{clj-uberjar,mvn-uberjar,docker}

sed $DEPS_EDN -e $CRUX_VERSION_SUB > crux-builder/clj-uberjar/deps.edn
sed clj-uberjar/build-uberjar.sh -e $CRUX_VERSION_SUB > crux-builder/clj-uberjar/build-uberjar.sh
chmod +x crux-builder/clj-uberjar/build-uberjar.sh

cp docker/build-docker.sh docker/crux.edn docker/Dockerfile crux-builder/docker/
sed $DEPS_EDN -e $CRUX_VERSION_SUB > crux-builder/docker/deps.edn

cp mvn-uberjar/pom.xml crux-builder/mvn-uberjar/
sed mvn-uberjar/build-uberjar.sh -e $CRUX_VERSION_SUB > crux-builder/mvn-uberjar/build-uberjar.sh

tar -czf crux-builder.tar.gz crux-builder/
