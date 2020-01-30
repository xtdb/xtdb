#!/bin/bash
CRUX_VERSION_SUB="s/derived-from-git/${CRUX_VERSION:-$(git describe --tags)}/g"
rm -rf crux-builder/
mkdir -p crux-builder/{clj-uberjar,mvn-uberjar,docker}

cp clj-uberjar/build-uberjar.sh crux-builder/clj-uberjar/
sed clj-uberjar/deps.edn -e $CRUX_VERSION_SUB > crux-builder/clj-uberjar/deps.edn

cp docker/build-docker.sh docker/crux.edn docker/Dockerfile crux-builder/docker/
sed docker/deps.edn -e $CRUX_VERSION_SUB > crux-builder/docker/deps.edn

cp mvn-uberjar/pom.xml crux-builder/mvn-uberjar/
sed mvn-uberjar/build-uberjar.sh -e $CRUX_VERSION_SUB > crux-builder/mvn-uberjar/build-uberjar.sh

tar -czf crux-builder.tar.gz crux-builder/
