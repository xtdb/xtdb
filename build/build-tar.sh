#!/bin/bash
XTDB_VERSION_SUB="s/xtdb-git-version/${XTDB_VERSION:-$(git describe --tags)}/g"
DEPS_EDN="${DEPS_EDN:-deps.edn}"
XTDB_EDN="${XTDB_EDN:-xtdb.edn}"
LOGBACK_XML="${LOGBACK_XML:-logback.xml}"
rm -rf xtdb-builder/
mkdir -p xtdb-builder/{clj-uberjar,mvn-uberjar,docker}

mkdir -p xtdb-builder/clj-uberjar/resources
cp $LOGBACK_XML xtdb-builder/clj-uberjar/resources/logback.xml
cp clj-uberjar/$XTDB_EDN xtdb-builder/clj-uberjar/xtdb.edn
sed $DEPS_EDN -e $XTDB_VERSION_SUB > xtdb-builder/clj-uberjar/deps.edn
sed clj-uberjar/build-uberjar.sh -e $XTDB_VERSION_SUB > xtdb-builder/clj-uberjar/build-uberjar.sh
chmod +x xtdb-builder/clj-uberjar/build-uberjar.sh

mkdir -p xtdb-builder/docker/resources
cp $LOGBACK_XML xtdb-builder/docker/resources/logback.xml
cp docker/$XTDB_EDN xtdb-builder/docker/xtdb.edn
cp docker/build-docker.sh docker/Dockerfile xtdb-builder/docker/
sed $DEPS_EDN -e $XTDB_VERSION_SUB > xtdb-builder/docker/deps.edn

sed mvn-uberjar/pom.xml -e $XTDB_VERSION_SUB > xtdb-builder/mvn-uberjar/pom.xml
mkdir -p xtdb-builder/mvn-uberjar/src/main/resources/
cp $LOGBACK_XML xtdb-builder/mvn-uberjar/src/main/resources/logback.xml
cp mvn-uberjar/$XTDB_EDN xtdb-builder/mvn-uberjar/src/main/resources/xtdb.edn
sed mvn-uberjar/build-uberjar.sh -e $XTDB_VERSION_SUB > xtdb-builder/mvn-uberjar/build-uberjar.sh
chmod +x xtdb-builder/mvn-uberjar/build-uberjar.sh

tar -czf xtdb-builder.tar.gz xtdb-builder/
