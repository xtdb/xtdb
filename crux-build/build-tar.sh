#!/bin/bash
rm -rf crux-builder/
mkdir -p crux-builder/{clj-uberjar,mvn-uberjar,docker}

cp clj-uberjar/build-uberjar.sh crux-builder/clj-uberjar/
sed clj-uberjar/deps.edn -e "s/derived-from-git/${CRUX_VERSION:-$(git describe --tags)}/g" > crux-builder/clj-uberjar/deps.edn

tar -czf crux-builder.tar.gz crux-builder/
