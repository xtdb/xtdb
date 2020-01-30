#!/bin/bash
rm -rf crux-builder/
mkdir -p crux-builder
cp build-crux.sh crux-builder/
sed deps.edn -e "s/derived-from-git/${CRUX_VERSION:-$(git describe --tags)}/g" > crux-builder/deps.edn
tar -czf crux-builder.tar.gz crux-builder/
