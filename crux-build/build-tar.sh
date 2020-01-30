#!/bin/bash
rm -rf crux-builder/
mkdir -p crux-builder
cp deps.edn build-crux.sh crux-builder/
tar -czf crux-builder.tar.gz crux-builder/
