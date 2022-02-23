#!/usr/bin/env bash
mkdir -p config
cp xtdb.edn config/

clojure -T:uberjar :uber-file '"'${UBERJAR_NAME:-xtdb.jar}'"'

rm -rf config/
