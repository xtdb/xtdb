#!/usr/bin/env bash
mkdir -p config
cp xtdb.edn config/

clojure -Sdeps '{:aliases {:depstar {:replace-deps {seancorfield/depstar {:mvn/version "2.0.171"}}}}}' \
        -X:depstar \
        hf.depstar/uberjar \
        :jar ${UBERJAR_NAME:-xtdb.jar} \
        :main-class xtdb.main \
        :aot true

rm -rf config/
