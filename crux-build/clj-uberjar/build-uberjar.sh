#!/usr/bin/env bash
mkdir -p config
cp crux.edn config/

clojure -Sdeps '{:deps {pack/pack.alpha {:git/url "https://github.com/juxt/pack.alpha.git", :sha "c70740ffc10805f34836da2160fa1899601fac02"}}}' \
        -m mach.pack.alpha.capsule ${UBERJAR_NAME:-crux.jar} \
        -e config \
        --application-id crux \
        --application-version crux-git-version \
        -m crux.main

rm -rf config/
