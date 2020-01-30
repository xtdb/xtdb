#!/usr/bin/env bash
clojure -Sdeps '{:deps {pack/pack.alpha {:git/url "https://github.com/juxt/pack.alpha.git", :sha "c70740ffc10805f34836da2160fa1899601fac02"}}}' \
        -m mach.pack.alpha.capsule crux.jar \
        --application-id crux \
        --application-version derived-from-git \
        -m crux.main
