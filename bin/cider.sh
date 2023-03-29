#!/usr/bin/env bash

set -e

clojure -M:xtdb:lib/kaocha:repl/cider-refactor $@
