#!/usr/bin/env bash

set -e

clojure -M:core2:lib/kaocha:repl/cider-refactor $@
