#!/usr/bin/env bash

set -e

clojure -M:lib/kaocha:repl/cider-refactor $@
