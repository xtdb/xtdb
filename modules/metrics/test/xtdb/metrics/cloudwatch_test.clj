(ns xtdb.metrics.cloudwatch-test
  (:require [xtdb.metrics.cloudwatch :as sut]
            [clojure.test :as t]))

(t/deftest test-include-metric
  (t/are [include? metric-name ignore-rules] (= include? (#'sut/include-metric? metric-name ignore-rules))
    true "xtdb.tx.ingest-rate" nil

    true "xtdb.tx.ingest-rate" ["*" "!xtdb.tx"]
    false "xtdb.kv.query-rate" ["*" "!xtdb.tx"]

    true "xtdb.kv.foo" ["xtdb.tx"]
    false "xtdb.tx.foo" ["xtdb.tx"]

    false "xtdb.tx.foo" ["xtdb.tx" "!xtdb.tx.ingest"]
    true "xtdb.tx.ingest" ["xtdb.tx" "!xtdb.tx.ingest"]))
