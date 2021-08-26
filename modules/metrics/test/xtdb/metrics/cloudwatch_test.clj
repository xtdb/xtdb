(ns xtdb.metrics.cloudwatch-test
  (:require [xtdb.metrics.cloudwatch :as sut]
            [clojure.test :as t]))

(t/deftest test-include-metric
  (t/are [include? metric-name ignore-rules] (= include? (#'sut/include-metric? metric-name ignore-rules))
    true "crux.tx.ingest-rate" nil

    true "crux.tx.ingest-rate" ["*" "!crux.tx"]
    false "crux.kv.query-rate" ["*" "!crux.tx"]

    true "crux.kv.foo" ["crux.tx"]
    false "crux.tx.foo" ["crux.tx"]

    false "crux.tx.foo" ["crux.tx" "!crux.tx.ingest"]
    true "crux.tx.ingest" ["crux.tx" "!crux.tx.ingest"]))
