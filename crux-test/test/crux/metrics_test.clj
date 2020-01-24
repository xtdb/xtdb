(ns crux.metrics-test
  (:require [clojure.test :as t]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.metrics.ingest :as ingest-metrics]
            [crux.api :as api]
            [metrics.core :as metrics]
            [metrics.timers :as timers]
            [metrics.gauges :as gauges]))

;; Hack to fix v while we wait for https://github.com/metrics-clojure/metrics-clojure/issues/141 to happen
;; https://github.com/metrics-clojure/metrics-clojure/commit/9650d765c991f648f67b9d7e195edd50330e6f60
(def ^:private number-recorded timers/number-recorded)

(t/use-fixtures :each kvf/with-kv-dir fs/with-standalone-node apif/with-node)

(t/deftest ingest
  (let [registry (metrics/new-registry)
        ;; Assign metrics
        mets (ingest-metrics/assign-ingest registry #:crux.node{:bus (:bus *api*)
                                                                :indexer (:indexer *api*)
                                                                :tx-log (:tx-log *api*)})]
    (t/testing "initial ingest values"
      (t/is (= 0 (gauges/value (:ingesting-docs mets))))
      (t/is (= 0 (gauges/value (:ingesting-tx mets))))
      (t/is (nil? (gauges/value (:tx-id-lag mets))))
      (t/is (= 0 (number-recorded (:docs-ingest-timer mets))))
      (t/is (= 0 (number-recorded (:tx-ingest-timer mets)))))

    (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :test}]])

    (t/testing "post ingest values"
      (t/is (= 1 (+ (gauges/value (:ingesting-docs mets))
                    (number-recorded (:docs-ingest-timer mets)))))
      (t/is (= 1 (+ (gauges/value (:ingesting-tx mets))
                    (number-recorded (:tx-ingest-timer mets)))))
      (t/is (zero? (gauges/value (:tx-id-lag mets)))))))
