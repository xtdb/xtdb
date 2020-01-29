(ns crux.metrics-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.metrics.indexer :as indexer-metrics]
            [crux.metrics.kv-store :as kv-store-metrics]
            [crux.metrics.query :as query-metrics]
            [metrics.core :as metrics]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [metrics.gauges :as gauges])
  (:import (java.io Closeable)))

(t/use-fixtures :each kvf/with-kv-dir fs/with-standalone-node fapi/with-node)

(t/deftest test-indexer-metrics
  (let [{:crux.node/keys [node bus indexer]} (:crux.node/topology (meta *api*))
        registry (metrics/new-registry)
        mets (indexer-metrics/assign-listeners registry #:crux.node{:node node, :bus bus, :indexer indexer})]
    (t/testing "initial ingest values"
      (t/is (nil? (gauges/value (:tx-id-lag mets))))
      (t/is (zero? (meters/count (:docs-ingest-meter mets))))
      (t/is (zero? (timers/number-recorded (:tx-ingest-timer mets)))))

    (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :test}]])
    (.close ^Closeable bus)

    (t/testing "post ingest values"
      (t/is (= 1 (meters/count (:docs-ingest-meter mets))))
      (t/is (zero? (gauges/value (:tx-id-lag mets))))
      (t/is (= 1 (timers/number-recorded (:tx-ingest-timer mets)))))))

(t/deftest test-kv-store-metrics
  (let [{:crux.node/keys [kv-store]} (:crux.node/topology (meta *api*))
        registry (metrics/new-registry)
        mets (kv-store-metrics/assign-listeners registry #:crux.node{:kv-store kv-store})]

    (t/testing "initial kv-store values"
      (t/is (gauges/value (:estimate-num-keys mets)))
      (t/is (gauges/value (:kv-size-mb mets))))))

(t/deftest test-query-metrics
  (let [{:crux.node/keys [bus]} (:crux.node/topology (meta *api*))
        registry (metrics/new-registry)
        mets (query-metrics/assign-listeners registry #:crux.node{:bus bus})]

    (t/testing "inital query timer values"
      (t/is (zero? (timers/number-recorded (:query-timer mets)))))

    (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :test}]])

    (api/q (api/db *api*) {:find ['e] :where [['e :crux.db/id '_]]})

    ;; Commented for now until metrics-clojure release
    ;; https://github.com/metrics-clojure/metrics-clojure/issues/141
    ;; https://github.com/metrics-clojure/metrics-clojure/commit/9650d765c991f648f67b9d7e195edd50330e6f60
    #_(t/testing "post query timer values"
        (t/is (zero? (timers/number-recorded (:query-timer mets)))))))
