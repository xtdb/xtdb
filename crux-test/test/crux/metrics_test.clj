(ns crux.metrics-test
  (:require [clojure.test :as t]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.metrics.indexer :as indexer-metrics]
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
