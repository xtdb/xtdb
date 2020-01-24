(ns crux.metrics-test
  (:require [clojure.test :as t]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.metrics.ingest :as ingest-metrics]
            [crux.api :as api]
            [metrics.core :as metrics]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [metrics.gauges :as gauges])
  (:import (java.io Closeable)))

(t/use-fixtures :each kvf/with-kv-dir fs/with-standalone-node fapi/with-node)

(t/deftest test-ingest-metrics
  (let [registry (metrics/new-registry)
        mets (ingest-metrics/assign-ingest registry #:crux.node{:bus (:bus *api*)
                                                                :indexer (:indexer *api*)
                                                                :tx-log (:tx-log *api*)})]
    (t/testing "initial ingest values"
      (t/is (nil? (gauges/value (:tx-id-lag mets))))
      (t/is (zero? (meters/count (:docs-ingest-meter mets))))
      (t/is (zero? (timers/number-recorded (:tx-ingest-timer mets)))))

    (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :test}]])

    (.close ^Closeable (:bus *api*))

    (t/testing "post ingest values"
      (t/is (= 1 (meters/count (:docs-ingest-meter mets))))
      (t/is (zero? (gauges/value (:tx-id-lag mets))))
      (t/is (= 1 (timers/number-recorded (:tx-ingest-timer mets)))))))
