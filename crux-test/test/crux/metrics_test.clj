(ns crux.metrics-test
  (:require [clojure.test :as t]
            [crux.fixtures.api :as apif :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.metrics.bus :as cx-bus]
            [crux.metrics.gauges :as cx-gauges]
            [crux.api :as api]))

(t/use-fixtures :each kvf/with-kv-dir fs/with-standalone-node apif/with-node)

(t/deftest ingest
  (let [!metrics (cx-bus/assign-ingest (:bus *api*) (:indexer *api*))]
    (t/testing "assign listeners"
      (t/is (= @!metrics
               {:crux.metrics/indexing-tx 0
                :crux.metrics/indexed-tx 0
                :crux.metrics/indexing-docs 0
                :crux.metrics/indexed-docs 0
                :crux.metrics/latest-tx-id []
                :crux.metrics/tx-time-lag 0}))
      (api/await-tx *api* (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :test}]]))
      ;; Might have not run the listener. May fail
      (t/is (= (dissoc @!metrics :crux.metrics/latest-tx-id :crux.metrics/tx-time-lag)
               {:crux.metrics/indexing-tx 0
                :crux.metrics/indexed-tx 1
                :crux.metrics/indexing-docs 0
                :crux.metrics/indexed-docs 1})))
    (t/testing "internal gauges"
      (t/is (= (cx-gauges/ingesting-tx !metrics) 0))
      (t/is (= (cx-gauges/ingested-tx !metrics) 1))
      (t/is (= (cx-gauges/ingesting-docs !metrics) 0))
      (t/is (= (cx-gauges/ingested-docs !metrics) 1)))))

