(ns crux.metrics-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.metrics.indexer :as indexer-metrics]
            [crux.metrics.query :as query-metrics]
            [crux.metrics.dropwizard :as dropwizard])
  (:import (java.io Closeable)))

(t/use-fixtures :each kvf/with-kv-dir fs/with-standalone-node fapi/with-node)

(t/deftest test-indexer-metrics
  (let [{:crux.node/keys [node bus indexer]} (:crux.node/topology (meta *api*))
        registry (dropwizard/new-registry)
        mets (indexer-metrics/assign-listeners registry #:crux.node{:node node, :bus bus, :indexer indexer})]
    (t/testing "initial ingest values"
      (t/is (nil? (dropwizard/value (:tx-id-lag mets))))
      (t/is (zero? (dropwizard/meter-count (:docs-ingested-meter mets))))
      (t/is (zero? (dropwizard/meter-count (:tx-ingested-timer mets)))))

    (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :test}]])
    (.close ^Closeable bus)

    (t/testing "post ingest values"
      (t/is (= 1 (dropwizard/meter-count (:docs-ingest-meter mets))))
      (t/is (zero? (dropwizard/value (:tx-id-lag mets))))
      (t/is (= 1 (dropwizard/meter-count (:tx-ingest-timer mets)))))))

(t/deftest test-query-metrics
  (let [{:crux.node/keys [bus]} (:crux.node/topology (meta *api*))
        registry (dropwizard/new-registry)
        mets (query-metrics/assign-listeners registry #:crux.node{:bus bus})]

    (t/testing "initial query timer values"
      (t/is (zero? (dropwizard/meter-count (:query-timer mets)))))

    (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :test}]])

    (api/q (api/db *api*) {:find ['e] :where [['e :crux.db/id '_]]})

    (.close ^Closeable bus)

    (t/testing "post query timer values"
      (t/is (not (zero? (dropwizard/meter-count (:query-timer mets))))))))
