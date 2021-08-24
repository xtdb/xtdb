(ns crux.metrics-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.metrics.index-store :as index-store-metrics]
            [crux.metrics.query :as query-metrics]
            [crux.metrics.dropwizard :as dropwizard])
  (:import (java.io Closeable)))

(t/use-fixtures :each fix/with-node)

(t/deftest test-index-store-metrics
  (let [{:crux/keys [bus] :as sys} @(:!system *api*)
        registry (dropwizard/new-registry)
        mets (index-store-metrics/assign-listeners registry sys)]
    (t/testing "initial ingest values"
      (t/is (nil? (dropwizard/value (:tx-id-lag mets))))
      (t/is (zero? (dropwizard/meter-count (:docs-ingested-meter mets))))
      (t/is (zero? (dropwizard/meter-count (:tx-ingest-timer mets)))))

    (fix/submit+await-tx [[:crux.tx/put {:xt/id :test}]])
    (.close ^Closeable bus)

    (t/testing "post ingest values"
      (t/is (= 1 (dropwizard/meter-count (:docs-ingested-meter mets))))
      (t/is (zero? (dropwizard/value (:tx-id-lag mets))))
      (t/is (pos? (dropwizard/value (:tx-latency-gauge mets))))
      (t/is (= 1 (dropwizard/meter-count (:tx-ingest-timer mets)))))))

(t/deftest test-query-metrics
  (let [{:crux/keys [bus] :as sys} @(:!system *api*)
        registry (dropwizard/new-registry)
        mets (query-metrics/assign-listeners registry sys)]

    (t/testing "initial query timer values"
      (t/is (zero? (dropwizard/meter-count (:query-timer mets)))))

    (fix/submit+await-tx [[:crux.tx/put {:xt/id :test}]])
    (t/is (= #{[:test]}
             (api/q (api/db *api*) '{:find [e] :where [[e :xt/id _]]})))
    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"Find refers to unknown variable:"
           (api/q (api/db *api*) '{:find [e] :where [[f :xt/id _]]})))
    (.close ^Closeable bus)
    (t/testing "post query timer values"
      (t/is (not (zero? (dropwizard/meter-count (:query-timer mets)))))
      (t/is (zero? (dropwizard/value (:current-query-count mets)))))))
