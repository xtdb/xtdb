(ns crux.rocksdb-microbench
  (:require [crux.bench.ts-weather :as ts-weather]
            [clojure.test :as t]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.api :refer [*node*] :as fapi]
            [crux.fixtures.kv :as fkv]
            [crux.db :as db]
            [crux.codec :as c]
            [crux.kv :as kv]))

(t/use-fixtures :each
  fs/with-standalone-node
  fkv/with-kv-dir
  fkv/with-rocksdb
  fapi/with-node)

(t/deftest test-weather-ingest
  (t/is :test-weather-ingest)

  (when ts-weather/conditions-csv-resource
    (ts-weather/with-condition-docs
      (fn [[first-doc :as condition-docs]]
        (time
         (doseq [doc-batch (->> (take 10000 condition-docs)
                                (partition-all 100))]
           (db/index-docs (:indexer *node*) (->> doc-batch (into {} (map (juxt c/new-id identity)))))))

        (with-open [snapshot (kv/new-snapshot (:kv-store *node*))]
          (t/is (= first-doc (db/get-single-object (:object-store *node*) snapshot (c/new-id first-doc)))))))))
