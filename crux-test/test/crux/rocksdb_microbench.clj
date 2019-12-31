(ns crux.rocksdb-microbench
  (:require [crux.ts-weather-test :as ts-weather]
            [clojure.test :as t]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.api :refer [*api*] :as fapi]
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

  (when ts-weather/run-ts-weather-tests?
    (ts-weather/with-condition-docs
      (fn [[first-doc :as condition-docs]]
        (time
         (doseq [doc-batch (->> (take 10000 condition-docs)
                                (partition-all 100))]
           (db/index-docs (:indexer *api*) (->> doc-batch (into {} (map (juxt c/new-id identity)))))))

        (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
          (t/is (= first-doc (db/get-single-object (:object-store *api*) snapshot (c/new-id first-doc)))))))))
