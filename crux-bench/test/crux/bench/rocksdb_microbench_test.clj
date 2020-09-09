(ns crux.bench.rocksdb-microbench-test
  (:require [crux.bench.ts-weather :as ts-weather]
            [clojure.test :as t]
            [crux.api :as api]
            [crux.db :as db]
            [crux.codec :as c]
            [crux.kv :as kv]
            [crux.fixtures :as f]
            [crux.bench :as bench]
            [clojure.java.io :as io]))

(defn with-rocksdb-node* [f]
  (f/with-tmp-dir "dev-storage" [data-dir]
    (with-open [node (api/start-node {:crux/tx-log {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "tx-log")}
                                      :crux/document-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "doc-store")}
                                      :crux/indexer {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "indexes")}})]
      (f node))))

(defmacro with-rocksdb-node [[node-binding] & body]
  `(with-rocksdb-node* (fn [~node-binding] ~@body)))

(t/deftest test-weather-ingest
  (t/is :test-weather-ingest)

  (when ts-weather/conditions-csv-resource
    (with-rocksdb-node [node]
      (bench/with-bench-ns :rocksdb-microbench
        (ts-weather/with-condition-docs
          (fn [[first-doc :as condition-docs]]
            (time
             (doseq [doc-batch (->> (take 10000 condition-docs)
                                    (partition-all 100))]
               (db/index-docs (:indexer node) (->> doc-batch (into {} (map (juxt c/new-id identity)))))))))))))
