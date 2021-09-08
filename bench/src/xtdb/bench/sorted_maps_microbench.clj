(ns xtdb.bench.sorted-maps-microbench
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [xtdb.bench :as bench]
            [xtdb.kafka.embedded :as ek]
            [xtdb.fixtures :as fix]
            [xtdb.kafka :as k]
            [xtdb.rocksdb :as rocks]))

(defn submit-batches [node]
  (for [doc-batch (->> (for [n (range 25000)]
                         [::xt/put {:xt/id (keyword (str "doc-" n))
                                   :nested-map {:foo :bar
                                                :baz :quux
                                                :doc-idx n}}])
                       (partition-all 1000))]
    (xt/submit-tx node (vec doc-batch))))

(defn run-benches [node [submit-bench-type await-bench-type]]
  (bench/run-bench await-bench-type
    (let [submitted-tx (-> (bench/run-bench submit-bench-type
                             (-> {:success? true}
                                 (with-meta {:submitted-tx (last (submit-batches node))})))
                           meta
                           :submitted-tx)]

      (xt/await-tx node submitted-tx (java.time.Duration/ofSeconds 20))
      {:success? true})))

(defn run-sorted-maps-microbench [node]
  (bench/with-bench-ns :sorted-maps
    (run-benches node [:initial-submits :initial-await])
    (run-benches node [:subsequent-submits :subsequent-await])))

(comment
  (fix/with-tmp-dir "xtdb" [tmp-dir]
    (with-open [ek (ek/start-embedded-kafka #::ek{:zookeeper-data-dir (io/file tmp-dir "zk-data")
                                                  :kafka-dir (io/file tmp-dir "kafka-data")
                                                  :kafka-log-dir (io/file tmp-dir "kafka-log")})
                node (xt/start-node {::k/kafka-config {:bootstrap-servers "localhost:9092"}
                                       :xtdb/tx-log {:xtdb/module `k/->tx-log, :kafka-config ::k/kafka-config}
                                       :xtdb/document-store {:xtdb/module `k/->document-store
                                                           :kafka-config ::k/kafka-config
                                                           :local-document-store {:kv-store {:xtdb/module `rocks/->kv-store,
                                                                                             :db-dir (io/file tmp-dir "doc-store")}}}
                                       :xtdb/index-store {:kv-store {:xtdb/module `rocks/->kv-store, :db-dir (io/file tmp-dir "index-store")}}})]
      (bench/with-bench-ns :sorted-maps
        (run-benches node [:initial-submits :initial-await])))))
