(ns crux.bench.compaction-microbench
  (:require [clojure.java.io :as io]
            [crux.api :as crux]
            [crux.bench :as bench]
            [crux.compaction :as cc])
  (:import java.util.Calendar))

;; Compaction should slow this down, keeping only two years worth - can validate with a query

(defn submit-batches [node]
  (for [doc-batch (->> (for [n (range 1000)
                             vt (->> (range 20)
                                     (map #(.getTime
                                            (doto (Calendar/getInstance)
                                              (.set Calendar/YEAR 2000)
                                              (.add Calendar/YEAR %)))))]
                         [:crux.tx/put {:crux.db/id (keyword (str "doc-" n))
                                        :name (str "dr-" n)}
                          vt])
                       (partition-all 1000))]
    (crux/submit-tx node (vec doc-batch))))

(defn run-benches [node [submit-bench-type await-bench-type]]
  (bench/run-bench await-bench-type
    (let [submitted-tx (-> (bench/run-bench submit-bench-type
                             (-> {:success? true}
                                 (with-meta {:submitted-tx (last (submit-batches node))})))
                           meta
                           :submitted-tx)]

      (crux/await-tx node submitted-tx (java.time.Duration/ofSeconds 20))
      {:success? true})))

(comment
  (require '[crux.kafka.embedded :as ek]
           '[crux.fixtures :as f])

  (f/with-tmp-dir "crux" [tmp-dir]
    (with-open [ek (ek/start-embedded-kafka
                    #:crux.kafka.embedded{:zookeeper-data-dir (str (io/file tmp-dir "zk-data"))
                                          :kafka-dir (str (io/file tmp-dir "kafka-data"))
                                          :kafka-log-dir (str (io/file tmp-dir "kafka-log"))})
                node (crux/start-node {:crux.node/topology ['crux.kafka/topology cc/module]
                                       :crux.node/kv-store 'crux.kv.rocksdb/kv
                                       :crux.kv/db-dir (str (io/file tmp-dir "rocks"))})]
      (bench/with-bench-ns :compaction
        (run-benches node [:initial-submits :initial-await])))))
