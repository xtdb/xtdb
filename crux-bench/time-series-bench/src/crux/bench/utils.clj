(ns crux.bench.utils
  (:require [clojure.data.json :as json]
            [crux.kafka.embedded :as ek]
            [crux.io :as cio]
            [crux.api :as api]))

(defn output [mp]
  (println (json/write-str mp)))

(defn bench [ingest queries version]
  (try
    (with-open [embedded-kafka (ek/start-embedded-kafka
                                 {:crux.kafka.embedded/zookeeper-data-dir "dev-storage/zookeeper"
                                  :crux.kafka.embedded/kafka-log-dir "dev-storage/kafka-log"
                                  :crux.kafka.embedded/kafka-port 9092})
                node (api/start-node {:crux.node/topology :crux.kafka/topology
                                      :crux.node/kv-store "crux.kv.rocksdb/kv"
                                      :crux.kafka/bootstrap-servers "localhost:9092"
                                      :crux.kv/db-dir "dev-storage/db-dir-1"
                                      :crux.standalone/event-log-dir "dev-storage/eventlog-1"})]
      (try
        (let [start-time (System/currentTimeMillis)]
          (api/await-tx node
                        (:last-tx (ingest node))
                        (java.time.Duration/ofMinutes 20))
          (output
            {:bench-type :ingest
             :bench-version version
             :ingest-time-ms (- (System/currentTimeMillis) start-time)
             :crux-node-status (select-keys (api/status node)
                                            [:crux.kv/estimate-num-keys
                                             :crux.kv/size])}))
        (catch Exception e
          (output
            {:bench-type :ingest
             :bench-version version
             :error e})
          (throw e)))
      (queries node))
    (catch Exception e)
    (finally (cio/delete-dir "dev-storage"))))
