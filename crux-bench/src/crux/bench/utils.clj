(ns crux.bench.utils
  (:require [clojure.data.json :as json]
            [crux.kafka.embedded :as ek]
            [crux.io :as cio]
            [clojure.java.io :as io]
            [crux.api :as api]
            [clojure.string :as string]
            [clojure.java.shell :as shell]))

(def commit-hash
  (string/trim (:out (shell/sh "git" "rev-parse" "HEAD"))))

(def crux-version
  (when-let [pom-file (io/resource "META-INF/maven/juxt/crux-core/pom.properties")]
    (with-open [in (io/reader pom-file)]
      (get (cio/load-properties in) "version"))))

(defn output [mp]
  (println (json/write-str (merge mp
                                  {:crux-commit commit-hash
                                   :crux-version crux-version}))))

(defn bench [ingest queries bench-ns]
  (try
    (with-open [embedded-kafka (ek/start-embedded-kafka
                                 {:crux.kafka.embedded/zookeeper-data-dir "dev-storage/zookeeper"
                                  :crux.kafka.embedded/kafka-log-dir "dev-storage/kafka-log"
                                  :crux.kafka.embedded/kafka-port 9092})
                node (api/start-node {:crux.node/topology 'crux.kafka/topology
                                      :crux.node/kv-store 'crux.kv.rocksdb/kv
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
             :bench-ns bench-ns
             :ingest-time-ms (- (System/currentTimeMillis) start-time)
             :crux-node-status (select-keys (api/status node)
                                            [:crux.kv/estimate-num-keys
                                             :crux.kv/size])}))
        (catch Exception e
          (output
            {:bench-type :ingest
             :bench-ns bench-ns
             :error e})
          (throw e)))
      (queries node))
    (catch Exception e)
    (finally (cio/delete-dir "dev-storage"))))
