(ns crux.main
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.tools.logging :as log]
            [clojure.tools.cli :as cli]
            [crux.core]
            [crux.kv-store]
            [crux.memdb]
            [crux.rocksdb]
            [crux.lmdb]
            [crux.kafka :as kafka])
  (:import [java.io Closeable]
           [java.net InetAddress]
           [java.util Properties])
  (:gen-class))

(def cli-options
  ;; An option with a required argument
  [["-b" "--bootstrap-servers BOOTSTRAP_SERVERS" "Kafka bootstrap servers"
    :default "localhost:9092"]
   ["-g" "--group-id GROUP_ID" "Kafka group.id for this node"
    :default (.getHostName (InetAddress/getLocalHost))]
   ["-t" "--topic TOPIC" "Kafka topic for the Crux transaction log"
    :default "crux-transaction-log"]
   ["-d" "--db-dir DB_DIR" "KV storage directory"
    :default "data"]
   ["-k" "--kv-backend KV_BACKEND" "KV storage backend: rocksdb, lmdb or memdb"
    :default "rocksdb"
    :validate [#{"rocksdb" "lmdb" "memdb"} "Unknown storage backend"]]
   ["-h" "--help"]])

(defn version []
  (with-open [in (io/reader (io/resource "META-INF/maven/crux/crux/pom.properties"))]
    (->> (doto (Properties.)
           (.load in))
         (into {}))))

(defn -main [& args]
  (let [{:keys [options
                errors
                summary]} (cli/parse-opts args cli-options)
        {:strs [version
                revision]} (version)]
    (cond
      (:help options)
      (println summary)

      errors
      (binding [*out* *err*]
        (doseq [error errors]
          (println error))
        (System/exit 1))

      :else
      (let [{:keys [bootstrap-servers
                    group-id
                    topic
                    db-dir
                    kv-backend]} options
            kv-store ((case kv-backend
                        "rocksdb" crux.rocksdb/map->CruxRocksKv
                        "lmdb" crux.lmdb/map->CruxLMDBKv
                        "memdb" crux.memdb/map->CruxMemKv) {})
            options-table (with-out-str
                            (pp/print-table (for [[k v] options]
                                              {:key k :value v})))]
        (log/warn "Starting Crux...")
        (log/warnf "version: %s revision: %s" version revision)
        (log/warn "options:" options-table)

        (with-open [^Closeable kv (->> (crux.core/kv db-dir {:kv-store kv-store})
                                       (crux.kv-store/open))
                    consumer (kafka/create-consumer {"bootstrap.servers" bootstrap-servers
                                                     "group.id" group-id})]
          (kafka/start-indexing kv consumer topic))))))
