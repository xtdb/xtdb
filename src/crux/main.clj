(ns crux.main
  (:require [clojure.tools.logging :as log]
            [clojure.tools.cli :as cli]
            [clojure.pprint :as pp]
            [crux.core]
            [crux.memdb]
            [crux.rocksdb]
            [crux.lmdb]
            [crux.kafka :as kafka])
  (:import [java.io Closeable]
           [java.net InetAddress]))

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
   ["-k" "--kv-backend KV_BACKEND" "KV storage backend"
    :default "rocksdb"
    :validate [#{"rocksdb" "lmdb" "memdb"} "Unknown storage back-end"]]
   ["-h" "--help"]])

(defn -main [& args]
  (let [{:keys [options
                errors
                summary]} (cli/parse-opts args cli-options)]
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
                        "memdb" crux.memdb/map->CruxMemKv) {})]
        (log/warn "Starting Crux...")
        (log/warn "Options:" (with-out-str
                               (pp/print-table (for [[k v] options]
                                                 {:key k :value v}))))
        (with-open [^Closeable kv (crux.core/kv db-dir {:kv-store kv-store})]
          (kafka/start-indexing kv topic {"bootstrap.servers" bootstrap-servers
                                          "group.id" group-id}))))))
