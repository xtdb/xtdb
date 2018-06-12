(ns crux.bootstrap
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.tools.logging :as log]
            [clojure.tools.cli :as cli]
            [crux.doc :as doc]
            [crux.tx :as tx]
            [crux.http-server :as srv]
            [crux.kv-store :as kv-store]
            [crux.memdb]
            [crux.rocksdb]
            [crux.lmdb]
            [crux.kafka :as k])
  (:import [java.io Closeable]
           [java.net InetAddress]
           [java.util Properties])
  (:gen-class))

(def cli-options
  [["-b" "--bootstrap-servers BOOTSTRAP_SERVERS" "Kafka bootstrap servers"
    :default "localhost:9092"]
   ["-g" "--group-id GROUP_ID" "Kafka group.id for this node"
    :default (.getHostName (InetAddress/getLocalHost))]
   ["-t" "--tx-topic TOPIC" "Kafka topic for the Crux transaction log"
    :default "crux-transaction-log"]
   ["-o" "--doc-topic TOPIC" "Kafka topic for the Crux documents"
    :default "crux-docs"]
   ["-p" "--doc-partitions PARTITIONS" "Kafka partitions for the Crux documents topic"
    :default "1"]
   ["-r" "--replication-factor FACTOR" "Kafka topic replication factor"
    :default "1"]
   ["-d" "--db-dir DB_DIR" "KV storage directory"
    :default "data"]
   ["-k" "--kv-backend KV_BACKEND" "KV storage backend: rocksdb, lmdb or memdb"
    :default "rocksdb"
    :validate [#{"rocksdb" "lmdb" "memdb"} "Unknown storage backend"]]
   ["-s" "--server-port SERVER_PORT" "port on which to run the HTTP server"
    :default "3000"]
   ["-h" "--help"]])

(def default-options (:options (cli/parse-opts [] cli-options)))

(defn parse-version []
  (with-open [in (io/reader (io/resource "META-INF/maven/crux/crux/pom.properties"))]
    (->> (doto (Properties.)
           (.load in))
         (into {}))))

(defn options->table [options]
  (with-out-str
    (pp/print-table (for [[k v] options]
                      {:key k :value v}))))

(defn ^Closeable start-kv-store [{:keys [db-dir
                                         kv-backend]
                                  :as options}]
  (let [kv-store ((case kv-backend
                    "rocksdb" crux.rocksdb/map->RocksKv
                    "lmdb" crux.lmdb/map->LMDBKv
                    "memdb" crux.memdb/map->MemKv) {})]
    (->> (assoc kv-store
                :db-dir db-dir
                :state (atom {}))
         (kv-store/open))))

(defn start-system
  ([{:keys [bootstrap-servers
            group-id]
     :as options}]
   (with-open [kv-store (start-kv-store options)
               consumer (k/create-consumer {"bootstrap.servers" bootstrap-servers
                                            "group.id" group-id})
               producer (k/create-producer {"bootstrap.servers" bootstrap-servers})
               admin-client (k/create-admin-client {"bootstrap.servers" bootstrap-servers})]
     (start-system kv-store consumer producer admin-client (delay true) options)))
  ([kv-store consumer producer admin-client running? options]
   (let [{:keys [bootstrap-servers
                 group-id
                 tx-topic
                 doc-topic
                 doc-partitions
                 replication-factor
                 db-dir
                 server-port]
          :as options} (merge default-options options)
         tx-log (k/->KafkaTxLog producer tx-topic doc-topic)
         object-store (doc/->DocObjectStore kv-store)
         indexer (tx/->DocIndexer kv-store tx-log object-store)
         replication-factor (Long/parseLong replication-factor)
         server-port (Long/parseLong server-port)]
     (k/create-topic admin-client tx-topic 1 replication-factor k/tx-topic-config)
     (k/create-topic admin-client doc-topic (Long/parseLong doc-partitions) replication-factor k/doc-topic-config)
     (k/subscribe-from-stored-offsets indexer consumer [tx-topic doc-topic])
     (with-open [http-server (srv/create-server kv-store tx-log db-dir server-port)]
       (while @running?
         (k/consume-and-index-entities indexer consumer 100))))))

(defn start-system-from-command-line [args]
  (let [{:keys [options
                errors
                summary]} (cli/parse-opts args cli-options)
        options (merge default-options options)
        {:strs [version
                revision]} (parse-version)]
    (cond
      (:help options)
      (println summary)

      errors
      (binding [*out* *err*]
        (doseq [error errors]
          (println error))
        (System/exit 1))

      :else
      (do (log/infof "Crux version: %s revision: %s" version revision)
          (log/info "options:" (options->table options))
          (start-system options)))))

(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread throwable]
     (log/error throwable "Uncaught exception:"))))
