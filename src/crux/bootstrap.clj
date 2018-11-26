(ns crux.bootstrap
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [crux.doc :as doc]
            [crux.http-server :as srv]
            [crux.kafka :as k]
            [crux.kv-store :as kv-store]
            [crux.kafka.nippy]
            crux.lmdb
            crux.memdb
            crux.rocksdb
            [crux.tx :as tx]
            [clojure.string :as str])
  (:import java.io.Closeable
           java.net.InetAddress
           java.util.Properties))

(def system)

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
    :default 1
    :parse-fn #(Long/parseLong %)]
   ["-r" "--replication-factor FACTOR" "Kafka topic replication factor"
    :default 1
    :parse-fn #(Long/parseLong %)]
   ["-d" "--db-dir DB_DIR" "KV storage directory"
    :default "data"]
   ["-k" "--kv-backend KV_BACKEND" "KV storage backend: rocksdb, lmdb or memdb"
    :default "rocksdb"
    :validate [#{"rocksdb" "lmdb" "memdb"} "Unknown storage backend"]]
   ["-s" "--server-port SERVER_PORT" "port on which to run the HTTP server"
    :default 3000
    :parse-fn #(Long/parseLong %)]

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
  [options with-system-fn]
  (let [{:keys [bootstrap-servers
                group-id
                tx-topic
                doc-topic
                db-dir
                server-port]
         :as options} (merge default-options options)]
    (log/info "starting system")
    (with-open [kv-store (start-kv-store options)
                producer (k/create-producer {"bootstrap.servers" bootstrap-servers})
                tx-log ^Closeable (k/->KafkaTxLog producer tx-topic doc-topic)
                object-store ^Closeable (doc/->DocObjectStore kv-store)
                indexer ^Closeable (tx/->DocIndexer kv-store tx-log object-store)
                admin-client (k/create-admin-client {"bootstrap.servers" bootstrap-servers})
                http-server (srv/create-server
                             kv-store
                             tx-log
                             db-dir
                             bootstrap-servers
                             server-port)
                indexing-consumer (k/create-indexing-consumer
                                   admin-client indexer options)]
      (with-system-fn
        {:kv-store kv-store
         :tx-log tx-log
         :producer producer
         :indexer indexer
         :admin-client admin-client
         :http-server http-server
         :indexing-consumer indexing-consumer})
      (log/info "stopping system"))))

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
          (start-system
           options
           (fn [running-system]
             (alter-var-root #'system (constantly running-system))
             @(:indexing-consumer running-system)))))))

(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread throwable]
     (log/error throwable "Uncaught exception:"))))
