(ns crux.bootstrap
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [crux.index :as idx]
            [crux.kafka :as k]
            [crux.kv :as kv]
            [crux.kafka.nippy]
            [crux.lru :as lru]
            [crux.tx :as tx]
            [clojure.string :as str])
  (:import clojure.lang.IPersistentMap
           [java.io Closeable Reader]
           java.net.InetAddress
           java.util.Properties))

(def cli-options
  [["-b" "--bootstrap-servers BOOTSTRAP_SERVERS" "Kafka bootstrap servers"
    :default "localhost:9092"]
   [nil "--kafka-properties-file KAFKA_PROPERTIES_FILE" "Kafka properties file for shared connection properties"]
   ["-g" "--group-id GROUP_ID" "Kafka group.id for this node"
    :default (.getHostName (InetAddress/getLocalHost))]
   ["-t" "--tx-topic TOPIC" "Kafka topic for the Crux transaction log"
    :default "crux-transaction-log"]
   ["-o" "--doc-topic TOPIC" "Kafka topic for the Crux documents"
    :default "crux-docs"]
   ["-c" "--[no-]create-topics" "Should Crux create Kafka topics"
    :default true]
   ["-p" "--doc-partitions PARTITIONS" "Kafka partitions for the Crux documents topic"
    :default 1
    :parse-fn #(Long/parseLong %)]
   ["-r" "--replication-factor FACTOR" "Kafka topic replication factor"
    :default 1
    :parse-fn #(Long/parseLong %)]
   ["-d" "--db-dir DB_DIR" "KV storage directory"
    :default "data"]
   ["-k" "--kv-backend KV_BACKEND" "KV storage backend: crux.kv.rocksdb.RocksKv, crux.kv.lmdb.LMDBKv or crux.kv.memdb.MemKv"
    :default "crux.kv.rocksdb.RocksKv"
    :validate [#'kv/require-and-ensure-kv-record "Unknown storage backend"]]
   ["-s" "--server-port SERVER_PORT" "Port on which to run the HTTP server"
    :default 3000
    :parse-fn #(Long/parseLong %)]

   ["-h" "--help"]])

(s/check-asserts true)

(s/def :crux.http-server/server-port :crux.io/port)

(s/def ::options (s/keys :opt-un [:crux.kafka/bootstrap-servers
                                  :crux.kafka/group-id
                                  :crux.kafka/tx-topic
                                  :crux.kafka/doc-topic
                                  :crux.kafka/doc-partitions
                                  :crux.kaka/create-topics
                                  :crux.kafka/replication-factor
                                  :crux.kv/db-dir
                                  :crux.kv/kv-backend
                                  :crux.http-server/server-port]))

(def default-options (:options (cli/parse-opts [] cli-options)))

(defn- load-properties [^Reader in]
  (->> (doto (Properties.)
         (.load in))
       (into {})))

(defn- parse-pom-version []
  (with-open [in (io/reader (io/resource "META-INF/maven/crux/crux/pom.properties"))]
    (load-properties in)))

(defn- options->table [options]
  (with-out-str
    (pp/print-table (for [[k v] options]
                      {:key k :value v}))))

(defn start-kv-store ^java.io.Closeable [{:keys [db-dir
                                                 kv-backend]
                                          :as options}]
  (-> (kv/new-kv-store options)
      (lru/new-cache-providing-kv-store)
      (kv/open options)))

(defn- read-kafka-properties-file [f]
  (when f
    (with-open [in (io/reader (io/file f))]
      (load-properties in))))

;; Inspired by
;; https://medium.com/@maciekszajna/reloaded-workflow-out-of-the-box-be6b5f38ea98

(defn start-system
  [{:keys [bootstrap-servers
           group-id
           tx-topic
           doc-topic
           server-port
           kafka-properties-file]
    :as options}
   with-system-fn]
  (log/info "starting system")
  (s/assert ::options options)
  (let [kafka-config (merge {"bootstrap.servers" bootstrap-servers}
                            (read-kafka-properties-file kafka-properties-file))
        consumer-config (merge {"group.id" (:group-id options)}
                               kafka-config)]
    (with-open [kv-store (start-kv-store options)
                producer (k/create-producer kafka-config)
                tx-log ^Closeable (k/->KafkaTxLog producer tx-topic doc-topic kafka-config)
                object-store ^Closeable (idx/->KvObjectStore kv-store)
                indexer ^Closeable (tx/->KvIndexer kv-store tx-log object-store)
                admin-client (k/create-admin-client kafka-config)
                indexing-consumer (k/start-indexing-consumer admin-client consumer-config indexer options)]
      (log/info "system started")
      (with-system-fn
        {:kv-store kv-store
         :tx-log tx-log
         :producer producer
         :consumer-config consumer-config
         :object-store object-store
         :indexer indexer
         :admin-client admin-client
         :indexing-consumer indexing-consumer})
      (log/info "stopping system")))
  (log/info "system stopped"))

;; NOTE: This isn't registered until the system manages to start up
;; cleanly, so ctrl-c keeps working as expected in case the system
;; fails to start.
(defn- shutdown-hook-promise []
  (let [main-thread (Thread/currentThread)
        shutdown? (promise)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (let [shutdown-ms 10000]
                                   (deliver shutdown? true)
                                   (.join main-thread shutdown-ms)
                                   (when (.isAlive main-thread)
                                     (log/warn "could not stop system cleanly after" shutdown-ms "ms, forcing exit")
                                     (.halt (Runtime/getRuntime) 1))))
                               "crux.bootstrap.shutdown-hook-thread"))
    shutdown?))

(def env-prefix "CRUX_")

(defn- options-from-env []
  (->> (for [id (keys default-options)
             :let [env-var (str env-prefix (str/replace (str/upper-case (name id)) "-" "_"))
                   v (System/getenv env-var)]
             :when v]
         [(str "--" (name id)) v])
       (apply concat)))

(defn start-system-from-command-line [args]
  (let [{:keys [options
                errors
                summary]} (cli/parse-opts (concat (options-from-env) args) cli-options)
        {:strs [version
                revision]} (parse-pom-version)]
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
           (fn [{:keys [kv-store tx-log indexer consumer-config] :as running-system}]
             (require 'crux.http-server)
             (with-open [http-server ^Closeable ((resolve 'crux.http-server/start-http-server)
                                                 kv-store tx-log indexer consumer-config options)]
               @(shutdown-hook-promise))))))))

(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread throwable]
     (log/error throwable "Uncaught exception:"))))
