(ns crux.bootstrap
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [crux.doc :as doc]
            [crux.http-server :as srv]
            [crux.kafka :as k]
            [crux.kv-store :as ks]
            [crux.kafka.nippy]
            [crux.lru :as lru]
            crux.lmdb
            crux.memdb
            crux.rocksdb
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
   ["-k" "--kv-backend KV_BACKEND" "KV storage backend:
   crux.rocksdb.RocksKv, crux.lmdb.LMDBKv or crux.memdb.MemKv"
    :default crux.rocksdb.RocksKv
    :parse-fn (comp deref resolve symbol)
    :validate [#(extends? ks/KvStore %) "Unknown storage backend"]]
   ["-s" "--server-port SERVER_PORT" "Port on which to run the HTTP server"
    :default 3000
    :parse-fn #(Long/parseLong %)]

   ["-h" "--help"]])

(s/def ::bootstrap-servers (s/and string? #(re-matches #"\w+:\d+(,\w+:\d+)*" %)))
(s/def ::group-id string?)
(s/def ::tx-topic string?)
(s/def ::doc-topic string?)
(s/def ::doc-partitions pos-int?)
(s/def ::create-topics boolean?)
(s/def ::replication-factor pos-int?)
(s/def ::db-dir string?)
(s/def ::kv-backend #(extends? ks/KvStore %))
(s/def ::server-port (s/int-in 1 65536))

(s/def ::options (s/keys :opt-un [::bootstrap-servers
                                  ::group-id
                                  ::tx-topic
                                  ::doc-topic
                                  ::doc-partitions
                                  ::create-topics
                                  ::replication-factor
                                  ::db-dir
                                  ::kv-backend
                                  ::server-port]))

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

(defn ^Closeable start-kv-store [{:keys [db-dir
                                         kv-backend]
                                  :as options}]
  (->> (lru/new-cache-providing-kv-store
        (eval (list (symbol (.getName crux.rocksdb.RocksKv) "create")
                    {:db-dir (str db-dir)})))
       (ks/open)))

(defn- read-kafka-properties-file [f]
  (when f
    (with-open [in (io/reader (io/file f))]
      (load-properties in))))

;; Inspired by
;; https://medium.com/@maciekszajna/reloaded-workflow-out-of-the-box-be6b5f38ea98

(defn start-system
  [options with-system-fn]
  (let [{:keys [bootstrap-servers
                group-id
                tx-topic
                doc-topic
                server-port
                kafka-properties-file]
         :as options} (merge default-options options)]
    (log/info "starting system")
    (when (s/invalid? (s/conform :crux.bootstrap/options options))
      (throw (IllegalArgumentException.
              (str "Invalid options: " (s/explain-str :crux.bootstrap/options options)))))
    (let [kafka-properties (merge {"bootstrap.servers" bootstrap-servers}
                                  (read-kafka-properties-file kafka-properties-file))]
      (with-open [kv-store (start-kv-store options)
                  producer (k/create-producer kafka-properties)
                  consumer (k/create-consumer (merge {"group.id" (:group-id options)}
                                                     kafka-properties))
                  tx-log ^Closeable (k/->KafkaTxLog producer tx-topic doc-topic)
                  object-store ^Closeable (doc/->DocObjectStore kv-store)
                  indexer ^Closeable (tx/->DocIndexer kv-store tx-log object-store)
                  admin-client (k/create-admin-client kafka-properties)
                  http-server (srv/create-server
                               kv-store
                               tx-log
                               bootstrap-servers
                               server-port)
                  indexing-consumer (k/create-indexing-consumer admin-client consumer indexer options)]
        (log/info "system started")
        (with-system-fn
          {:kv-store kv-store
           :tx-log tx-log
           :producer producer
           :consumer consumer
           :indexer indexer
           :admin-client admin-client
           :http-server http-server
           :indexing-consumer indexing-consumer})
        (log/info "stopping system")))
    (log/info "system stopped")))

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
           (fn [running-system]
             @(shutdown-hook-promise)))))))

(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread throwable]
     (log/error throwable "Uncaught exception:"))))
