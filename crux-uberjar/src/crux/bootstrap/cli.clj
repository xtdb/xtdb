(ns crux.bootstrap.cli
  (:require [clojure.edn :as edn]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [crux.bootstrap :as b]
            [crux.db :as db]
            [crux.kafka :as k]
            [crux.http-server :as srv]
            [crux.io :as cio]
            [crux.kv :as kv])
  (:import java.io.Closeable))

(def cli-options
  [;; Kafka
   ["-b" "--bootstrap-servers BOOTSTRAP_SERVERS" "Kafka bootstrap servers"
    :default (:bootstrap-servers b/default-options)]
   [nil "--kafka-properties-file KAFKA_PROPERTIES_FILE" "Kafka properties file for shared connection properties"]
   ["-g" "--group-id GROUP_ID" "Kafka group.id for this node"
    :default (:group-id b/default-options)]
   ["-t" "--tx-topic TOPIC" "Kafka topic for the Crux transaction log"
    :default (:tx-topic b/default-options)]
   ["-o" "--doc-topic TOPIC" "Kafka topic for the Crux documents"
    :default (:doc-topic b/default-options)]
   ["-c" "--[no-]create-topics" "Should Crux create Kafka topics"
    :default (:create-topics b/default-options)]
   ["-p" "--doc-partitions PARTITIONS" "Kafka partitions for the Crux documents topic"
    :default (:doc-partitions b/default-options)
    :parse-fn #(Long/parseLong %)]
   ["-r" "--replication-factor FACTOR" "Kafka topic replication factor"
    :default (:replication-factor b/default-options)
    :parse-fn #(Long/parseLong %)]

   ;; KV
   ["-d" "--db-dir DB_DIR" "KV storage directory"
    :default (:db-dir b/default-options)]
   ["-k" "--kv-backend KV_BACKEND" "KV storage backend: crux.kv.rocksdb.RocksKv, crux.kv.lmdb.LMDBKv or crux.kv.memdb.MemKv"
    :default (:kv-backend b/default-options)
    :validate [#'kv/require-and-ensure-kv-record "Unknown storage backend"]]

   ;; HTTP
   ["-s" "--server-port SERVER_PORT" "Port on which to run the HTTP server"
    :default (:server-port b/default-options)
    :parse-fn #(Long/parseLong %)]

   ;; Query
   ["-w" "--await-tx-timeout TIMEOUT" "Maximum time in ms to wait for transact time specified at query"
    :default (:await-tx-timeout b/default-options)
    :parse-fn #(Long/parseLong %)]
   ["-z" "--doc-cache-size SIZE" "Limit of number of documents in the query document cache"
    :default (:doc-cache-size b/default-options)
    :parse-fn #(Long/parseLong %)]
   ["-j" "--object-store OBJECT_STORE" "Type of object store to use."
    :default (:object-store b/default-options)
    :validate [#'db/require-and-ensure-object-store-record "Unknown object store"]]

   ;; Extra
   ["-x" "--extra-edn-options EDN_OPTIONS" "Extra options as an quoted EDN map."
    :default nil
    :parse-fn edn/read-string]

   ["-h" "--help"]])

;; NOTE: This isn't registered until the node manages to start up
;; cleanly, so ctrl-c keeps working as expected in case the node
;; fails to start.
(defn- shutdown-hook-promise []
  (let [main-thread (Thread/currentThread)
        shutdown? (promise)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (let [shutdown-ms 10000]
                                   (deliver shutdown? true)
                                   (shutdown-agents)
                                   (.join main-thread shutdown-ms)
                                   (when (.isAlive main-thread)
                                     (log/warn "could not stop node cleanly after" shutdown-ms "ms, forcing exit")
                                     (.halt (Runtime/getRuntime) 1))))
                               "crux.bootstrap.shutdown-hook-thread"))
    shutdown?))

(def env-prefix "CRUX_")

(defn- options-from-env []
  (->> (for [id (keys b/default-options)
             :let [env-var (str env-prefix (str/replace (str/upper-case (name id)) "-" "_"))
                   v (System/getenv env-var)]
             :when v]
         [(str "--" (name id)) v])
       (apply concat)))

(defn- options->table [options]
  (with-out-str
    (pp/print-table (for [[k v] options]
                      {:key k :value v}))))

(defn start-node-from-command-line [args]
  (b/install-uncaught-exception-handler!)
  (let [{:keys [options
                errors
                summary]} (cli/parse-opts (concat (options-from-env) args) cli-options)
        options (merge (dissoc options :extra-edn-options) (:extra-edn-options options))
        {:keys [version
                revision]} (b/crux-version)]
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
          (with-open [node (b/start-node k/node-config options)
                      http-server ^Closeable (srv/start-http-server node)]
            @(shutdown-hook-promise))))))
