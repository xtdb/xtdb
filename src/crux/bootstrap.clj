(ns crux.bootstrap
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.tools.logging :as log]
            [clojure.tools.cli :as cli]
            [crux.core :as crux]
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
   ["-t" "--topic TOPIC" "Kafka topic for the Crux transaction log"
    :default "crux-transaction-log"]
   ["-d" "--db-dir DB_DIR" "KV storage directory"
    :default "data"]
   ["-k" "--kv-backend KV_BACKEND" "KV storage backend: rocksdb, lmdb or memdb"
    :default "rocksdb"
    :validate [#{"rocksdb" "lmdb" "memdb"} "Unknown storage backend"]]
   ["-h" "--help"]])

(def default-options
  (merge {:replication-factor 3
          :running? (delay true)}
         (:options (cli/parse-opts [] cli-options))))

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
                    "rocksdb" crux.rocksdb/map->CruxRocksKv
                    "lmdb" crux.lmdb/map->CruxLMDBKv
                    "memdb" crux.memdb/map->CruxMemKv) {})]
    (->> (crux/kv db-dir {:kv-store kv-store})
         (kv-store/open))))

(defn start-system
  ([{:keys [bootstrap-servers
            group-id]
     :as options}]
   (with-open [kv-store (start-kv-store options)
               consumer (k/create-consumer {"bootstrap.servers" bootstrap-servers
                                            "group.id" group-id})
               admin-client (k/create-admin-client {"bootstrap.servers" bootstrap-servers})]
     (start-system kv-store consumer admin-client options)))
  ([kv-store consumer admin-client options]
   (let [{:keys [bootstrap-servers
                 group-id
                 topic
                 replication-factor
                 running?]
          :as options} (merge default-options options)
         indexer (crux/indexer kv-store)]
     (k/create-topic admin-client topic 1 replication-factor {})
     (k/subscribe-from-stored-offsets indexer consumer topic)
     (while @running?
       (k/consume-and-index-entities indexer consumer 100)))))

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
