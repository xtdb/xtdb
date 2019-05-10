(ns crux.bootstrap
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [crux.backup :as backup]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.index :as idx]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.query :as q]
            [crux.status :as status]
            [crux.tx :as tx])
  (:import java.io.Closeable
           java.net.InetAddress
           crux.api.ICruxAPI))

(s/check-asserts (if-let [check-asserts (System/getProperty "clojure.spec.compile-asserts")]
                   (Boolean/parseBoolean check-asserts)
                   true))

(def default-options {:bootstrap-servers "localhost:9092"
                      :group-id (.trim ^String (:out (clojure.java.shell/sh "hostname")))
                      :tx-topic "crux-transaction-log"
                      :doc-topic "crux-docs"
                      :create-topics true
                      :doc-partitions 1
                      :replication-factor 1
                      :db-dir "data"
                      :kv-backend "crux.kv.rocksdb.RocksKv"
                      :server-port 3000
                      :await-tx-timeout 10000
                      :doc-cache-size (* 128 1024)
                      :object-store "crux.index.KvObjectStore"})

(defrecord CruxVersion [version revision]
  status/Status
  (status-map [this]
    {:crux.version/version version
     :crux.version/revision revision}))

(def crux-version
  (memoize
   (fn []
     (with-open [in (io/reader (io/resource "META-INF/maven/juxt/crux/pom.properties"))]
       (let [{:strs [version
                     revision]} (cio/load-properties in)]
         (->CruxVersion version revision))))))

(defrecord CruxNode [close-promise kv-store tx-log indexer object-store consumer-config options ^Thread node-thread]
  ICruxAPI
  (db [_]
    (let [tx-time (tx/latest-completed-tx-time (db/read-index-meta indexer :crux.tx-log/consumer-state))]
      (q/db kv-store object-store tx-time tx-time options)))

  (db [_ valid-time]
    (let [tx-time (tx/latest-completed-tx-time (db/read-index-meta indexer :crux.tx-log/consumer-state))]
      (q/db kv-store object-store valid-time tx-time options)))

  (db [_ valid-time transact-time]
    (tx/await-tx-time indexer transact-time options)
    (q/db kv-store object-store valid-time transact-time options))

  (document [_ content-hash]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (db/get-single-object object-store snapshot (c/new-id content-hash))))

  (history [_ eid]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (mapv c/entity-tx->edn (idx/entity-history snapshot eid))))

  (historyRange [_ eid valid-time-start transaction-time-start valid-time-end transaction-time-end]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (->> (idx/entity-history-range snapshot eid valid-time-start transaction-time-start valid-time-end transaction-time-end)
           (mapv c/entity-tx->edn)
           (sort-by (juxt :crux.db/valid-time :crux.tx/tx-time)))))

  (status [this]
    (apply merge (map status/status-map (cons (crux-version) (vals this)))))

  (submitTx [_ tx-ops]
    @(db/submit-tx tx-log tx-ops))

  (hasSubmittedTxUpdatedEntity [this submitted-tx eid]
    (.hasSubmittedTxCorrectedEntity this submitted-tx (:crux.tx/tx-time submitted-tx) eid))

  (hasSubmittedTxCorrectedEntity [_ submitted-tx valid-time eid]
    (tx/await-tx-time indexer (:crux.tx/tx-time submitted-tx) (:crux.tx-log/await-tx-timeout options))
    (q/submitted-tx-updated-entity? kv-store submitted-tx valid-time eid))

  (newTxLogContext [_]
    (db/new-tx-log-context tx-log))

  (txLog [_ tx-log-context from-tx-id with-documents?]
    (for [tx-log-entry (db/tx-log tx-log tx-log-context from-tx-id)]
      (if with-documents?
        (update tx-log-entry
                :crux.tx/tx-ops
                #(with-open [snapshot (kv/new-snapshot kv-store)]
                   (tx/enrich-tx-ops-with-documents snapshot object-store %)))
        tx-log-entry)))

  (sync [_ timeout]
    (tx/await-no-consumer-lag
      indexer
      (cond-> options
        timeout (assoc :crux.tx-log/await-tx-timeout (.toMillis timeout)))))

  backup/ISystemBackup
  (write-checkpoint [this {:keys [crux.backup/checkpoint-directory]}]
    (kv/backup kv-store (io/file checkpoint-directory "kv-store")))

  Closeable
  (close [_]
    (some-> close-promise (deliver true))
    (some-> node-thread (.join))))

(defn start-kv-store ^java.io.Closeable [{:keys [db-dir
                                                 kv-backend
                                                 sync?
                                                 crux.index/check-and-store-index-version]
                                          :as options
                                          :or {check-and-store-index-version true}}]
  (s/assert :crux.kv/options options)
  (let [kv (-> (kv/new-kv-store kv-backend)
               (lru/new-cache-providing-kv-store)
               (kv/open options))]
    (try
      (if check-and-store-index-version
        (idx/check-and-store-index-version kv)
        kv)
      (catch Throwable t
        (.close ^Closeable kv)
        (throw t)))))

(defn start-object-store ^java.io.Closeable [partial-system {:keys [object-store]
                                                             :or {object-store (:object-store default-options)}
                                                             :as options}]
  (-> (db/require-and-ensure-object-store-record object-store)
      (cio/new-record)
      (db/init partial-system options)))

(defn install-uncaught-exception-handler! []
  (when-not (Thread/getDefaultUncaughtExceptionHandler)
    (Thread/setDefaultUncaughtExceptionHandler
     (reify Thread$UncaughtExceptionHandler
       (uncaughtException [_ thread throwable]
         (log/error throwable "Uncaught exception:"))))))
