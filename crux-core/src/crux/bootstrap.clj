(ns crux.bootstrap
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.stuartsierra.dependency :as dep]
            [crux.backup :as backup]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.query :as q]
            [crux.status :as status]
            [crux.tx :as tx])
  (:import [crux.api ICruxAPI ICruxAsyncIngestAPI]
           java.io.Closeable
           [java.util.concurrent Executors ThreadFactory]
           java.util.UUID))

(s/check-asserts (if-let [check-asserts (System/getProperty "clojure.spec.compile-asserts")]
                   (Boolean/parseBoolean check-asserts)
                   true))

(def default-options {:bootstrap-servers "localhost:9092"
                      :group-id (str/trim (or (System/getenv "HOSTNAME")
                                              (System/getenv "COMPUTERNAME")
                                              (.toString (java.util.UUID/randomUUID))))
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
     (when-let [pom-file (io/resource "META-INF/maven/juxt/crux-core/pom.properties")]
       (with-open [in (io/reader pom-file)]
         (let [{:strs [version
                       revision]} (cio/load-properties in)]
           (->CruxVersion version revision)))))))

(defrecord CruxNode [kv-store tx-log indexer object-store options close-fn]
  ICruxAPI
  (db [this]
    (let [tx-time (tx/latest-completed-tx-time (db/read-index-meta indexer :crux.tx-log/consumer-state))]
      (q/db kv-store object-store tx-time tx-time options)))

  (db [this valid-time]
    (let [transact-time (tx/latest-completed-tx-time (db/read-index-meta indexer :crux.tx-log/consumer-state))]
      (.db this valid-time transact-time)))

  (db [_ valid-time transact-time]
    (q/db kv-store object-store valid-time transact-time options))

  (document [_ content-hash]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (db/get-single-object object-store snapshot (c/new-id content-hash))))

  (documents [_ content-hash-set]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (db/get-objects object-store snapshot (map c/new-id content-hash-set))))

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

  (attributeStats [this]
    (idx/read-meta kv-store :crux.kv/stats))

  (submitTx [_ tx-ops]
    @(db/submit-tx tx-log tx-ops))

  (hasSubmittedTxUpdatedEntity [this submitted-tx eid]
    (.hasSubmittedTxCorrectedEntity this submitted-tx (:crux.tx/tx-time submitted-tx) eid))

  (hasSubmittedTxCorrectedEntity [_ submitted-tx valid-time eid]
    (tx/await-tx-time indexer (:crux.tx/tx-time submitted-tx) (:crux.tx-log/await-tx-timeout options))
    (q/submitted-tx-updated-entity? kv-store submitted-tx valid-time eid))

  (newTxLogContext [_]
    (db/new-tx-log-context tx-log))

  (txLog [_ tx-log-context from-tx-id with-ops?]
    (for [{:keys [crux.tx/tx-id
                  crux.tx.event/tx-events] :as tx-log-entry} (db/tx-log tx-log tx-log-context from-tx-id)
          :when (with-open [snapshot (kv/new-snapshot kv-store)]
                  (nil? (kv/get-value snapshot (c/encode-failed-tx-id-key-to nil tx-id))))]
      (if with-ops?
        (-> tx-log-entry
            (dissoc :crux.tx.event/tx-events)
            (assoc :crux.api/tx-ops
                   (with-open [snapshot (kv/new-snapshot kv-store)]
                     (tx/tx-events->tx-ops snapshot object-store tx-events))))
        tx-log-entry)))

  (sync [_ timeout]
    (tx/await-no-consumer-lag
     indexer
     (cond-> options
       timeout (assoc :crux.tx-log/await-tx-timeout (.toMillis timeout)))))

  (sync [_ tx-time timeout]
    (tx/await-tx-time indexer tx-time (when timeout {:crux.tx-log/await-tx-timeout (.toMillis timeout)})))

  ICruxAsyncIngestAPI
  (submitTxAsync [_ tx-ops]
    (db/submit-tx tx-log tx-ops))

  backup/INodeBackup
  (write-checkpoint [this {:keys [crux.backup/checkpoint-directory] :as opts}]
    (kv/backup kv-store (io/file checkpoint-directory "kv-store"))
    (when (satisfies? tx-log backup/INodeBackup)
      (backup/write-checkpoint tx-log opts)))

  Closeable
  (close [_]
    (when close-fn (close-fn))))

(defn start-kv-store
  (^java.io.Closeable [_ options]
   (start-kv-store options))
  (^java.io.Closeable [{:keys [db-dir
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
         (throw t))))))

(defn start-object-store ^java.io.Closeable [partial-node {:keys [object-store]
                                                           :or {object-store (:object-store default-options)}
                                                           :as options}]
  (-> (db/require-and-ensure-object-store-record object-store)
      (cio/new-record)
      (db/init partial-node options)))

(defn start-raw-object-store [{:keys [kv-store]} options]
  (start-object-store {:kv kv-store} options))

(defn- start-cached-object-store [{:keys [kv-store]} {:keys [doc-cache-size] :as options}]
  (lru/->CachedObjectStore (lru/new-cache doc-cache-size)
                           (start-object-store {:kv kv-store} options)))

(defn install-uncaught-exception-handler! []
  (when-not (Thread/getDefaultUncaughtExceptionHandler)
    (Thread/setDefaultUncaughtExceptionHandler
     (reify Thread$UncaughtExceptionHandler
       (uncaughtException [_ thread throwable]
         (log/error throwable "Uncaught exception:"))))))

(defn- start-kv-indexer [{:keys [kv-store tx-log object-store]} _]
  (tx/->KvIndexer kv-store tx-log object-store
                  (Executors/newSingleThreadExecutor
                   (reify ThreadFactory
                     (newThread [_ r]
                       (doto (Thread. r)
                         (.setName "crux.tx.update-stats-thread")))))))

(defn- start-order [system]
  (let [g (reduce-kv (fn [g k module-def]
                       (reduce (fn [g d] (dep/depend g k d)) g (second module-def)))
                     (dep/graph)
                     system)
        dep-order (dep/topo-sort g)
        dep-order (->> (keys system)
                       (remove #(contains? (set dep-order) %))
                       (into dep-order))]
    dep-order))

(defn start-modules [node-system options]
  (let [started (atom {})
        start-order (start-order node-system)
        started-modules (try
                          (for [k start-order]
                            (let [[start-fn deps spec] (node-system k)
                                  deps (select-keys @started deps)]
                              (when spec
                                (s/assert spec options))
                              [k (doto (start-fn deps options) (->> (swap! started assoc k)))]))
                          (catch Throwable t
                            (doseq [c (reverse @started)]
                              (when (instance? Closeable c)
                                (cio/try-close c)))
                            (throw t)))]
    [(into {} started-modules) (fn []
                                 (doseq [k (reverse start-order)
                                         :let [m (get @started k)]
                                         :when (instance? Closeable m)]
                                   (cio/try-close m)))]))

(comment
  (start-modules {:a [(fn [deps] (println deps) :start-a) :b]
                  :b (fn [deps] :start-b)
                  :c (fn [deps] :start-c)}))

(def raw-object-store [start-raw-object-store [:kv-store] (s/keys :opt-un [:crux.db/object-store])])
(def object-store [start-cached-object-store [:kv-store] (s/keys :opt-un [:crux.lru/doc-cache-size])])
(def kv-indexer [start-kv-indexer [:kv-store :tx-log :object-store]])
(def kv-store [start-kv-store [] :crux.kv/options])

(def base-node-config {:kv-store kv-store
                       :raw-object-store raw-object-store
                       :object-store object-store
                       :indexer kv-indexer})

(defn start-node ^ICruxAPI [node-config options]
  (let [options (merge default-options options)
        node-config (merge base-node-config node-config)
        [node-modules close-fn] (start-modules node-config options)]
    (map->CruxNode (assoc node-modules :close-fn close-fn :options options))))
