(ns crux.node
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
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
           [java.util.concurrent Executors ThreadFactory]))

(s/check-asserts (if-let [check-asserts (System/getProperty "clojure.spec.compile-asserts")]
                   (Boolean/parseBoolean check-asserts)
                   true))

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

(defrecord CruxNode [kv-store tx-log indexer object-store options close-fn status-fn]
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
    (status-fn))

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

(defn start-object-store ^java.io.Closeable [partial-node {:keys [crux.db/object-store] :as options}]
  (-> (db/require-and-ensure-object-store-record object-store)
      (cio/new-record)
      (db/init partial-node options)))

(defn install-uncaught-exception-handler! []
  (when-not (Thread/getDefaultUncaughtExceptionHandler)
    (Thread/setDefaultUncaughtExceptionHandler
     (reify Thread$UncaughtExceptionHandler
       (uncaughtException [_ thread throwable]
         (log/error throwable "Uncaught exception:"))))))

(defn- start-kv-indexer [{::keys [kv-store tx-log object-store]} _]
  (tx/->KvIndexer kv-store tx-log object-store
                  (Executors/newSingleThreadExecutor
                   (reify ThreadFactory
                     (newThread [_ r]
                       (doto (Thread. r)
                         (.setName "crux.tx.update-stats-thread")))))))

(defn resolve-topology-or-module [s]
  (assert s)
  (if (or (string? s) (keyword? s) (symbol? s))
    (let [v (symbol s)
          ns (namespace v)]
      (assert ns)
      (require (symbol ns))
      (var-get (find-var v)))
    s))

(defn- start-order [system]
  (let [g (reduce-kv (fn [g k m]
                       (let [m (resolve-topology-or-module m)]
                         (reduce (fn [g d] (dep/depend g k d)) g (second m))))
                     (dep/graph)
                     system)
        dep-order (dep/topo-sort g)
        dep-order (->> (keys system)
                       (remove #(contains? (set dep-order) %))
                       (into dep-order))]
    dep-order))

(s/def ::module (s/cat :start-fn fn?
                       :deps (s/? (s/every keyword?))
                       :spec (s/? (fn [s] (or (s/spec? s) (s/get-spec s))))
                       :meta-args (s/? (s/map-of keyword?
                                                 (s/keys :req-un [::doc]
                                                         :opt-un [::default])))))

(defn start-module [m started options]
  (let [m (resolve-topology-or-module m)
        _ (s/assert ::module m)
        {:keys [start-fn deps spec meta-args]} (s/conform ::module m)
        deps (select-keys started deps)
        default-options (into {} (map (juxt key (comp :default val))
                                      (filter #(find (val %) :default) meta-args)))
        options (merge default-options options)]
    (when spec
      (s/assert spec options))
    (start-fn deps options)))

(defn start-modules [{:keys [crux.node/topology] :as options}]
  (let [topology (resolve-topology-or-module topology)
        topology (merge topology (select-keys options (keys topology)))
        started (atom {})
        start-order (start-order topology)
        started-modules (try
                          (for [k start-order]
                            (let [m (topology k)
                                  _ (assert m (str "Could not find module " k))
                                  m (start-module m @started options)]
                              (swap! started assoc k m)
                              [k m]))
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

(def base-topology {::kv-store 'crux.kv.rocksdb/kv
                    ::raw-object-store [(fn [{::keys [kv-store]} options]
                                          (start-object-store {:kv kv-store} options))
                                        [::kv-store]
                                        (s/keys :req [:crux.db/object-store])
                                        {:crux.db/object-store {:doc "Node local store for documents/objects"
                                                                :default "crux.index.KvObjectStore"}}]
                    ::object-store [(fn [{::keys [raw-object-store]} {:keys [crux.lru/doc-cache-size]}]
                                      (lru/->CachedObjectStore (lru/new-cache doc-cache-size) raw-object-store))
                                    [::raw-object-store]
                                    (s/keys :req [:crux.lru/doc-cache-size])
                                    {:crux.lru/doc-cache-size {:doc "Cache size to use for document store"
                                                               :default lru/default-doc-cache-size}}]
                    ::indexer [start-kv-indexer
                               [::kv-store ::tx-log ::object-store]
                               {:crux.tx-log/await-tx-timeout
                                {:doc "Timeout waiting for tx-time to be reached by a Crux node"
                                 :default crux.tx/default-await-tx-timeout}}]})

(defn start ^ICruxAPI [options]
  (let [[{::keys [kv-store tx-log indexer object-store] :as modules} close-fn] (start-modules options)
        status-fn (fn [] (apply merge (map status/status-map (cons (crux-version) (vals modules)))))]
    (map->CruxNode {:close-fn close-fn
                    :status-fn status-fn
                    :options options
                    :kv-store kv-store
                    :tx-log tx-log
                    :indexer indexer
                    :object-store object-store})))
