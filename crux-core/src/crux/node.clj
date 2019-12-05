(ns crux.node
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [com.stuartsierra.dependency :as dep]
            [crux.backup :as backup]
            [crux.codec :as c]
            [crux.config :as cc]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            crux.object-store
            [crux.query :as q]
            [crux.status :as status]
            [crux.tx :as tx])
  (:import [crux.api ICruxAPI ICruxAsyncIngestAPI]
           java.io.Closeable
           [java.util.concurrent Executors ThreadFactory]
           java.util.concurrent.locks.StampedLock))

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

(defn- ensure-node-open [{:keys [closed?]}]
  (when @closed?
    (throw (IllegalStateException. "Crux node is closed"))))

(defrecord CruxNode [kv-store tx-log indexer object-store options close-fn status-fn closed? ^StampedLock lock]
  ICruxAPI
  (db [this]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (let [tx-time (:crux.tx/tx-time (db/read-index-meta indexer :crux.tx/latest-completed-tx))]
        (q/db kv-store object-store tx-time tx-time))))

  (db [this valid-time]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (let [transact-time (:crux.tx/tx-time (db/read-index-meta indexer :crux.tx/latest-completed-tx))]
        (.db this valid-time transact-time))))

  (db [this valid-time transact-time]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (q/db kv-store object-store valid-time transact-time)))

  (document [this content-hash]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (with-open [snapshot (kv/new-snapshot kv-store)]
        (db/get-single-object object-store snapshot (c/new-id content-hash)))))

  (documents [this content-hash-set]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (with-open [snapshot (kv/new-snapshot kv-store)]
        (db/get-objects object-store snapshot (map c/new-id content-hash-set)))))

  (history [this eid]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (with-open [snapshot (kv/new-snapshot kv-store)]
        (mapv c/entity-tx->edn (idx/entity-history snapshot eid)))))

  (historyRange [this eid valid-time-start transaction-time-start valid-time-end transaction-time-end]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (with-open [snapshot (kv/new-snapshot kv-store)]
        (->> (idx/entity-history-range snapshot eid valid-time-start transaction-time-start valid-time-end transaction-time-end)
             (mapv c/entity-tx->edn)
             (sort-by (juxt :crux.db/valid-time :crux.tx/tx-time))))))

  (status [this]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (status-fn)))

  (attributeStats [this]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (idx/read-meta kv-store :crux.kv/stats)))

  (submitTx [this tx-ops]
    (cio/with-read-lock lock
      (ensure-node-open this)
      @(db/submit-tx tx-log tx-ops)))

  (hasSubmittedTxUpdatedEntity [this submitted-tx eid]
    (.hasSubmittedTxCorrectedEntity this submitted-tx (:crux.tx/tx-time submitted-tx) eid))

  (hasSubmittedTxCorrectedEntity [this submitted-tx valid-time eid]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (tx/await-tx-time indexer (:crux.tx/tx-time submitted-tx) (:crux.tx-log/await-tx-timeout options))
      (q/submitted-tx-updated-entity? kv-store object-store submitted-tx valid-time eid)))

  (newTxLogContext [this]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (db/new-tx-log-context tx-log)))

  (txLog [this tx-log-context from-tx-id with-ops?]
    (cio/with-read-lock lock
      (ensure-node-open this)
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
          tx-log-entry))))

  (sync [this timeout]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (-> (tx/await-no-consumer-lag indexer (or (and timeout (.toMillis timeout))
                                                (:crux.tx-log/await-tx-timeout options)))
          :crux.tx/tx-time)))

  (sync [this tx-time timeout]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (-> (tx/await-tx-time indexer tx-time (or (and timeout (.toMillis timeout))
                                                (:crux.tx-log/await-tx-timeout options)))
          :crux.tx/tx-time)))

  ICruxAsyncIngestAPI
  (submitTxAsync [this tx-ops]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (db/submit-tx tx-log tx-ops)))

  backup/INodeBackup
  (write-checkpoint [this {:keys [crux.backup/checkpoint-directory] :as opts}]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (kv/backup kv-store (io/file checkpoint-directory "kv-store"))

      (when (satisfies? tx-log backup/INodeBackup)
        (backup/write-checkpoint tx-log opts))))

  Closeable
  (close [_]
    (cio/with-write-lock lock
      (when (and (not @closed?) close-fn) (close-fn))
      (reset! closed? true))))

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

(s/def ::topology-id
  (fn [id]
    (and (or (string? id) (keyword? id) (symbol? id))
         (namespace (symbol id)))))

(s/def ::start-fn fn?)
(s/def ::deps (s/coll-of keyword?))
(s/def ::args (s/map-of keyword?
                        (s/keys :req [:crux.config/type]
                                :req-un [:crux.config/doc]
                                :opt-un [:crux.config/default
                                         :crux.config/required?])))

(defn- resolve-topology-id [id]
  (s/assert ::topology-id id)
  (-> id symbol requiring-resolve var-get))

(s/def ::module (s/and (s/and (s/or :module-id ::topology-id :module map?)
                              (s/conformer
                               (fn [[m-or-id s]]
                                 (if (= :module-id m-or-id)
                                   (resolve-topology-id s) s))))
                       (s/keys :req-un [::start-fn]
                               :opt-un [::deps ::args])))

(defn- start-order [system]
  (let [g (reduce-kv (fn [g k m]
                       (let [m (s/conform ::module m)]
                         (reduce (fn [g d] (dep/depend g k d)) g (:deps m))))
                     (dep/graph)
                     system)
        dep-order (dep/topo-sort g)
        dep-order (->> (keys system)
                       (remove #(contains? (set dep-order) %))
                       (into dep-order))]
    dep-order))

(defn- parse-opts [args options]
  (into {}
        (for [[k {:keys [crux.config/type default required?]}] args
              :let [[validate-fn parse-fn] (s/conform :crux.config/type type)
                    v (some-> (get options k) parse-fn)
                    v (if (nil? v) default v)]]
          (do
            (when (and required? (not v))
              (throw (IllegalArgumentException. (format "Arg %s required" k))))
            (when (and v (not (validate-fn v)))
              (throw (IllegalArgumentException. (format "Arg %s invalid" k))))
            [k v]))))

(defn start-module [m started options]
  (s/assert ::module m)
  (let [{:keys [start-fn deps spec args]} (s/conform ::module m)
        deps (select-keys started deps)
        options (merge options (parse-opts args options))]
    (start-fn deps options)))

(s/def ::topology-map (s/map-of keyword? ::module))

(defn start-modules [topology options]
  (s/assert ::topology-map topology)
  (let [started-order (atom [])
        started (atom {})
        started-modules (try
                          (into {}
                                (for [k (start-order topology)]
                                  (let [m (topology k)
                                        _ (assert m (str "Could not find module " k))
                                        m (start-module m @started options)]
                                    (swap! started-order conj m)
                                    (swap! started assoc k m)
                                    [k m])))
                          (catch Throwable t
                            (doseq [c (reverse @started-order)]
                              (when (instance? Closeable c)
                                (cio/try-close c)))
                            (throw t)))]
    [started-modules (fn []
                       (doseq [m (reverse @started-order)
                               :when (instance? Closeable m)]
                         (cio/try-close m)))]))

(def base-topology {::kv-store 'crux.kv.rocksdb/kv
                    ::object-store 'crux.object-store/kv-object-store
                    ::indexer {:start-fn start-kv-indexer
                               :deps [::kv-store ::tx-log ::object-store]}})

(defn options->topology [{:keys [crux.node/topology] :as options}]
  (when-not topology
    (throw (IllegalArgumentException. "Please specify :crux.node/topology")))
  (let [topology (if (map? topology) topology (resolve-topology-id topology))
        topology-overrides (select-keys options (keys topology))
        topology (merge topology (zipmap (keys topology-overrides)
                                         (map resolve-topology-id (vals topology-overrides))))]
    (s/assert ::topology-map topology)
    topology))

(def node-args
  {:crux.tx-log/await-tx-timeout
   {:doc "Default timeout in milliseconds for waiting."
    :default 10000
    :crux.config/type :crux.config/nat-int}})

(defn start ^ICruxAPI [options]
  (let [options (into {} options)
        topology (options->topology options)
        [modules close-fn] (start-modules topology options)
        {::keys [kv-store tx-log indexer object-store]} modules
        status-fn (fn [] (apply merge (map status/status-map (cons (crux-version) (vals modules)))))
        node-opts (parse-opts node-args options)]
    (map->CruxNode {:close-fn close-fn
                    :status-fn status-fn
                    :options node-opts
                    :kv-store kv-store
                    :tx-log tx-log
                    :indexer indexer
                    :object-store object-store
                    :closed? (atom false)
                    :lock (StampedLock.)})))
