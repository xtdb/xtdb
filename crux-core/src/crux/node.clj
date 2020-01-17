(ns crux.node
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [com.stuartsierra.dependency :as dep]
            [crux.api :as api]
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
            [crux.tx :as tx]
            [crux.bus :as bus])
  (:import [crux.api ICruxAPI ICruxAsyncIngestAPI NodeOutOfSyncException TxLogIterator]
           java.io.Closeable
           java.util.Date
           [java.util.concurrent Executors]
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

(defrecord CruxNode [kv-store tx-log indexer object-store bus
                     options close-fn status-fn closed? ^StampedLock lock]
  ICruxAPI
  (db [this]
    (.db this nil nil))

  (db [this valid-time]
    (.db this valid-time nil))

  (db [this valid-time tx-time]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (let [latest-tx-time (:crux.tx/tx-time (db/read-index-meta indexer :crux.tx/latest-completed-tx))
            _ (when (and tx-time (or (nil? latest-tx-time) (pos? (compare tx-time latest-tx-time))))
                (throw (NodeOutOfSyncException. (format "node hasn't indexed the requested transaction: requested: %s, available: %s"
                                                        tx-time latest-tx-time)
                                                tx-time latest-tx-time)))
            tx-time (or tx-time latest-tx-time)
            valid-time (or valid-time (Date.))]

        (q/db kv-store object-store valid-time tx-time))))

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

  (hasTxCommitted [this {:keys [crux.tx/tx-id
                                crux.tx/tx-time] :as submitted-tx}]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (let [{latest-tx-id :crux.tx/tx-id
             latest-tx-time :crux.tx/tx-time} (db/read-index-meta indexer :crux.tx/latest-completed-tx)]
        (if (and tx-id (or (nil? latest-tx-id) (pos? (compare tx-id latest-tx-id))))
          (throw
           (NodeOutOfSyncException.
            (format "Node hasn't indexed the transaction: requested: %s, available: %s" tx-time latest-tx-time)
            tx-time latest-tx-time))
          (nil?
           (kv/get-value (kv/new-snapshot kv-store)
                         (c/encode-failed-tx-id-key-to nil tx-id)))))))

  (openTxLogIterator ^TxLogIterator [this from-tx-id with-ops?]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (let [tx-log-iterator (db/open-tx-log-iterator tx-log from-tx-id)
            snapshot (kv/new-snapshot kv-store)
            filtered-txs (filter
                          #(nil?
                            (kv/get-value snapshot
                                          (c/encode-failed-tx-id-key-to nil (:crux.tx/tx-id %))))
                          (iterator-seq tx-log-iterator))
            tx-log (if with-ops?
                     (map (fn [{:keys [crux.tx/tx-id
                                       crux.tx.event/tx-events] :as tx-log-entry}]
                            (-> tx-log-entry
                                (dissoc :crux.tx.event/tx-events)
                                (assoc :crux.api/tx-ops
                                       (->> tx-events
                                            (mapv #(tx/tx-event->tx-op % snapshot object-store))))))
                          filtered-txs)
                     filtered-txs)]

        (db/->closeable-tx-log-iterator (fn []
                                          (.close snapshot)
                                          (.close tx-log-iterator))
                                        tx-log))))

  (sync [this timeout]
    (when-let [tx (db/latest-submitted-tx (:tx-log this))]
      (-> (api/await-tx this tx nil)
          :crux.tx/tx-time)))

  (awaitTxTime [this tx-time timeout]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (-> (tx/await-tx-time indexer tx-time (or (and timeout (.toMillis timeout))
                                                (:crux.tx-log/await-tx-timeout options)))
          :crux.tx/tx-time)))

  (awaitTx [this submitted-tx timeout]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (tx/await-tx indexer submitted-tx (or (and timeout (.toMillis timeout))
                                            (:crux.tx-log/await-tx-timeout options)))))

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

(s/def ::resolvable-id
  (fn [id]
    (and (or (string? id) (keyword? id) (symbol? id))
         (namespace (symbol id)))))

(defn- resolve-id [id]
  (s/assert ::resolvable-id id)
  (-> (or (-> id symbol requiring-resolve)
          (throw (IllegalArgumentException. (format "Can't resolve symbol: '%s'" id))))
      var-get))

(s/def ::start-fn ifn?)
(s/def ::deps (s/coll-of keyword?))

(s/def ::args
  (s/map-of keyword?
            (s/keys :req [:crux.config/type]
                    :req-un [:crux.config/doc]
                    :opt-un [:crux.config/default
                             :crux.config/required?])))

(s/def ::component
  (s/and (s/or :component-id ::resolvable-id, :component map?)
         (s/conformer (fn [[c-or-id s]]
                        (cond-> s (= :component-id c-or-id) resolve-id)))
         (s/keys :req-un [::start-fn]
                 :opt-un [::deps ::args])))

(s/def ::module
  (s/and (s/or :module-id ::resolvable-id, :module map?)
         (s/conformer (fn [[m-or-id s]]
                        (cond-> s (= :module-id m-or-id) resolve-id)))))

(s/def ::resolved-topology (s/map-of keyword? ::component))

(defn options->topology [{:keys [crux.node/topology] :as options}]
  (when-not topology
    (throw (IllegalArgumentException. "Please specify :crux.node/topology")))

  (let [topology (-> topology
                     (cond-> (not (vector? topology)) vector)
                     (->> (map #(s/conform ::module %))
                          (apply merge)))]
    (->> (merge topology (select-keys options (keys topology)))
         (s/conform ::resolved-topology))))

(defn- start-order [system]
  (let [g (reduce-kv (fn [g k c]
                       (let [c (s/conform ::component c)]
                         (reduce (fn [g d] (dep/depend g k d)) g (:deps c))))
                     (dep/graph)
                     system)
        dep-order (dep/topo-sort g)
        dep-order (->> (keys system)
                       (remove #(contains? (set dep-order) %))
                       (into dep-order))]
    dep-order))

(defn- parse-opts [args options]
  (into {}
        (for [[k {:keys [crux.config/type default required?]}] args]
          (let [[validate-fn parse-fn] (s/conform :crux.config/type type)
                v (some-> (get options k) parse-fn)
                v (if (nil? v) default v)]

            (when (and required? (not v))
              (throw (IllegalArgumentException. (format "Arg %s required" k))))

            (when (and v (not (validate-fn v)))
              (throw (IllegalArgumentException. (format "Arg %s invalid" k))))

            [k v]))))

(defn start-component [c started options]
  (s/assert ::component c)
  (let [{:keys [start-fn deps spec args]} (s/conform ::component c)
        deps (select-keys started deps)
        options (merge options (parse-opts args options))]
    (start-fn deps options)))

(defn start-components [topology options]
  (s/assert ::resolved-topology topology)
  (let [started-order (atom [])
        started (atom {})
        started-modules (try
                          (into {}
                                (for [k (start-order topology)]
                                  (let [c (or (get topology k)
                                              (throw (IllegalArgumentException. (str "Could not find component " k))))
                                        c (start-component c @started options)]
                                    (swap! started-order conj c)
                                    (swap! started assoc k c)
                                    [k c])))
                          (catch Throwable t
                            (doseq [c (reverse @started-order)]
                              (when (instance? Closeable c)
                                (cio/try-close c)))
                            (throw t)))]
    [started-modules (fn []
                       (doseq [c (reverse @started-order)
                               :when (instance? Closeable c)]
                         (cio/try-close c)))]))

(def base-topology
  {::kv-store 'crux.kv.rocksdb/kv
   ::object-store 'crux.object-store/kv-object-store
   ::indexer 'crux.tx/kv-indexer
   ::bus 'crux.bus/bus})

(def node-args
  {:crux.tx-log/await-tx-timeout
   {:doc "Default timeout in milliseconds for waiting."
    :default 10000
    :crux.config/type :crux.config/nat-int}})

(defn start ^crux.api.ICruxAPI [options]
  (let [options (into {} options)
        topology (options->topology options)
        [components close-fn] (start-components topology options)
        {::keys [kv-store tx-log indexer object-store bus]} components
        status-fn (fn [] (apply merge (map status/status-map (cons (crux-version) (vals components)))))
        node-opts (parse-opts node-args options)]
    (map->CruxNode {:close-fn close-fn
                    :status-fn status-fn
                    :options node-opts
                    :kv-store kv-store
                    :tx-log tx-log
                    :indexer indexer
                    :object-store object-store
                    :bus bus
                    :closed? (atom false)
                    :lock (StampedLock.)})))
