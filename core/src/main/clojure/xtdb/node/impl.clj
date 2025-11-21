(ns xtdb.node.impl
  (:require [clojure.pprint :as pp]
            [integrant.core :as ig]
            [xtdb.antlr :as antlr]
            [xtdb.api :as api]
            [xtdb.basis :as basis]
            [xtdb.error :as err]
            [xtdb.garbage-collector]
            [xtdb.log :as xt-log]
            [xtdb.metrics :as metrics]
            [xtdb.protocols :as xtp]
            [xtdb.query :as q]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.tracer]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import io.micrometer.core.instrument.composite.CompositeMeterRegistry
           io.micrometer.core.instrument.Counter
           (java.io Closeable Writer)
           (java.util HashMap)
           [java.util.concurrent.atomic AtomicReference]
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (xtdb.adbc XtdbConnection XtdbConnection$Node)
           (xtdb.antlr Sql$DirectlyExecutableStatementContext)
           (xtdb.api DataSource TransactionResult Xtdb Xtdb$CompactorNode Xtdb$Config Xtdb$ExecutedTx Xtdb$SubmittedTx Xtdb$XtdbInternal)
           (xtdb.api.log Log$Message$Tx Log$MessageMetadata)
           xtdb.api.module.XtdbModule$Factory
           (xtdb.database Database Database$Catalog)
           xtdb.error.Anomaly
           (xtdb.query IQuerySource PreparedQuery)
           (xtdb.tx TxWriter)
           (xtdb.util MsgIdUtil)))

(set! *unchecked-math* :warn-on-boxed)

(defmethod ig/expand-key :xtdb/config [k ^Xtdb$Config config]
  {k {:config config
      :node-id (.getNodeId config)
      :default-tz (.getDefaultTz config)}})

(defmethod ig/init-key :xtdb/config [_ cfg] cfg)

(defmethod ig/init-key :xtdb/allocator [_ _]
  (RootAllocator. (long (* 0.9 (util/max-direct-memory)))))

(defmethod ig/halt-key! :xtdb/allocator [_ ^BufferAllocator a]
  (util/close a))

(defn- with-query-opts-defaults [query-opts {:keys [default-tz]}]
  (-> (into {:default-tz default-tz,
             :key-fn (serde/read-key-fn :snake-case-string)
             :default-db "xtdb"}
            query-opts)
      (update :current-time #(some-> % (time/->instant)))))

(defn- then-execute-prepared-query [^PreparedQuery prepared-query, allocator {:keys [args], :as query-opts} {:keys [query-timer] :as metrics}]
  (util/with-close-on-catch [cursor (util/with-close-on-catch [args-rel (vw/open-args allocator args)]
                                      (.openQuery prepared-query (assoc query-opts :args args-rel)))]
    ;;TODO metrics only currently wrapping openQueryAsync results
    (-> (q/cursor->stream cursor query-opts metrics)
        (metrics/wrap-query query-timer))))

(defn- await-msg-result [node ^Database db msg-id]
  (or (let [^TransactionResult tx-res (-> @(.awaitAsync (.getLogProcessor db) msg-id)
                                          (util/rethrowing-cause))]
        (when (and tx-res
                   (= (.getTxId tx-res) msg-id))
          tx-res))

      (with-open [res (xtp/open-sql-query node "SELECT system_time, committed AS \"committed?\", error FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?"
                                          {:args [msg-id]
                                           :key-fn (serde/read-key-fn :kebab-case-keyword)
                                           :default-db (.getName db)})]
        (let [{:keys [system-time committed? error]} (-> (.findFirst res) (.orElse nil))
              system-time (time/->instant system-time)]
          (if committed?
            (serde/->tx-committed msg-id system-time)
            (serde/->tx-aborted msg-id system-time error))))))

(defrecord Node [^BufferAllocator allocator, ^Database$Catalog db-cat
                 ^IQuerySource q-src
                 ^CompositeMeterRegistry metrics-registry
                 default-tz, ^AtomicReference !await-token
                 system, close-fn,
                 query-timer, ^Counter query-error-counter, ^Counter tx-error-counter]
  Xtdb
  (getAllocator [_] allocator)

  (getServerPort [this]
    (get-in (util/component this :xtdb.pgwire/server) [:read-write :port] -1))

  (getServerReadOnlyPort [this]
    (get-in (util/component this :xtdb.pgwire/server) [:read-only :port] -1))

  (createConnectionBuilder [this]
    (let [server (util/component this :xtdb.pgwire/server)
          ^DataSource data-source (or (:read-write server) (:read-only server))]
      (.createConnectionBuilder data-source)))

  (connect [this-node]
    (XtdbConnection.
     (reify XtdbConnection$Node
       (getAllocator [_] allocator)
       (executeTx [_ db-name ops opts] (.executeTx this-node db-name ops opts))

       (openSqlQuery [_ sql]
         (let [query-opts (-> {} (with-query-opts-defaults this-node))]
           (-> (xtp/prepare-sql this-node sql query-opts)
               (.openQuery query-opts)))))))

  (addMeterRegistry [_ reg]
    (.add metrics-registry reg))

  (getAwaitToken [_] (.get !await-token))

  (setAwaitToken [_ await-token]
    (loop []
      (let [old-token (.get !await-token)]
        (when (or (nil? old-token) await-token)
          (when-not (.compareAndSet !await-token old-token (basis/merge-tx-tokens old-token await-token))
            (recur))))))

  (module [_ clazz]
    (->> (vals (:xtdb/modules system))
         (some #(when (instance? clazz %) %))))

  (^PreparedQuery prepareSql [this ^String sql query-opts]
    (.prepareSql this (antlr/parse-statement sql) query-opts))

  (^PreparedQuery prepareSql [this ^Sql$DirectlyExecutableStatementContext ast query-opts]
    (let [{:keys [await-token tx-timeout] :as query-opts} (-> query-opts (with-query-opts-defaults this))]
      (.awaitAll db-cat await-token tx-timeout)
      (.prepareQuery q-src ast db-cat query-opts)))

  (submitTx [_ db-name tx-ops tx-opts]
    (try
      (let [^Database db (or (.databaseOrNull db-cat db-name)
                             (throw (err/incorrect :xtdb/unknown-db (format "Unknown database: %s" db-name)
                                                   {:db-name db-name})))
            log (.getLog db)
            tx-opts (.withFallbackTz tx-opts default-tz)]
        (util/rethrowing-cause
         (let [tx-msg (Log$Message$Tx. (TxWriter/serializeTxOps tx-ops allocator tx-opts))
               ^Log$MessageMetadata message-meta @(.appendMessage log tx-msg)]
           (Xtdb$SubmittedTx. (MsgIdUtil/offsetToMsgId (.getEpoch log) (.getLogOffset message-meta))))))
      (catch Anomaly e
        (when tx-error-counter
          (.increment tx-error-counter))
        (throw e))))

  (executeTx [this db-name tx-ops tx-opts]
    (let [tx-id (.getTxId (.submitTx this db-name tx-ops tx-opts))
          db (.databaseOrNull db-cat db-name)
          {:keys [tx-id system-time committed? error]} (await-msg-result this db tx-id)]
      (Xtdb$ExecutedTx. tx-id system-time committed? error)))

  Xtdb$XtdbInternal
  (getDbCatalog [_] db-cat)

  xtp/PNode
  (submit-tx [this tx-ops opts]
    (try
      (xt-log/submit-tx this tx-ops opts)
      (catch Anomaly e
        (when tx-error-counter
          (.increment tx-error-counter))
        (throw e))))

  (execute-tx [this tx-ops opts]
    (let [tx-id (xtp/submit-tx this tx-ops opts)
          db (.databaseOrNull db-cat (:default-db opts))]
      (await-msg-result this db tx-id)))

  (open-sql-query [this query query-opts]
    (let [query-opts (-> query-opts (with-query-opts-defaults this))]
      ;; We catch exceptions here to count errors outside of query execution e.g. parsing errors
      (try
        (-> (xtp/prepare-sql this query query-opts)
            (then-execute-prepared-query allocator query-opts {:query-timer query-timer :query-error-counter query-error-counter}))
        (catch Exception e
          (when query-error-counter
            (.increment query-error-counter))
          (throw e)))))

  (attach-db [this db-name db-config]
    (let [primary-db (.getPrimary db-cat)
          msg-id (xt-log/send-attach-db! primary-db db-name db-config)]
      (await-msg-result this primary-db msg-id)))

  (detach-db [this db-name]
    (let [primary-db (.getPrimary db-cat)
          msg-id (xt-log/send-detach-db! primary-db db-name)]
      (await-msg-result this primary-db msg-id)))

  xtp/PStatus
  (latest-completed-txs [_]
    (->> (.getDatabaseNames db-cat)
         (into {} (map (fn [db-name]
                         ;; TODO multi-part
                         [db-name [(-> (.databaseOrNull db-cat db-name)
                                       (.getLiveIndex)
                                       (.getLatestCompletedTx))]])))))

  (latest-submitted-msg-ids [_]
    (->> (.getDatabaseNames db-cat)
         (into {} (map (fn [db-name]
                         ;; TODO multi-part
                         [db-name [(-> (.databaseOrNull db-cat db-name)
                                       (.getLogProcessor)
                                       (.getLatestSubmittedMsgId))]])))))

  (latest-processed-msg-ids [_]
    (->> (.getDatabaseNames db-cat)
         (into {} (map (fn [db-name]
                         ;; TODO multi-part
                         [db-name [(-> (.databaseOrNull db-cat db-name)
                                       (.getLogProcessor)
                                       (.getLatestProcessedMsgId))]])))))

  (await-token [this]
    (basis/->tx-basis-str (xtp/latest-submitted-msg-ids this)))

  (snapshot-token [this]
    (basis/->time-basis-str (-> (xtp/latest-completed-txs this)
                                (update-vals #(mapv :system-time %)))))

  (status [this]
    {:latest-completed-txs (xtp/latest-completed-txs this)
     :latest-submitted-tx-ids (xtp/latest-submitted-msg-ids this)

     :await-token (xtp/await-token this)})

  xtp/PLocalNode
  (prepare-sql [this query query-opts]
    (cond
      (instance? Sql$DirectlyExecutableStatementContext query)
      (.prepareSql this ^Sql$DirectlyExecutableStatementContext query query-opts)

      (string? query)
      (.prepareSql this ^String query query-opts)

      :else (throw (err/incorrect :xtdb/unsupported-query-type (format "Unsupported SQL query type: %s" (type query))))))

  (prepare-ra [this plan query-opts]
    (let [{:keys [await-token tx-timeout] :as query-opts} (-> query-opts (with-query-opts-defaults this))]

      (.awaitAll db-cat await-token tx-timeout)

      (.prepareQuery q-src plan db-cat query-opts)))

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method Node [_node ^Writer w] (.write w "#<XtdbNode>"))
(defmethod pp/simple-dispatch Node [it] (print-method it *out*))

(defmethod ig/expand-key :xtdb/node [k opts]
  {k (merge {:allocator (ig/ref :xtdb/allocator)
             :config (ig/ref :xtdb/config)
             :q-src (ig/ref :xtdb.query/query-source)
             :db-cat (ig/ref :xtdb/db-catalog)
             :metrics-registry (ig/ref :xtdb.metrics/registry)
             :authn (ig/ref :xtdb/authn)}
            opts)})

(defmethod ig/init-key :xtdb/node [_ {:keys [metrics-registry config] :as deps}]
  (let [node (map->Node (-> deps
                            (dissoc :config)
                            (assoc :default-tz (:default-tz config)
                                   :!await-token (AtomicReference. nil))
                            (assoc :query-timer (metrics/add-timer metrics-registry "query.timer"
                                                                   {:description "indicates the timings for queries"})
                                   :query-error-counter (metrics/add-counter metrics-registry "query.error")
                                   :tx-error-counter (metrics/add-counter metrics-registry "tx.error"))))]

    (doto metrics-registry
      (metrics/add-gauge "node.tx.latestCompletedTxId"
                         (fn []
                           (-> (xtp/latest-completed-txs node)
                               (get-in ["xtdb" 0 :tx-id] -1))))

      (metrics/add-gauge "node.tx.latestSubmittedMsgId"
                         (fn []
                           (-> (xtp/latest-submitted-msg-ids node)
                               (get-in ["xtdb" 0] -1))))

      (metrics/add-gauge "node.tx.latestProcessedMsgId"
                         (fn []
                           (-> (xtp/latest-processed-msg-ids node)
                               (get-in ["xtdb" 0] -1))))

      (metrics/add-gauge "node.tx.lag.MsgId"
                         (fn []
                           (max (- (long (-> (xtp/latest-submitted-msg-ids node)
                                             (get-in ["xtdb" 0] -1)))
                                   (long (-> (xtp/latest-processed-msg-ids node)
                                             (get-in ["xtdb" 0] -1))))
                                0))))
    node))

(defmethod ig/halt-key! :xtdb/node [_ node]
  (util/try-close node))

(defmethod ig/expand-key :xtdb/modules [k modules]
  {k {:node (ig/ref :xtdb/node)
      :modules (vec modules)}})

(defmethod ig/init-key :xtdb/modules [_ {:keys [node modules]}]
  (util/with-close-on-catch [!started-modules (HashMap. (count modules))]
    (doseq [^XtdbModule$Factory module modules]
      (.put !started-modules (.getModuleKey module) (.openModule module node)))

    (into {} !started-modules)))

(defmethod ig/halt-key! :xtdb/modules [_ modules]
  (util/close modules))

(defn node-system [^Xtdb$Config opts]
  (let [srv-config (.getServer opts)
        healthz (.getHealthz opts)
        tracer (.getTracer opts)]
    (-> {:xtdb/node {}
         :xtdb/config opts
         :xtdb/allocator {}
         :xtdb/indexer {}
         :xtdb/information-schema {}
         :xtdb.operator.scan/scan-emitter {}
         :xtdb.query/query-source {}
         :xtdb/compactor (.getCompactor opts)
         :xtdb.metrics/registry {}
         :xtdb/tracer tracer
         :xtdb.log/clusters (.getLogClusters opts)
         :xtdb/db-catalog {}
         :xtdb/authn {:authn-factory (.getAuthn opts)}
         :xtdb.cache/memory (.getMemoryCache opts)
         :xtdb.cache/disk (.getDiskCache opts)
         :xtdb/garbage-collector (.getGarbageCollector opts)
         :xtdb/modules (.getModules opts)}
        (cond-> srv-config (assoc :xtdb.pgwire/server srv-config)
                healthz (assoc :xtdb/healthz healthz))
        (doto ig/load-namespaces))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-node ^xtdb.api.Xtdb [opts]
  (try
    (let [!closing (atom false)
          system (-> (node-system opts)
                     ig/expand
                     ig/init)]

      (-> (:xtdb/node system)
          (assoc :system system
                 :close-fn #(when (compare-and-set! !closing false true)
                              (ig/halt! system)
                              #_(println (.toVerboseString ^RootAllocator (:xtdb/allocator system)))))))
    (catch clojure.lang.ExceptionInfo e
      (try
        (ig/halt! (:system (ex-data e)))
        (catch Throwable t
          (let [^Throwable e (or (ex-cause e) e)]
            (throw (doto e (.addSuppressed t))))))
      (throw (ex-cause e)))))

(defrecord CompactorNode [system !closing?]
  Xtdb$CompactorNode
  (close [_]
    (when (compare-and-set! !closing? false true)
      (ig/halt! system))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-compactor ^xtdb.api.Xtdb$CompactorNode [opts]
  (let [system (-> (node-system opts) ig/expand (ig/init [:xtdb/compactor]))]
    (try
      (->CompactorNode system (atom false))
      (catch clojure.lang.ExceptionInfo e
        (ig/halt! system)
        (throw (ex-cause e))))))
