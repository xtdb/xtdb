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
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw]
            [xtdb.xtql :as xtql])
  (:import io.micrometer.core.instrument.composite.CompositeMeterRegistry
           io.micrometer.core.instrument.Counter
           (java.io Closeable Writer)
           (java.util HashMap)
           [java.util.concurrent.atomic AtomicReference]
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (xtdb.antlr Sql$DirectlyExecutableStatementContext)
           (xtdb.api DataSource TransactionResult Xtdb Xtdb$CompactorNode Xtdb$Config Xtdb$XtdbInternal)
           xtdb.api.module.XtdbModule$Factory
           (xtdb.api.query XtqlQuery)
           (xtdb.database Database$Catalog)
           xtdb.error.Anomaly
           (xtdb.query IQuerySource PreparedQuery)))

(set! *unchecked-math* :warn-on-boxed)

(defmethod ig/prep-key :xtdb/config [_ ^Xtdb$Config config]
  {:config config
   :node-id (.getNodeId config)
   :default-tz (.getDefaultTz config)})

(defmethod ig/init-key :xtdb/config [_ cfg] cfg)

(defmethod ig/init-key :xtdb/allocator [_ _] (RootAllocator.))
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

(defrecord Node [^BufferAllocator allocator, ^Database$Catalog db-cat
                 ^IQuerySource q-src
                 ^CompositeMeterRegistry metrics-registry
                 default-tz, ^AtomicReference !await-token
                 system, close-fn,
                 query-timer, ^Counter query-error-counter, ^Counter tx-error-counter]
  Xtdb
  (getServerPort [this]
    (get-in (util/component this :xtdb.pgwire/server) [:read-write :port] -1))

  (getServerReadOnlyPort [this]
    (get-in (util/component this :xtdb.pgwire/server) [:read-only :port] -1))

  (createConnectionBuilder [this]
    (let [server (util/component this :xtdb.pgwire/server)
          ^DataSource data-source (or (:read-write server) (:read-only server))]
      (.createConnectionBuilder data-source)))

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
    (let [tx-id (xtp/submit-tx this tx-ops opts)]
      (or (let [db (.databaseOrNull db-cat (:default-db opts))
                ^TransactionResult tx-res (-> @(.awaitAsync (.getLogProcessor db) tx-id)
                                              (util/rethrowing-cause))]
            (when (and tx-res
                       (= (.getTxId tx-res) tx-id))
              tx-res))

          (with-open [res (xtp/open-sql-query this "SELECT system_time, committed AS \"committed?\", error FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?"
                                              {:args [tx-id]
                                               :key-fn (serde/read-key-fn :kebab-case-keyword)})]
            (let [{:keys [system-time committed? error]} (-> (.findFirst res) (.orElse nil))
                  system-time (time/->instant system-time)]
              (if committed?
                (serde/->tx-committed tx-id system-time)
                (serde/->tx-aborted tx-id system-time error)))))))

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

  (open-xtql-query [this query query-opts]
    (let [query-opts (-> query-opts (with-query-opts-defaults this))]
      (try
        (-> (xtp/prepare-xtql this query query-opts)
            (then-execute-prepared-query allocator query-opts {:query-timer query-timer :query-error-counter query-error-counter}))
        (catch Exception e
          (when query-error-counter
            (.increment query-error-counter))
          (throw e)))))

  (attach-db [this db-name db-config]
    (let [primary-db (.getPrimary db-cat)
          msg-id (xt-log/send-attach-db! primary-db db-name db-config)]
      (or (let [^TransactionResult tx-res (-> @(.awaitAsync (.getLogProcessor primary-db) msg-id)
                                              (util/rethrowing-cause))]
            (when (and tx-res
                       (= (.getTxId tx-res) msg-id))
              tx-res))

          (with-open [res (xtp/open-sql-query this "SELECT system_time, committed AS \"committed?\", error FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?"
                                              {:args [msg-id]
                                               :key-fn (serde/read-key-fn :kebab-case-keyword)})]
            (let [{:keys [system-time committed? error]} (-> (.findFirst res) (.orElse nil))
                  system-time (time/->instant system-time)]
              (if committed?
                (serde/->tx-committed msg-id system-time)
                (serde/->tx-aborted msg-id system-time error)))))))

  xtp/PStatus
  (latest-completed-txs [_]
    (->> (.getDatabaseNames db-cat)
         (into {} (map (fn [db-name]
                         ;; TODO multi-part
                         [db-name [(-> (.databaseOrNull db-cat db-name)
                                       (.getLiveIndex)
                                       (.getLatestCompletedTx))]])))))

  (latest-submitted-tx-ids [_]
    (->> (.getDatabaseNames db-cat)
         (into {} (map (fn [db-name]
                         ;; TODO multi-part
                         [db-name [(-> (.databaseOrNull db-cat db-name)
                                       (.getLogProcessor)
                                       (.getLatestSubmittedMsgId))]])))))

  (await-token [this]
    (basis/->tx-basis-str (xtp/latest-submitted-tx-ids this)))

  (snapshot-token [this]
    (basis/->time-basis-str (-> (xtp/latest-completed-txs this)
                                (update-vals #(mapv :system-time %)))))
  
  (status [this]
    {:latest-completed-txs (xtp/latest-completed-txs this)
     :latest-submitted-tx-ids (xtp/latest-submitted-tx-ids this)

     :await-token (xtp/await-token this)})

  xtp/PLocalNode
  (prepare-sql [this query query-opts]
    (let [ast (cond
                (instance? Sql$DirectlyExecutableStatementContext query) query
                (string? query) (antlr/parse-statement query)
                :else (throw (err/illegal-arg :xtdb/unsupported-query-type
                                              {::err/message (format "Unsupported SQL query type: %s" (type query))})))

          {:keys [await-token tx-timeout] :as query-opts} (-> query-opts (with-query-opts-defaults this))]

      (.awaitAll db-cat await-token tx-timeout)

      (.prepareQuery q-src ast db-cat query-opts)))

  (prepare-xtql [this query query-opts]
    (let [{:keys [await-token tx-timeout] :as query-opts} (-> query-opts (with-query-opts-defaults this))
          ast (cond
                (sequential? query) (xtql/parse-query query nil)
                (instance? XtqlQuery query) query
                :else (throw (err/illegal-arg :xtdb/unsupported-query-type
                                              {::err/message (format "Unsupported XTQL query type: %s" (type query))})))]

      (.awaitAll db-cat await-token tx-timeout)

      (.prepareQuery q-src ast db-cat query-opts)))

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

(defmethod ig/prep-key :xtdb/node [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :config (ig/ref :xtdb/config)
          :q-src (ig/ref :xtdb.query/query-source)
          :db-cat (ig/ref :xtdb/db-catalog)
          :metrics-registry (ig/ref :xtdb.metrics/registry)
          :authn (ig/ref :xtdb/authn)}
         opts))

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
      (metrics/add-gauge "node.tx.latestSubmittedTxId"
                         (fn []
                           (-> (xtp/latest-submitted-tx-ids node)
                               (get-in ["xtdb" 0] -1))))

      (metrics/add-gauge "node.tx.latestCompletedTxId"
                         (fn []
                           (-> (xtp/latest-completed-txs node)
                               (get-in ["xtdb" 0 :tx-id] -1))))

      (metrics/add-gauge "node.tx.lag.TxId"
                         (fn []
                           (max (- (long (-> (xtp/latest-submitted-tx-ids node)
                                             (get-in ["xtdb" 0] -1)))
                                   (long (-> (xtp/latest-completed-txs node)
                                             (get-in ["xtdb" 0 :tx-id] -1))))
                                0))))
    node))

(defmethod ig/halt-key! :xtdb/node [_ node]
  (util/try-close node))

(defmethod ig/prep-key :xtdb/modules [_ modules]
  {:node (ig/ref :xtdb/node)
   :modules (vec modules)})

(defmethod ig/init-key :xtdb/modules [_ {:keys [node modules]}]
  (util/with-close-on-catch [!started-modules (HashMap. (count modules))]
    (doseq [^XtdbModule$Factory module modules]
      (.put !started-modules (.getModuleKey module) (.openModule module node)))

    (into {} !started-modules)))

(defmethod ig/halt-key! :xtdb/modules [_ modules]
  (util/close modules))

(defn node-system [^Xtdb$Config opts]
  (let [srv-config (.getServer opts)
        healthz (.getHealthz opts)]
    (-> {:xtdb/node {}
         :xtdb/config opts
         :xtdb/allocator {}
         :xtdb/indexer {}
         :xtdb/information-schema {}
         :xtdb.operator.scan/scan-emitter {}
         :xtdb.query/query-source {}
         :xtdb/compactor (.getCompactor opts)
         :xtdb.metrics/registry {}
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
                     ig/prep
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
  (let [system (-> (node-system opts) ig/prep (ig/init [:xtdb/compactor]))]
    (try
      (->CompactorNode system (atom false))
      (catch clojure.lang.ExceptionInfo e
        (ig/halt! system)
        (throw (ex-cause e))))))
