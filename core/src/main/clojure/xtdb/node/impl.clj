(ns xtdb.node.impl
  (:require [clojure.pprint :as pp]
            [integrant.core :as ig]
            [xtdb.antlr :as antlr]
            [xtdb.api :as api]
            [xtdb.error :as err]
            [xtdb.indexer :as idx]
            [xtdb.log :as xt-log]
            [xtdb.metrics :as metrics]
            [xtdb.protocols :as xtp]
            [xtdb.query :as q]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw]
            [xtdb.xtql :as xtql])
  (:import io.micrometer.core.instrument.composite.CompositeMeterRegistry
           io.micrometer.core.instrument.Counter
           (java.io Closeable Writer)
           (java.util.concurrent ExecutionException)
           java.util.HashMap
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (xtdb.antlr Sql$DirectlyExecutableStatementContext)
           (xtdb.api TransactionKey TransactionResult Xtdb Xtdb$Config)
           (xtdb.api.log Log)
           xtdb.api.module.XtdbModule$Factory
           (xtdb.api.query XtqlQuery)
           [xtdb.api.tx TxOp]
           (xtdb.indexer IIndexer LiveIndex LogProcessor)
           (xtdb.query IQuerySource PreparedQuery)))

(set! *unchecked-math* :warn-on-boxed)

(defmethod ig/prep-key :xtdb/config [_ ^Xtdb$Config opts]
  {:node-id (.getNodeId opts)
   :default-tz (.getDefaultTz opts)})

(defmethod ig/init-key :xtdb/config [_ cfg] cfg)

(defmethod ig/init-key :xtdb/allocator [_ _] (RootAllocator.))
(defmethod ig/halt-key! :xtdb/allocator [_ ^BufferAllocator a]
  (util/close a))

(defn- with-query-opts-defaults [query-opts {:keys [default-tz] :as node}]
  (-> (into {:default-tz default-tz,
             :after-tx-id (xtp/latest-submitted-tx-id node)
             :key-fn (serde/read-key-fn :snake-case-string)}
            query-opts)
      (update :snapshot-time #(some-> % (time/->instant)))
      (update :current-time #(some-> % (time/->instant)))))

(defn- validate-snapshot-not-before [snapshot-time ^TransactionKey latest-completed-tx]
  (when (and snapshot-time (or (nil? latest-completed-tx) (neg? (compare (.getSystemTime latest-completed-tx) snapshot-time))))
    (throw (err/illegal-arg :xtdb/unindexed-tx
                            {::err/message (format "snapshot-time (%s) is after the latest completed tx (%s)"
                                                   (pr-str snapshot-time) (pr-str latest-completed-tx))
                             :latest-completed-tx latest-completed-tx
                             :snapshot-time snapshot-time}))))

(defn- then-execute-prepared-query [^PreparedQuery prepared-query, allocator {:keys [args], :as query-opts} {:keys [query-timer] :as metrics}]
  (util/with-close-on-catch [bound-query (util/with-close-on-catch [args-rel (vw/open-args allocator args)]
                                           (.bind prepared-query (assoc query-opts :args args-rel)))]
    ;;TODO metrics only currently wrapping openQueryAsync results
    (-> (q/open-cursor-as-stream bound-query query-opts metrics)
        (metrics/wrap-query query-timer))))

(defn- ->TxOps [tx-ops]
  (->> tx-ops
       (mapv (fn [tx-op]
               (cond-> tx-op
                 (not (instance? TxOp tx-op)) tx-ops/parse-tx-op)))))

(defrecord Node [^BufferAllocator allocator
                 ^IIndexer indexer
                 ^LiveIndex live-idx
                 ^Log log
                 ^LogProcessor log-processor
                 ^IQuerySource q-src, scan-emitter
                 ^CompositeMeterRegistry metrics-registry
                 default-tz
                 system, close-fn,
                 query-timer
                 ^Counter query-error-counter
                 ^Counter tx-error-counter]
  Xtdb
  (getServerPort [this]
    (get-in (util/component this :xtdb.pgwire/server) [:read-write :port] -1))

  (getServerReadOnlyPort [this]
    (get-in (util/component this :xtdb.pgwire/server) [:read-only :port] -1))

  (addMeterRegistry [_ reg]
    (.add metrics-registry reg))

  (module [_ clazz]
    (->> (vals (:xtdb/modules system))
         (some #(when (instance? clazz %) %))))

  xtp/PNode
  (submit-tx [this tx-ops opts]
    (try
      @(xt-log/submit-tx& this (->TxOps tx-ops) opts)
      (catch ExecutionException e
        (throw (ex-cause e)))
      (catch IllegalArgumentException e
        (when tx-error-counter
          (.increment tx-error-counter))
        (throw e))))

  (execute-tx [this tx-ops opts]
    (let [tx-id (xtp/submit-tx this tx-ops opts)]
      (or (let [^TransactionResult tx-res (-> @(.awaitAsync log-processor tx-id)
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

  xtp/PStatus
  (latest-completed-tx [_] (.getLatestCompletedTx live-idx))
  (latest-submitted-tx-id [_] (.getLatestSubmittedOffset log))
  (status [this]
    {:latest-completed-tx (.getLatestCompletedTx live-idx)
     :latest-submitted-tx-id (xtp/latest-submitted-tx-id this)})

  xtp/PLocalNode
  (prepare-sql [this query query-opts]
    (let [ast (cond
                (instance? Sql$DirectlyExecutableStatementContext query) query
                (string? query) (antlr/parse-statement query)
                :else (throw (err/illegal-arg :xtdb/unsupported-query-type
                                              {::err/message (format "Unsupported SQL query type: %s" (type query))})))

          {:keys [snapshot-time ^long after-tx-id tx-timeout] :as query-opts} (-> query-opts (with-query-opts-defaults this))]

      (xt-log/await-tx this after-tx-id tx-timeout)

      (validate-snapshot-not-before snapshot-time (xtp/latest-completed-tx this))
      (let [plan (.planQuery q-src ast query-opts)]
        (.prepareRaQuery q-src plan query-opts))))

  (prepare-xtql [this query query-opts]
    (let [{:keys [snapshot-time ^long after-tx-id tx-timeout] :as query-opts} (-> query-opts (with-query-opts-defaults this))
          ast (cond
                (sequential? query) (xtql/parse-query query)
                (instance? XtqlQuery query) query
                :else (throw (err/illegal-arg :xtdb/unsupported-query-type
                                              {::err/message (format "Unsupported XTQL query type: %s" (type query))})))]
      (xt-log/await-tx this after-tx-id tx-timeout)
      (validate-snapshot-not-before snapshot-time (xtp/latest-completed-tx this))

      (let [plan (.planQuery q-src ast query-opts)]
        (.prepareRaQuery q-src plan query-opts))))

  (prepare-ra [this plan query-opts]
    (let [{:keys [snapshot-time ^long after-tx-id tx-timeout] :as query-opts} (-> query-opts (with-query-opts-defaults this))]
      (xt-log/await-tx this after-tx-id tx-timeout)
      (validate-snapshot-not-before snapshot-time (xtp/latest-completed-tx this))

      (.prepareRaQuery q-src plan query-opts)))

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method Node [_node ^Writer w] (.write w "#<XtdbNode>"))
(defmethod pp/simple-dispatch Node [it] (print-method it *out*))

(defmethod ig/prep-key :xtdb/node [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :indexer (ig/ref :xtdb/indexer)
          :live-idx (ig/ref :xtdb.indexer/live-index)
          :log (ig/ref :xtdb/log)
          :log-processor (ig/ref :xtdb.log/processor)
          :config (ig/ref :xtdb/config)
          :q-src (ig/ref :xtdb.query/query-source)
          :scan-emitter (ig/ref :xtdb.operator.scan/scan-emitter)
          :metrics-registry (ig/ref :xtdb.metrics/registry)
          :authn (ig/ref :xtdb/authn)}
         opts))

(defmethod ig/init-key :xtdb/node [_ {:keys [metrics-registry config] :as deps}]
  (let [node (map->Node (-> deps
                            (dissoc :config)
                            (assoc :default-tz (:default-tz config))
                            (assoc :query-timer (metrics/add-timer metrics-registry "query.timer"
                                                                   {:description "indicates the timings for queries"})
                                   :query-error-counter (metrics/add-counter metrics-registry "query.error")
                                   :tx-error-counter (metrics/add-counter metrics-registry "tx.error"))))]

    (doto metrics-registry
      (metrics/add-gauge "node.tx.latestSubmittedTxId" (fn [] (xtp/latest-submitted-tx-id node)))
      (metrics/add-gauge "node.tx.latestCompletedTxId" (fn [] (get-in (xtp/status node) [:latest-completed-tx :tx-id] -1)))
      (metrics/add-gauge "node.tx.lag.TxId"
                         (fn []
                           (let [{:keys [latest-completed-tx ^long latest-submitted-tx-id]} (xtp/status node)]
                             (if (and latest-completed-tx (> latest-submitted-tx-id ^long (:tx-id latest-completed-tx)))
                               (- latest-submitted-tx-id ^long (:tx-id latest-completed-tx))
                               0)))))
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
        healthz (.getHealthz opts)
        indexer-cfg (.getIndexer opts)]
    (-> {:xtdb/node {}
         :xtdb/config opts
         :xtdb/allocator {}
         :xtdb/indexer {}
         :xtdb/trie-catalog {}
         :xtdb.log/processor opts
         :xtdb.metadata/metadata-manager {}
         :xtdb.operator.scan/scan-emitter {}
         :xtdb.query/query-source {}
         :xtdb/compactor (.getCompactor opts)
         :xtdb.metrics/registry {}
         :xtdb/authn (.getAuthn opts)
         :xtdb/log (.getLog opts)
         :xtdb/buffer-pool (.getStorage opts)
         :xtdb.indexer/live-index indexer-cfg
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
      (ig/halt! (:system (ex-data e)))
      (throw (ex-cause e)))))
