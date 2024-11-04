(ns xtdb.node.impl
  (:require [clojure.pprint :as pp]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.antlr :as antlr]
            [xtdb.api :as api]
            [xtdb.error :as err]
            xtdb.indexer
            [xtdb.log :as log]
            [xtdb.metrics :as metrics]
            [xtdb.protocols :as xtp]
            [xtdb.query :as q]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.util :as util]
            [xtdb.xtql.edn :as xtql.edn])
  (:import io.micrometer.core.instrument.composite.CompositeMeterRegistry
           (java.io Closeable Writer)
           (java.util.concurrent ExecutionException)
           java.util.HashMap
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (xtdb.antlr Sql$DirectlyExecutableStatementContext)
           (xtdb.api TransactionKey Xtdb Xtdb$Config)
           (xtdb.api.log Log)
           xtdb.api.module.XtdbModule$Factory
           (xtdb.api.query XtqlQuery)
           [xtdb.api.tx TxOp]
           xtdb.indexer.IIndexer
           (xtdb.query IQuerySource PreparedQuery)))

(set! *unchecked-math* :warn-on-boxed)

(defmethod ig/init-key :xtdb/allocator [_ _] (RootAllocator.))
(defmethod ig/halt-key! :xtdb/allocator [_ ^BufferAllocator a]
  (util/close a))

(defmethod ig/init-key :xtdb/default-tz [_ default-tz] default-tz)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IXtdbInternal
  (^xtdb.query.PreparedQuery prepareQuery [^java.lang.String query, query-opts])
  (^xtdb.query.PreparedQuery prepareQuery [^xtdb.antlr.Sql$DirectlyExecutableStatementContext parsed-query, query-opts])
  (^xtdb.query.PreparedQuery prepareQuery [^xtdb.api.query.XtqlQuery query, query-opts])
  (^xtdb.query.PreparedQuery prepareRaQuery [ra-plan query-opts]))

(defn- with-query-opts-defaults [query-opts {:keys [default-tz !latest-submitted-tx-id]}]
  (-> (into {:default-tz default-tz,
             :after-tx-id @!latest-submitted-tx-id
             :key-fn (serde/read-key-fn :snake-case-string)}
            query-opts)
      (update :snapshot-time #(some-> % (time/->instant)))
      (update :current-time #(some-> % (time/->instant)))))

(defn- validate-snapshot-not-before [^TransactionKey latest-completed-tx snapshot-time]
  (when (and snapshot-time (or (nil? latest-completed-tx) (neg? (compare (.getSystemTime latest-completed-tx) snapshot-time))))
    (throw (err/illegal-arg :xtdb/unindexed-tx
                            {::err/message (format "snapshot-time (%s) is after the latest completed tx (%s)"
                                                   (pr-str snapshot-time) (pr-str latest-completed-tx))
                             :latest-completed-tx latest-completed-tx
                             :snapshot-time snapshot-time}))))

(defn- then-execute-prepared-query [^PreparedQuery prepared-query, query-timer query-opts]
  (let [bound-query (.bind prepared-query query-opts)]
    ;;TODO metrics only currently wrapping openQueryAsync results
    (-> (q/open-cursor-as-stream bound-query query-opts)
        (metrics/wrap-query query-timer))))

(defn- ->TxOps [tx-ops]
  (->> tx-ops
       (mapv (fn [tx-op]
               (cond-> tx-op
                 (not (instance? TxOp tx-op)) tx-ops/parse-tx-op)))))

(defrecord Node [^BufferAllocator allocator
                 ^IIndexer indexer
                 ^Log log
                 ^IQuerySource q-src, wm-src, scan-emitter
                 ^CompositeMeterRegistry metrics-registry
                 default-tz
                 !latest-submitted-tx-id
                 system, close-fn,
                 query-timer]
  Xtdb
  (getServerPort [this]
    (or (:port (util/component this :xtdb.pgwire/server))
        (throw (IllegalStateException. "No Postgres wire server running."))))

  (addMeterRegistry [_ reg]
    (.add metrics-registry reg))

  (module [_ clazz]
    (->> (vals (:xtdb/modules system))
         (some #(when (instance? clazz %) %))))

  xtp/PNode
  (submit-tx [this tx-ops opts]
    (let [^long tx-id (try
                        @(log/submit-tx& this (->TxOps tx-ops) opts)
                        (catch ExecutionException e
                          (throw (ex-cause e))))]

      (swap! !latest-submitted-tx-id (fnil max tx-id) tx-id)
      tx-id))

  (execute-tx [this tx-ops opts]
    (let [tx-id (xtp/submit-tx this tx-ops opts)]
      (with-open [res (xtp/open-sql-query this "SELECT system_time, committed AS \"committed?\", error FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?"
                                          {:args [tx-id]
                                           :key-fn (serde/read-key-fn :kebab-case-keyword)})]
        (let [{:keys [system-time committed? error]} (-> (.findFirst res) (.orElse nil))
              system-time (time/->instant system-time)]
          (if committed?
            (serde/->tx-committed tx-id system-time)
            (serde/->tx-aborted tx-id system-time error))))))

  (open-sql-query [this query query-opts]
    (let [query-opts (-> query-opts (with-query-opts-defaults this))]
      (-> (.prepareQuery this ^String query query-opts)
          (then-execute-prepared-query query-timer query-opts))))

  (open-xtql-query [this query query-opts]
    (let [query-opts (-> query-opts (with-query-opts-defaults this))]
      (-> (.prepareQuery this (xtql.edn/parse-query query) query-opts)
          (then-execute-prepared-query query-timer query-opts))) )

  xtp/PStatus
  (latest-submitted-tx-id [_] @!latest-submitted-tx-id)
  (status [this]
    {:latest-completed-tx (.latestCompletedTx indexer)
     :latest-submitted-tx-id (xtp/latest-submitted-tx-id this)})

  IXtdbInternal
  (^PreparedQuery prepareQuery [this ^String query, query-opts]
   (.prepareQuery this (antlr/parse-statement query)
                  query-opts))

  (^PreparedQuery prepareQuery [this ^Sql$DirectlyExecutableStatementContext parsed-query, query-opts]
   (let [{:keys [snapshot-time ^long after-tx-id tx-timeout] :as query-opts} (-> query-opts (with-query-opts-defaults this))]
     (doto (.awaitTx indexer after-tx-id tx-timeout)
       (validate-snapshot-not-before snapshot-time))
     (let [plan (.planQuery q-src parsed-query wm-src query-opts)]
       (.prepareRaQuery q-src plan wm-src query-opts))))

  (^PreparedQuery prepareQuery [this ^XtqlQuery query, query-opts]
   (let [{:keys [snapshot-time ^long after-tx-id tx-timeout] :as query-opts} (-> query-opts (with-query-opts-defaults this))]
     (doto (.awaitTx indexer after-tx-id tx-timeout)
       (validate-snapshot-not-before snapshot-time))

     (let [plan (.planQuery q-src query wm-src query-opts)]
       (.prepareRaQuery q-src plan wm-src query-opts))))

  (prepareRaQuery [this plan query-opts]
    (let [{:keys [snapshot-time ^long after-tx-id tx-timeout] :as query-opts} (-> query-opts (with-query-opts-defaults this))]
      (doto (.awaitTx indexer after-tx-id tx-timeout)
        (validate-snapshot-not-before snapshot-time))

     (.prepareRaQuery q-src plan wm-src query-opts)))

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method Node [_node ^Writer w] (.write w "#<XtdbNode>"))
(defmethod pp/simple-dispatch Node [it] (print-method it *out*))

(defmethod ig/prep-key :xtdb/node [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :indexer (ig/ref :xtdb/indexer)
          :wm-src (ig/ref :xtdb/indexer)
          :log (ig/ref :xtdb/log)
          :default-tz (ig/ref :xtdb/default-tz)
          :q-src (ig/ref :xtdb.query/query-source)
          :scan-emitter (ig/ref :xtdb.operator.scan/scan-emitter)
          :metrics-registry (ig/ref :xtdb.metrics/registry)}
         opts))

(defmethod ig/init-key :xtdb/node [_ {:keys [metrics-registry] :as deps}]
  (let [node (map->Node (-> deps
                            (assoc :!latest-submitted-tx-id (atom -1)
                                   :query-timer (metrics/add-timer metrics-registry "query.timer"
                                                                   {:description "indicates the timings for queries"}))))]
    (metrics/add-gauge metrics-registry "node.tx.latestSubmittedTxId" (fn [] (xtp/latest-submitted-tx-id node)))
    (metrics/add-gauge metrics-registry "node.tx.latestCompletedTxId" (fn [] (get-in (xtp/status node) [:latest-completed-tx :tx-id] 0)))
    (metrics/add-gauge metrics-registry "node.tx.lag.TxId" (fn []
                                                            (let [{:keys [latest-completed-tx ^long latest-submitted-tx-id]} (xtp/status node)]
                                                              (if (and latest-completed-tx (pos? latest-submitted-tx-id))
                                                                (- latest-submitted-tx-id ^long (:tx-id latest-completed-tx))
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
         :xtdb/allocator {}
         :xtdb/indexer {}
         :xtdb.log/watcher {}
         :xtdb.metadata/metadata-manager {}
         :xtdb.operator.scan/scan-emitter {}
         :xtdb.query/query-source {}
         :xtdb/compactor (.getCompactor opts)
         :xtdb.metrics/registry {}

         :xtdb/log (.getTxLog opts)
         :xtdb/buffer-pool (.getStorage opts)
         :xtdb.indexer/live-index (.getIndexer opts)
         :xtdb/modules (.getModules opts)
         :xtdb/default-tz (.getDefaultTz opts)
         :xtdb.stagnant-log-flusher/flusher (.getIndexer opts)}
        (cond-> srv-config (assoc :xtdb.pgwire/server srv-config)
                healthz (assoc :xtdb/healthz healthz))
        (doto ig/load-namespaces))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-node ^xtdb.api.Xtdb [opts]
  (let [!closing (atom false)
        system (-> (node-system opts)
                   ig/prep
                   ig/init)]

    (-> (:xtdb/node system)
        (assoc :system system
               :close-fn #(when (compare-and-set! !closing false true)
                            (ig/halt! system)
                            #_(println (.toVerboseString ^RootAllocator (:xtdb/allocator system))))))))
