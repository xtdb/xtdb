(ns xtdb.node.impl
  (:require [clojure.pprint :as pp]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as api]
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
  (:import (java.io Closeable Writer)
           (java.util.concurrent ExecutionException)
           java.util.HashMap
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (xtdb.api IXtdb TransactionKey Xtdb$Config)
           (xtdb.api.log Log)
           xtdb.api.module.XtdbModule$Factory
           (xtdb.api.query Basis XtqlQuery)
           [xtdb.api.tx TxOp TxOptions]
           xtdb.indexer.IIndexer
           (xtdb.query IQuerySource PreparedQuery)))

(set! *unchecked-math* :warn-on-boxed)

(defmethod ig/init-key :xtdb/allocator [_ _] (RootAllocator.))
(defmethod ig/halt-key! :xtdb/allocator [_ ^BufferAllocator a]
  (util/close a))

(defmethod ig/init-key :xtdb/default-tz [_ default-tz] default-tz)

(defn- with-after-tx-default [opts]
  (-> opts
      (update :after-tx time/max-tx (get-in opts [:basis :at-tx]))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IXtdbInternal
  (^xtdb.query.PreparedQuery prepareQuery [^java.lang.String query, query-opts])
  (^xtdb.query.PreparedQuery prepareQuery [^xtdb.api.query.XtqlQuery query, query-opts]))

(defn- mapify-query-opts-with-defaults [{:keys [basis] :as query-opts} default-tz latest-submitted-tx default-key-fn]
  ;;not all callers care about all defaulted query opts returned here
  (let [{:keys [at-tx] :as basis} (cond->> basis (instance? Basis basis) (into {}))]
    (-> (into {:default-tz default-tz,
               :after-tx (or at-tx latest-submitted-tx)
               :key-fn default-key-fn}
              query-opts)
        (assoc :basis basis)
        (with-after-tx-default))))

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
                 default-tz
                 !latest-submitted-tx
                 system, close-fn,
                 query-timer]
  IXtdb
  (getPgPort [this]
    (or (some-> (util/component this :xtdb/modules)
                (get "xtdb.pgwire-server")
                (:port))
        (throw (IllegalStateException. "No Postgres wire server running."))))

  (submitTx [this opts tx-ops]
    (xtp/submit-tx this tx-ops opts))

  (executeTx [this opts tx-ops]
    (xtp/execute-tx this tx-ops opts))

  (openQuery [this query query-opts]
    (xtp/open-sql-query this query query-opts))

  xtp/PNode
  (submit-tx [this tx-ops opts]
    (let [system-time (some-> ^TxOptions opts .getSystemTime)
          tx-key (try
                   @(log/submit-tx& this (->TxOps tx-ops) opts)
                   (catch ExecutionException e
                     (throw (ex-cause e))))
          tx-key (cond-> tx-key
                   system-time (assoc :system-time system-time))]

      (swap! !latest-submitted-tx time/max-tx tx-key)
      tx-key))

  (execute-tx [this tx-ops opts]
    (let [{:keys [tx-id] :as tx-key} (xtp/submit-tx this tx-ops opts)]
      (with-open [res (xtp/open-sql-query this "SELECT committed AS \"committed?\", error FROM xt.txs WHERE xt$id = ?"
                                          {:args [tx-id]
                                           :key-fn #xt/key-fn :kebab-case-keyword})]
        (let [{:keys [committed? error]} (-> (.findFirst res) (.orElse nil))]
          (if committed?
            (serde/->tx-committed tx-key)
            (serde/->tx-aborted tx-key error))))))

  (open-sql-query [this query query-opts]
    (let [query-opts (mapify-query-opts-with-defaults query-opts default-tz @!latest-submitted-tx #xt/key-fn :snake-case-string)]
      (-> (.prepareQuery this ^String query query-opts)
          (then-execute-prepared-query query-timer query-opts))))

  (open-xtql-query [this query query-opts]
    (let [query-opts (mapify-query-opts-with-defaults query-opts default-tz @!latest-submitted-tx #xt/key-fn :camel-case-string)]
      (-> (.prepareQuery this (xtql.edn/parse-query query) query-opts)
          (then-execute-prepared-query query-timer query-opts))) )

  xtp/PStatus
  (latest-submitted-tx [_] @!latest-submitted-tx)
  (status [this]
    {:latest-completed-tx (.latestCompletedTx indexer)
     :latest-submitted-tx (xtp/latest-submitted-tx this)})

  IXtdbInternal
  (^PreparedQuery prepareQuery [_ ^String query, query-opts]
   (let [{:keys [after-tx tx-timeout] :as query-opts}
         (mapify-query-opts-with-defaults query-opts default-tz @!latest-submitted-tx #xt/key-fn :snake-case-string)]
     (.awaitTx indexer after-tx tx-timeout)
     (let [plan (.planQuery q-src query wm-src query-opts)]
       (.prepareRaQuery q-src plan wm-src query-opts))))

  (^PreparedQuery prepareQuery [_ ^XtqlQuery query, query-opts]
   (let [{:keys [after-tx tx-timeout] :as query-opts}
         (mapify-query-opts-with-defaults query-opts default-tz @!latest-submitted-tx #xt/key-fn :camel-case-string)]
     (.awaitTx indexer after-tx tx-timeout)

     (let [plan (.planQuery q-src query wm-src query-opts)]
       (.prepareRaQuery q-src plan wm-src query-opts))))

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

(defn gauge-lag-secs-fn [node]
  (fn []
    (let [{:keys [^TransactionKey latest-completed-tx
                  ^TransactionKey latest-submitted-tx]} (xtp/status node)]
      (if (and latest-completed-tx latest-submitted-tx)
        (let [completed-tx-time (.getSystemTime latest-completed-tx)
              submitted-tx-time (.getSystemTime latest-submitted-tx)]
          (/ (- ^long (inst-ms submitted-tx-time) ^long (inst-ms completed-tx-time)) (long 1e3)))
        0.0))))

(defmethod ig/init-key :xtdb/node [_ {:keys [metrics-registry] :as deps}]
  (let [node (map->Node (-> deps
                            (assoc :!latest-submitted-tx (atom nil))
                            (assoc :query-timer (metrics/add-timer metrics-registry "query.timer"
                                                                   {:description "indicates the timings for queries"}))))]
    ;; TODO seems to create heap memory pressure, disabled for now
    #_(metrics/add-gauge registry "node.tx.lag.seconds"
                         (gauge-lag-secs-fn node))
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
  (-> {:xtdb/node {}
       :xtdb/allocator {}
       :xtdb/indexer {}
       :xtdb.log/watcher {}
       :xtdb.metadata/metadata-manager {}
       :xtdb.operator.scan/scan-emitter {}
       :xtdb.query/query-source {}
       :xtdb/compactor {}

       :xtdb.metrics/registry (.getMetrics opts)
       :xtdb/buffer-pool (.getStorage opts)
       :xtdb.indexer/live-index (.indexer opts)
       :xtdb/log (.getTxLog opts)
       :xtdb/modules (.getModules opts)
       :xtdb/default-tz (.getDefaultTz opts)
       :xtdb.stagnant-log-flusher/flusher (.indexer opts)}
      (doto ig/load-namespaces)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-node ^xtdb.api.IXtdb [opts]
  (let [!closing (atom false)
        system (-> (node-system opts)
                   ig/prep
                   ig/init)]

    (-> (:xtdb/node system)
        (assoc :system system
               :close-fn #(when (compare-and-set! !closing false true)
                            (ig/halt! system)
                            #_(println (.toVerboseString ^RootAllocator (:xtdb/allocator system))))))))
