(ns xtdb.node.impl
  (:require [clojure.pprint :as pp]
            [integrant.core :as ig]
            [xtdb.adbc :as adbc]
            [xtdb.basis :as basis]
            [xtdb.error :as err]
            [xtdb.garbage-collector]
            [xtdb.metrics :as metrics]
            [xtdb.protocols :as xtp]
            [xtdb.sql :as sql]
            [xtdb.tracer]
            [xtdb.util :as util])
  (:import (clojure.lang IReduceInit)
           io.micrometer.core.instrument.composite.CompositeMeterRegistry
           (java.io Closeable Writer)
           (java.time InstantSource)
           (java.util HashMap)
           [java.util.concurrent.atomic AtomicReference]
           (org.apache.arrow.memory BufferAllocator)
           xtdb.NodeBase
           xtdb.XtdbInternal
           (xtdb.api DataSource Xtdb Xtdb$CompactorNode Xtdb$Config Xtdb$Connection)
           (xtdb.api.metrics ConnectionMetrics Healthz)
           xtdb.api.module.XtdbModule$Factory
           (xtdb.database Database$Catalog)
           (xtdb.query IQuerySource SqlPlanner)
           xtdb.util.NormalForm))

(set! *unchecked-math* :warn-on-boxed)

;; the in-process ADBC path for `xtdb.api` — driven natively, bypassing pgwire. Extended here (not in
;; `xtdb.api`) because this is where the connection type and the core query/tx machinery are visible;
;; loaded whenever a node (and hence a connection) can exist.

;; opens a connection to the op's `:database` (the connection carries the routing, so the ADBC
;; bodies read/write against it) — defaulting to the node's default db.
(defn- connect-node ^Xtdb$Connection [^Xtdb node {:keys [database]}]
  (if database
    (let [^String db-name (cond-> database (keyword? database) (-> symbol str NormalForm/normalForm))]
      ;; validate up-front so an unknown db surfaces the same on the read path as the write path (the
      ;; connection's db() only checks on submit, so a read would otherwise fail later as 'table not found')
      (when-not (.contains (.getDatabaseNames node) db-name)
        (throw (err/incorrect :xtdb/unknown-db (str "Unknown database: " db-name) {:db-name db-name})))
      (.connect node db-name))
    (.connect node)))

;; threads the node's await-basis onto the per-op connection (and, for writes, back off it) so a read
;; sees this node's prior writes — the read-your-writes the DataSource/pgwire path gets from the same
;; token.
(defn- with-node-conn [^Xtdb node opts f]
  (with-open [conn (connect-node node opts)]
    (some->> (.getAwaitToken node) (.setAwaitToken conn))
    (let [res (f conn)]
      (.setAwaitToken node (.getAwaitToken conn))
      res)))

(extend-protocol xtp/Connectable
  ;; a connection is already bound to its database, so it rejects `:database` as the JDBC impls do —
  ;; only the node (below) can route by it.
  Xtdb$Connection
  (-plan-q [conn sql args opts]
    (xtp/check-no-database conn opts)
    (adbc/plan-q conn sql args opts))

  (-submit-tx [conn tx-ops opts]
    (xtp/check-no-database conn opts)
    (adbc/submit-tx conn tx-ops opts))

  (-execute-tx [conn tx-ops opts]
    (xtp/check-no-database conn opts)
    (adbc/execute-tx conn tx-ops opts))
  (-status [conn] (xtp/build-status conn))

  Xtdb
  ;; a node is more specific than the `DataSource` (pgwire) impl, so it wins dispatch — while an
  ;; `xt/client` DataSource, which isn't an `Xtdb`, stays on pgwire.
  (-plan-q [node sql args opts]
    (reify IReduceInit
      (reduce [_ f start]
        (with-open [conn (connect-node node opts)]
          (some->> (.getAwaitToken node) (.setAwaitToken conn))
          (reduce f start (adbc/plan-q conn sql args opts))))))

  (-submit-tx [node tx-ops opts] (with-node-conn node opts #(adbc/submit-tx % tx-ops opts)))
  (-execute-tx [node tx-ops opts] (with-node-conn node opts #(adbc/execute-tx % tx-ops opts)))

  ;; deliberately without the node's await-token, so status reports where the node is now rather than
  ;; waiting on a tx someone else submitted through it.
  (-status [node]
    (with-open [conn (.connect node)]
      (xtp/-status conn))))

(defmethod ig/expand-key :xtdb/base [k ^Xtdb$Config config]
  {k {:config config}})

(defmethod ig/init-key :xtdb/base [_ {:keys [^Xtdb$Config config]}]
  (NodeBase/open config))

(defmethod ig/halt-key! :xtdb/base [_ ^NodeBase base]
  (.close base))

(defn- ->node-connection ^Xtdb$Connection [^Database$Catalog db-cat ^BufferAllocator allocator
                                           ^IQuerySource q-src ^SqlPlanner sql-planner query-tracer default-tz
                                           ^ConnectionMetrics conn-metrics db-name]
  (Xtdb$Connection. allocator db-cat q-src sql-planner db-name (InstantSource/system) default-tz query-tracer conn-metrics))

(defrecord Node [^BufferAllocator allocator, ^Database$Catalog db-cat
                 ^IQuerySource q-src ^SqlPlanner sql-planner
                 ^CompositeMeterRegistry metrics-registry
                 default-tz, ^AtomicReference !await-token
                 system, close-fn,
                 ^ConnectionMetrics conn-metrics query-tracer]
  Xtdb
  (getAllocator [_] allocator)

  (getServerPort [this]
    (get-in (util/component this :xtdb.pgwire/server) [:read-write :port] -1))

  (getServerReadOnlyPort [this]
    (get-in (util/component this :xtdb.pgwire/server) [:read-only :port] -1))

  (getFlightSqlPort [this]
    (if-let [fsql (util/component this :xtdb.flight-sql/server)]
      (.getPort ^xtdb.api.FlightSql fsql)
      -1))

  (getHealthzPort [this]
    (if-let [healthz (util/component this :xtdb/healthz)]
      (.getPort ^Healthz healthz)
      -1))

  (getDatabaseNames [_] (.getDatabaseNames db-cat))

  (latestCompletedTxs [_] (.latestCompletedTxs db-cat))

  (createConnectionBuilder [this]
    (let [server (util/component this :xtdb.pgwire/server)
          ^DataSource data-source (or (:read-write server) (:read-only server))]
      (when-not data-source
        (throw (err/incorrect ::pgwire-not-enabled "Cannot create JDBC connection: pgwire server is not enabled")))
      (.createConnectionBuilder data-source)))

  (connect [this] (.connect this "xtdb"))

  (connect [_ db-name]
    (->node-connection db-cat allocator q-src sql-planner query-tracer default-tz conn-metrics db-name))

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

  (submitTx [this tx-ops tx-opts]
    (with-open [conn (.connect this (or (.getDbName tx-opts) "xtdb"))]
      (.submitTx conn tx-ops tx-opts)))

  (executeTx [this tx-ops tx-opts]
    (with-open [conn (.connect this (or (.getDbName tx-opts) "xtdb"))]
      (.executeTx conn tx-ops tx-opts)))

  XtdbInternal
  (getDbCatalog [_] db-cat)

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method Node [_node ^Writer w] (.write w "#<XtdbNode>"))
(defmethod pp/simple-dispatch Node [it] (print-method it *out*))

(defmethod ig/expand-key :xtdb/node [k opts]
  {k (merge {:base (ig/ref :xtdb/base)
             :db-cat (ig/ref :xtdb/db-catalog)
             :authn (ig/ref :xtdb/authn)}
            opts)})

(defn- ->conn-metrics ^ConnectionMetrics [metrics-registry]
  (ConnectionMetrics. (metrics/add-timer metrics-registry "query.timer"
                                         {:description "indicates the timings for queries"})
                      (metrics/add-counter metrics-registry "query.error")
                      (metrics/add-counter metrics-registry "tx.error")
                      (metrics/add-timer metrics-registry "node.tx.await"
                                         {:description "Time spent in executeTx waiting for the indexer to catch up to a just-submitted tx (sync-path indexer await)."})
                      (metrics/add-timer metrics-registry "node.tx.submit"
                                         {:description "Time spent in submitTx (async-path log append + ack), across all frontends."})
                      (metrics/add-timer metrics-registry "node.tx.execute"
                                         {:description "Time spent in executeTx (sync-path log append + indexer await), across all frontends."})))

(defmethod ig/init-key :xtdb/node [_ {:keys [^NodeBase base] :as deps}]
  (let [metrics-registry (.getMeterRegistry base)]
    (map->Node (-> deps
                   (dissoc :base)
                   (assoc :allocator (.getAllocator base)
                          :q-src (.getQuerySource base)
                          :sql-planner (sql/->sql-planner)
                          :metrics-registry metrics-registry
                          :default-tz (.getDefaultTz (.getConfig base))
                          :!await-token (AtomicReference. nil)

                          ;; the query/tx meters every connection records into - the connection owns them, so
                          ;; they fire for every frontend alike
                          :conn-metrics (->conn-metrics metrics-registry)

                          ;; query tracer gated by config, threaded into every connection's reads — the
                          ;; connection owns query tracing, so it applies across all frontends alike
                          :query-tracer (when (.getQueryTracing (.getTracer (.getConfig base)))
                                          (.getTracer base)))))))

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
        flight-sql-config (.getFlightSql opts)
        healthz (.getHealthz opts)]
    (-> {:xtdb/node {}
         :xtdb/base opts
         :xtdb/db-catalog {}
         :xtdb/authn {:authn-factory (.getAuthn opts)}
         :xtdb/modules (.getModules opts)}
        (cond-> srv-config (assoc :xtdb.pgwire/server srv-config)
                flight-sql-config (assoc :xtdb.flight-sql/server flight-sql-config)
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
                              (ig/halt! system)))))
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
  (let [system (-> (node-system opts) ig/expand (ig/init [:xtdb/base]))]
    (try
      (->CompactorNode system (atom false))
      (catch clojure.lang.ExceptionInfo e
        (ig/halt! system)
        (throw (ex-cause e))))))
