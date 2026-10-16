(ns ^{:clojure.tools.namespace.repl/load false, :clojure.tools.namespace.repl/unload false}
    xtdb.protocols
  (:require [xtdb.error :as err]
            [xtdb.serde :as serde]
            [xtdb.time :as time])
  (:import (clojure.lang IBlockingDeref IDeref IPending)
           (java.util.concurrent Callable ExecutionException Future FutureTask RunnableFuture TimeUnit TimeoutException)))

(defn- get-tx ^Future [^Future fut]
  ;; a remote tx's future runs its read on the first deref, on the dereffing thread
  (when (instance? RunnableFuture fut)
    (.run ^RunnableFuture fut))
  fut)

(deftype TxFuture [^Future fut ->tx-key]
  IDeref
  (deref [_]
    (err/wrap-anomaly {}
      (try
        (->tx-key (.get (get-tx fut)))
        (catch ExecutionException e
          (throw (or (.getCause e) e))))))

  IBlockingDeref
  (deref [_ ms timeout-val]
    (err/wrap-anomaly {}
      (try
        (->tx-key (.get (get-tx fut) ms TimeUnit/MILLISECONDS))
        (catch TimeoutException _ timeout-val)
        (catch ExecutionException e
          (throw (or (.getCause e) e))))))

  IPending
  (isRealized [_] (.isDone fut)))

(defn tx-future
  "A future of a submitted tx: deref yields `(->tx-key result)` once `fut` completes, raising what `execute-tx` would."
  [^Future fut ->tx-key]
  (->TxFuture fut ->tx-key))

(defn lazy-tx-future
  "A future of a submitted tx whose `(await-tx-key)` runs on its first deref."
  [await-tx-key]
  (->TxFuture (FutureTask. ^Callable await-tx-key) identity))

(defprotocol Connectable
  "How `xtdb.api` runs its operations against a connectable.

  The default impl (on `Object`, in `xtdb.api`) goes over JDBC/pgwire. An in-process
  `Xtdb.Connection` is driven natively, bypassing pgwire — that impl is extended in
  `xtdb.node.impl`, where the concrete connection type (and the core query/tx machinery) is visible.
  Keeping the protocol here lets `xtdb.api` (which can't see the core types) dispatch across the
  module boundary."
  (-plan-q ^clojure.lang.IReduceInit [connectable sql args opts])
  (-submit-tx [connectable tx-ops opts])
  (-execute-tx [connectable tx-ops opts])
  (-status [connectable]))

(defn await-tx-key
  "Reads a submitted tx's outcome through `conn`, once the await token says it's indexed.
  `src-msg-id` is the source position the tx was submitted at.
  Returns its TxKey, or throws its error as `execute-tx` would."
  [conn src-msg-id await-token]
  (let [read-tx (fn [where]
                  (first (into [] (-plan-q conn (str "SELECT _id, _system_from AS ingest_time, system_time, committed, error FROM xt.txs FOR ALL VALID_TIME WHERE " where)
                                           nil {:await-token await-token}))))
        {:keys [xt/id ingest-time system-time committed error] :as tx}
        (or (read-tx (str "src_msg_id = " src-msg-id))
            (read-tx (str "_id = " src-msg-id " AND src_msg_id IS NULL")))]
    (when-not tx
      (throw (err/fault :xtdb/tx-await-missing (str "tx " src-msg-id " not found after await"))))

    (when-not committed
      (throw (if (instance? Throwable error)
               error
               (err/fault :xtdb/tx-aborted "Transaction aborted" {:error error}))))

    (serde/->TxKey id (time/->instant ingest-time) (some-> system-time time/->instant))))

(defn build-status
  "Shapes a node's status out of the metrics tables and the `SHOW` statements behind it.

  Takes an open connection, so it can read through `-plan-q` directly: the impls that already have a
  connection share this, and the ones that don't (a node, a `DataSource`) open one and delegate to it."
  [conn]
  (let [status-q #(into [] (-plan-q conn % nil {}))]
    {:metrics (-> (concat (status-q "SELECT * FROM xt.metrics_counters")
                          (status-q "SELECT * FROM xt.metrics_timers")
                          (status-q "SELECT * FROM xt.metrics_gauges"))
                  (->> (group-by :name))
                  (update-vals (fn [metrics]
                                 (mapv #(dissoc % :name) metrics))))

     :latest-completed-txs (-> (status-q "SHOW LATEST_COMPLETED_TXS")
                               (->> (group-by :db-name))
                               (update-vals (fn [txs]
                                              (mapv #(serde/map->TxKey (select-keys % [:tx-id :ingest-time :system-time])) txs))))

     :latest-submitted-msg-ids (-> (status-q "SHOW LATEST_SUBMITTED_MSG_IDS")
                                   (->> (group-by :db-name))
                                   (update-vals #(mapv :msg-id %)))

     :latest-processed-msg-ids (-> (status-q "SHOW LATEST_PROCESSED_MSG_IDS")
                                   (->> (group-by :db-name))
                                   (update-vals #(mapv :msg-id %)))}))

(defn check-no-database
  "A connectable that's already bound to a database can't select one — only a node/DataSource can.
  Lives with the protocol because it's part of every impl's contract: a raw JDBC connection and an
  in-process `Xtdb.Connection` are both already bound, so both must reject `:database` rather than
  silently ignore it."
  [connectable {:keys [database]}]
  (when database
    (throw (err/incorrect :cannot-set-db "Can't set :default-db when connectable is not an XT node"
                          {:default-db database, :connectable-class (class connectable)}))))
