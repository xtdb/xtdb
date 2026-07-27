(ns ^{:clojure.tools.namespace.repl/load false, :clojure.tools.namespace.repl/unload false}
    xtdb.protocols
  (:require [xtdb.error :as err]
            [xtdb.serde :as serde]))

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
                                              (mapv #(serde/map->TxKey (select-keys % [:tx-id :system-time])) txs))))

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
