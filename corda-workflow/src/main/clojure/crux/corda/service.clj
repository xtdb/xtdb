(ns crux.corda.service
  (:require [crux.api :as crux]
            [crux.tx :as tx]
            [crux.db :as db]
            [crux.codec :as c]
            [crux.system :as sys]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as jdbcr]
            [clojure.set :as set]
            [crux.io :as cio])
  (:import (crux.corda.contract CruxState)
           (crux.api ICursor)
           (net.corda.core.crypto SecureHash)
           (net.corda.core.node AppServiceHub)
           (net.corda.core.transactions SignedTransaction)
           (net.corda.core.contracts TransactionState StateAndRef)))

(set! *warn-on-reflection* true)

(defprotocol SQLDialect
  (db-type [_])
  (setup-tx-schema! [_ jdbc-session]))

(comment
  (import '(net.corda.testing.node MockNetwork MockNetworkParameters TestCordapp)
          '(com.example.workflow ExampleFlow$Initiator)
          '(net.corda.core.crypto SecureHash))

  (do
    (defonce ^MockNetwork network
      (MockNetwork. (MockNetworkParameters. [(TestCordapp/findCordapp "crux.corda.contract")
                                             (TestCordapp/findCordapp "crux.corda.workflow")
                                             (TestCordapp/findCordapp "com.example.contract")
                                             (TestCordapp/findCordapp "com.example.workflow")])))
    (defonce node-a
      (.createPartyNode network nil))

    (defonce node-b
      (.createPartyNode network nil))

    (def b-party
      (-> (.getLegalIdentitiesAndCerts (.getInfo node-b))
          first
          .getParty)))

  (let [fut (.startFlow node-a (ExampleFlow$Initiator. 2 b-party))]
    (.runNetwork network)
    @fut)

  (let [corda-tx-id (-> (jdbc/execute-one! (.getDataSource (.getDatabase (.getServices node-a)))
                                           ["SELECT * FROM crux_txs ORDER BY crux_tx_id DESC LIMIT 1"]
                                           {:builder-fn jdbcr/as-unqualified-lower-maps})
                        :corda_tx_id)
        service-hub (.getServices node-a)
        _corda-tx (.getTransaction (.getValidatedTransactions service-hub)
                                   (SecureHash/parse corda-tx-id))]

    ))

(defmulti tx-row->tx
  (fn [tx-row dialect]
    (db-type dialect))
  :default ::default)

(defn- ^ICursor open-tx-log [dialect ^AppServiceHub service-hub after-tx-id]
  (let [stmt (jdbc/prepare (.jdbcSession service-hub)
                           (if after-tx-id
                             ["SELECT * FROM crux_txs WHERE crux_tx_id > ? ORDER BY crux_tx_id"
                              after-tx-id]
                             ["SELECT * FROM crux_txs ORDER BY crux_tx_id"]))
        rs (.executeQuery stmt)]
    (->> (resultset-seq rs)
         (map #(tx-row->tx % dialect))
         (cio/->cursor #(run! cio/try-close [rs stmt])))))

(defn- ->corda-tx [corda-tx-id ^AppServiceHub service-hub]
  (.getTransaction (.getValidatedTransactions service-hub)
                   (SecureHash/parse corda-tx-id)))

(defn- ->crux-doc [^TransactionState tx-state]
  (when-let [^CruxState
             crux-state (let [data (.getData tx-state)]
                          (when (instance? CruxState data)
                            data))]
    (merge {:crux.db/id (.getCruxId crux-state)}
           (->> (.getCruxDoc crux-state)
                (into {} (map (juxt (comp keyword key) val)))))))

(defn- transform-corda-tx [^SignedTransaction corda-tx service-hub]
  (let [ledger-tx (.toLedgerTransaction corda-tx service-hub)
        consumed-ids (->> (.getInputs ledger-tx)
                          (map #(.getState ^StateAndRef %))
                          (keep ->crux-doc)
                          (into #{} (map :crux.db/id)))
        new-docs (->> (.getOutputs ledger-tx)
                      (keep ->crux-doc)
                      (into {} (map (juxt c/new-id identity))))]
    {::tx/tx-events (concat (for [deleted-id (set/difference consumed-ids
                                                             (set (keys new-docs)))]
                              [::tx/delete deleted-id])

                            (for [[new-doc-id new-doc] new-docs]
                              [::tx/put (:crux.db/id new-doc) new-doc-id]))
     :docs new-docs}))

(defrecord CordaTxLog [dialect ^AppServiceHub service-hub]
  db/TxLog
  (submit-tx [this tx-events]
    (throw (UnsupportedOperationException.
            "CordaTxLog does not support submit-tx - submit transactions directly to Corda")))

  (open-tx-log ^crux.api.ICursor [this after-tx-id]
    (let [txs (open-tx-log dialect service-hub after-tx-id)]
      (cio/->cursor #(cio/try-close txs)
                    (->> (iterator-seq txs)
                         (map (fn [{:keys [corda-tx-id] :as tx}]
                                (let [corda-tx (->corda-tx corda-tx-id service-hub)
                                      {:keys [tx-events]} (transform-corda-tx corda-tx service-hub)]
                                  (-> (select-keys tx [::tx/tx-id ::tx/tx-time])
                                      (assoc ::tx/tx-events tx-events)))))))))

  (latest-submitted-tx [this]
    (some-> (jdbc/execute-one! (.jdbcSession service-hub)
                               ["SELECT * FROM crux_txs ORDER BY crux_tx_id DESC LIMIT 1"])
            (tx-row->tx dialect)
            (select-keys [::tx/tx-id ::tx/tx-time]))))

(defn ->tx-log {::sys/deps {:service-hub ::service-hub
                            :dialect 'crux.corda.h2/->dialect}}
  [{:keys [dialect ^AppServiceHub service-hub] :as opts}]
  (setup-tx-schema! dialect (.jdbcSession service-hub))
  (map->CordaTxLog opts))

(defn- index-tx [{:keys [tx-ingester document-store] :as _node}
                 ^AppServiceHub service-hub
                 ^SignedTransaction corda-tx
                 crux-tx]
  (let [{:keys [docs ::tx/tx-events]} (transform-corda-tx corda-tx service-hub)
        in-flight-tx (db/begin-tx tx-ingester crux-tx)]
    (try
      (db/submit-docs document-store docs)
      (db/index-tx-events in-flight-tx tx-events)
      (db/commit in-flight-tx)
      (catch Exception e
        (.printStackTrace e)
        (db/abort in-flight-tx)))))

(defn process-tx [crux-node ^SignedTransaction corda-tx]
  (let [{:keys [dialect ^AppServiceHub service-hub]} (:tx-log crux-node)]
    (index-tx crux-node service-hub corda-tx
              (-> (jdbc/execute-one! (.jdbcSession service-hub)
                                     ["SELECT * FROM crux_txs WHERE corda_tx_id = ?" (str (.getId corda-tx))]
                                     {:builder-fn jdbcr/as-unqualified-lower-maps})
                  (tx-row->tx dialect)))))

(defn- sync-node [crux-node]
  (let [{:keys [dialect service-hub]} (:tx-log crux-node)]
    (with-open [txs (open-tx-log dialect service-hub (::tx/tx-id (crux/latest-completed-tx crux-node)))]
      (->> (iterator-seq txs)
           (run! (fn [{:keys [corda-tx-id] :as tx}]
                   (index-tx crux-node service-hub
                             (->corda-tx corda-tx-id service-hub)
                             tx)))))))

(defn start-node [service-hub]
  (doto (crux/start-node {::service-hub (fn [_] service-hub)
                          :crux/tx-log `->tx-log})
    sync-node))
