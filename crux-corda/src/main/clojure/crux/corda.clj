(ns crux.corda
  (:require [crux.api :as crux]
            [crux.tx :as tx]
            [crux.db :as db]
            [crux.codec :as c]
            [crux.system :as sys]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.result-set :as jdbcr]
            [clojure.set :as set]
            [crux.io :as cio])
  (:import (crux.corda.state CruxState)
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
      (MockNetwork. (MockNetworkParameters. [(TestCordapp/findCordapp "crux.corda.service")
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

(defn ->crux-tx [^SecureHash corda-tx-id {{{:keys [dialect ^AppServiceHub service-hub]} :tx-log} :node}]
  (some-> (jdbc/execute-one! (.jdbcSession service-hub)
                             ["SELECT * FROM crux_txs WHERE corda_tx_id = ?"
                              (str corda-tx-id)]
                             {:builder-fn jdbcr/as-unqualified-lower-maps})
          (tx-row->tx dialect)
          (select-keys [::tx/tx-id ::tx/tx-time])))

(defn- ->corda-tx [corda-tx-id ^AppServiceHub service-hub]
  (.getTransaction (.getValidatedTransactions service-hub)
                   (SecureHash/parse corda-tx-id)))

(defn- ->crux-docs [^TransactionState tx-state {:keys [document-mapper]}]
  (for [^CruxState crux-state (document-mapper (.getData tx-state))
        :when (instance? CruxState crux-state)]
    (merge {:crux.db/id (.getCruxId crux-state)}
           (->> (.getCruxDoc crux-state)
                (into {} (map (juxt (comp keyword key) val)))))))

(defn- transform-corda-tx [^SignedTransaction corda-tx {:keys [service-hub] :as opts}]
  (let [ledger-tx (.toLedgerTransaction corda-tx service-hub)
        consumed-ids (->> (.getInputs ledger-tx)
                          (map #(.getState ^StateAndRef %))
                          (mapcat #(->crux-docs % opts))
                          (into #{} (map :crux.db/id)))
        new-docs (->> (.getOutputs ledger-tx)
                      (mapcat #(->crux-docs % opts))
                      (into {} (map (juxt c/new-id identity))))]
    {::tx/tx-events (concat (for [deleted-id (set/difference consumed-ids (set (keys new-docs)))]
                              [::tx/delete deleted-id])

                            (for [[new-doc-id new-doc] new-docs]
                              [::tx/put (:crux.db/id new-doc) new-doc-id]))
     :docs new-docs}))

(defn- ^ICursor open-tx-log [{:keys [dialect ^AppServiceHub service-hub] :as tx-log} after-tx-id]
  (let [stmt (jdbc/prepare (.jdbcSession service-hub)
                           (if after-tx-id
                             ["SELECT * FROM crux_txs WHERE crux_tx_id > ? ORDER BY crux_tx_id"
                              after-tx-id]
                             ["SELECT * FROM crux_txs ORDER BY crux_tx_id"]))
        rs (.executeQuery stmt)]
    (->> (for [row (resultset-seq rs)]
           (let [{:keys [corda-tx-id] :as tx} (tx-row->tx row dialect)
                 corda-tx (->corda-tx corda-tx-id service-hub)]
             (merge (select-keys tx [::tx/tx-id ::tx/tx-time])
                    (transform-corda-tx corda-tx tx-log))))
         (cio/->cursor #(run! cio/try-close [rs stmt])))))

(defrecord CordaTxLog [dialect ^AppServiceHub service-hub, document-mapper]
  db/TxLog
  (submit-tx [this tx-events]
    (throw (UnsupportedOperationException.
            "CordaTxLog does not support submit-tx - submit transactions directly to Corda")))

  (open-tx-log ^crux.api.ICursor [this after-tx-id]
    (let [txs (open-tx-log this after-tx-id)]
      (cio/->cursor #(cio/try-close txs)
                    (->> (iterator-seq txs)
                         (map #(select-keys % [::tx/tx-id ::tx/tx-time ::tx/tx-events]))))))

  (latest-submitted-tx [this]
    (some-> (jdbc/execute-one! (.jdbcSession service-hub)
                               ["SELECT * FROM crux_txs ORDER BY crux_tx_id DESC LIMIT 1"]
                               {:builder-fn jdbcr/as-unqualified-lower-maps})
            (tx-row->tx dialect)
            (select-keys [::tx/tx-id ::tx/tx-time]))))

(defn sync-txs [{{:keys [tx-log tx-ingester document-store] :as crux-node} :node}]
  (with-open [txs (open-tx-log tx-log (::tx/tx-id (crux/latest-completed-tx crux-node)))]
    (doseq [{:keys [docs ::tx/tx-events] :as tx} (iterator-seq txs)]
      (let [in-flight-tx (db/begin-tx tx-ingester tx nil)]
        (try
          (db/submit-docs document-store docs)
          (db/index-tx-events in-flight-tx tx-events)
          (db/commit in-flight-tx)
          (catch Exception e
            (.printStackTrace e)
            (db/abort in-flight-tx)
            ;; TODO behaviour here? abort consumption entirely?
            ))))))

(defn ->document-mapper [_]
  (fn [doc]
    (when (instance? CruxState doc)
      [doc])))

(defn ->tx-log {::sys/deps {:dialect 'crux.corda.h2/->dialect
                            :tx-ingester :crux/tx-ingester
                            :document-store :crux/document-store
                            :service-hub ::service-hub
                            :document-mapper `->document-mapper}}
  [{:keys [dialect ^AppServiceHub service-hub] :as opts}]
  (setup-tx-schema! dialect (.jdbcSession service-hub))
  (map->CordaTxLog opts))
