(ns crux.corda.service
  (:require [crux.api :as crux]
            [crux.tx :as tx]
            [crux.db :as db]
            [crux.codec :as c]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as jdbcr]
            [clojure.set :as set]
            [crux.io :as cio])
  (:import (crux.corda.contract CruxState)
           (net.corda.core.crypto SecureHash)
           (net.corda.core.node AppServiceHub)
           (net.corda.core.transactions SignedTransaction)
           (net.corda.core.contracts TransactionState StateAndRef)
           (java.time OffsetDateTime LocalDate LocalTime ZoneOffset)
           (java.util Date)
           org.h2.api.TimestampWithTimeZone))

(set! *warn-on-reflection* true)

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

(defn- tx-row->tx [tx-row]
  (let [^TimestampWithTimeZone
        h2-tx-time (:crux_tx_time tx-row)
        tx-time (-> (OffsetDateTime/of (LocalDate/of (.getYear h2-tx-time)
                                                     (.getMonth h2-tx-time)
                                                     (.getDay h2-tx-time))
                                       (LocalTime/ofNanoOfDay (.getNanosSinceMidnight h2-tx-time))
                                       (ZoneOffset/ofTotalSeconds (* 60 (.getTimeZoneOffsetMins h2-tx-time))))
                    .toInstant
                    Date/from)]
    {::tx/tx-id (:crux_tx_id tx-row)
     ::tx/tx-time tx-time
     :corda-tx-id (:corda_tx_id tx-row)}))

(defn- open-tx-log [^AppServiceHub service-hub after-tx-id]
  (let [stmt (jdbc/prepare (.jdbcSession service-hub)
                           (if after-tx-id
                             [(str "SELECT * FROM crux_txs WHERE crux_tx_id > ? ORDER BY crux_tx_id") after-tx-id]
                             [(str "SELECT * FROM crux_txs ORDER BY crux_tx_id")]))
        rs (.executeQuery stmt)]
    (->> (resultset-seq rs)
         (map tx-row->tx)
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
    {:crux.tx/tx-events (concat (for [deleted-id (set/difference consumed-ids
                                                                 (set (keys new-docs)))]
                                  [:crux.tx/delete deleted-id])

                                (for [[new-doc-id new-doc] new-docs]
                                  [:crux.tx/put (:crux.db/id new-doc) new-doc-id]))
     :docs new-docs}))

(defrecord CordaTxLog [service-hub]
  db/TxLog
  (submit-tx [this tx-events]
    (throw (UnsupportedOperationException.
            "CordaTxLog does not support submit-tx - submit transactions directly to Corda")))

  (open-tx-log ^crux.api.ICursor [this after-tx-id]
    (let [txs (open-tx-log service-hub after-tx-id)]
      (cio/->cursor #(cio/try-close txs)
                    (->> (iterator-seq txs)
                         (map (fn [{:keys [corda-tx-id] :as tx}]
                                (let [corda-tx (->corda-tx corda-tx-id service-hub)
                                      {:keys [tx-events]} (transform-corda-tx corda-tx service-hub)]
                                  (-> (select-keys tx [:crux.tx/tx-id :crux.tx/tx-time])
                                      (assoc :crux.tx/tx-events tx-events)))))))))

  (latest-submitted-tx [this]
    (some-> (jdbc/execute-one! ["SELECT * FROM crux_txs ORDER BY crux_tx_id DESC LIMIT 1"])
            (tx-row->tx)
            (select-keys [:crux.tx/tx-id :crux.tx/tx-time]))))

(defn- index-tx [{:keys [tx-ingester document-store] :as _node}
                 ^AppServiceHub service-hub
                 ^SignedTransaction corda-tx
                 crux-tx]
  (let [{:keys [docs :crux.tx/tx-events]} (transform-corda-tx corda-tx service-hub)
        in-flight-tx (db/begin-tx tx-ingester crux-tx)]
    (try
      (db/submit-docs document-store docs)
      (db/index-tx-events in-flight-tx tx-events)
      (db/commit in-flight-tx)
      (catch Exception e
        (.printStackTrace e)
        (db/abort in-flight-tx)))))

(defn process-tx [crux-node ^AppServiceHub service-hub ^SignedTransaction corda-tx]
  (index-tx crux-node service-hub corda-tx
            (-> (jdbc/execute-one! (.jdbcSession service-hub)
                                   ["SELECT * FROM crux_txs WHERE corda_tx_id = ?" (str (.getId corda-tx))]
                                   {:builder-fn jdbcr/as-unqualified-lower-maps})
                (tx-row->tx))))

(defn- sync-node [crux-node ^AppServiceHub service-hub]
  (with-open [txs (open-tx-log service-hub (:crux.tx/tx-id (crux/latest-completed-tx crux-node)))]
    (->> (iterator-seq txs)
         (run! (fn [{:keys [corda-tx-id] :as tx}]
                 (index-tx crux-node service-hub
                           (->corda-tx corda-tx-id service-hub)
                           tx))))))

(defn start-node [service-hub]
  (doto (crux/start-node {:crux/tx-log {:crux/module (fn [_]
                                                       (->CordaTxLog service-hub))}})
    (sync-node service-hub)))
