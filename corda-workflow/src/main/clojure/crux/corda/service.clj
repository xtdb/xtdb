(ns crux.corda.service
  (:require [crux.api :as crux]
            [crux.tx :as tx]
            [crux.db :as db]
            [crux.codec :as c]
            [next.jdbc :as jdbc]
            [next.jdbc.prepare :as jdbc-prep]
            [next.jdbc.result-set :as jdbcr]
            [clojure.set :as set]
            [clojure.string :as str]
            [taoensso.nippy :as nippy])
  (:import (crux.corda.contract CruxState)
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

(defrecord CordaDocStore [^AppServiceHub service-hub]
  db/DocumentStore
  (submit-docs [_ id+docs]
    (with-open [stmt (jdbc/prepare (.jdbcSession service-hub)
                                   ["MERGE INTO crux_docs (id, doc) KEY (id) VALUES (?, ?)"])]
      (jdbc-prep/execute-batch! stmt (->> id+docs
                                          (map (juxt (comp str key)
                                                     (comp nippy/freeze val)))))))

  (fetch-docs [_ ids]
    (when (seq ids)
      (->> (jdbc/execute! (.jdbcSession service-hub)
                          (into [(format "SELECT * FROM crux_docs WHERE id IN (%s)"
                                         (->> (repeat (count ids) "?") (str/join ", ")))]
                                (map str ids))
                          {:builder-fn jdbcr/as-unqualified-lower-maps})
           (into {} (map (juxt (comp c/new-id :id)
                               (comp nippy/thaw :doc))))))))

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
     ::tx/tx-time tx-time}))

(defrecord CordaTxLog [service-hub]
  db/TxLog
  (submit-tx [this tx-events]
    (throw (UnsupportedOperationException.
            "CordaTxLog does not support submit-tx - submit transactions directly to Corda")))

  (open-tx-log ^crux.api.ICursor [this after-tx-id]
    ;; TODO
    (throw (UnsupportedOperationException. "not supported yet")))

  (latest-submitted-tx [this]
    (some-> (jdbc/execute-one! ["SELECT * FROM crux_txs ORDER BY crux_tx_id DESC LIMIT 1"])
            (tx-row->tx))))

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
    {:tx-events (concat (for [deleted-id (set/difference consumed-ids
                                                         (set (keys new-docs)))]
                          [:crux.tx/delete deleted-id])

                        (for [[new-doc-id new-doc] new-docs]
                          [:crux.tx/put (:crux.db/id new-doc) new-doc-id]))
     :docs new-docs}))

(defn process-tx [{:keys [tx-ingester document-store] :as _node}
                  ^AppServiceHub service-hub
                  ^SignedTransaction corda-tx]
  (let [corda-tx-id (str (.getId corda-tx))
        tx (-> (jdbc/execute-one! (.jdbcSession service-hub)
                                  ["SELECT * FROM crux_txs WHERE corda_tx_id = ?" corda-tx-id]
                                  {:builder-fn jdbcr/as-unqualified-lower-maps})
               (tx-row->tx))

        {:keys [docs tx-events]} (transform-corda-tx corda-tx service-hub)

        in-flight-tx (db/begin-tx tx-ingester tx)]

    (try
      (db/submit-docs document-store docs)
      (db/index-tx-events in-flight-tx tx-events)
      (db/commit in-flight-tx)
      (catch Exception _e
        (.printStackTrace _e)
        (db/abort in-flight-tx)))))

(defn start-node [^AppServiceHub service-hub]
  (crux/start-node {:crux/tx-log {:crux/module (fn [_]
                                                 (->CordaTxLog service-hub))}
                    :crux/document-store {:crux/module (fn [_]
                                                         (->CordaDocStore service-hub))}}))
