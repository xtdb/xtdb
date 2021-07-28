(ns crux.corda.h2
  (:require [crux.corda :as crux-corda]
            [crux.tx :as tx]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc])
  (:import (java.time OffsetDateTime LocalDate LocalTime ZoneOffset)
           (java.util Date)
           org.h2.api.TimestampWithTimeZone))

(defn ->dialect [_]
  (reify crux-corda/SQLDialect
    (db-type [_] :h2)

    (setup-tx-schema! [_ jdbc-session]
      (jdbc/execute-one! jdbc-session ["
CREATE TABLE IF NOT EXISTS crux_txs (
  crux_tx_id IDENTITY NOT NULL PRIMARY KEY,
  crux_tx_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  corda_tx_id VARCHAR(64) NOT NULL UNIQUE REFERENCES node_transactions(tx_id)
)"])

      (jdbc/execute-one! jdbc-session ["
CREATE TRIGGER IF NOT EXISTS crux_tx_trigger
  AFTER INSERT, UPDATE ON node_transactions
  FOR EACH ROW
  CALL \"crux.corda.NodeTransactionTrigger\""]))))

(defmethod crux-corda/tx-row->tx :h2 [tx-row _]
  (let [^TimestampWithTimeZone h2-tx-time (:crux_tx_time tx-row)]
    {::tx/tx-id (:crux_tx_id tx-row)
     ::tx/tx-time (-> (OffsetDateTime/of (LocalDate/of (.getYear h2-tx-time)
                                                       (.getMonth h2-tx-time)
                                                       (.getDay h2-tx-time))
                                         (LocalTime/ofNanoOfDay
                                          (.getNanosSinceMidnight h2-tx-time))
                                         (ZoneOffset/ofTotalSeconds
                                          (* 60 (.getTimeZoneOffsetMins h2-tx-time))))
                      .toInstant
                      Date/from)
     :corda-tx-id (:corda_tx_id tx-row)}))
