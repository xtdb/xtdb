(ns crux.corda.service
  (:require [crux.api :as crux]
            [crux.tx :as tx]
            [crux.db :as db]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as jdbcr])
  (:import (net.corda.core.node AppServiceHub)
           (net.corda.core.transactions SignedTransaction LedgerTransaction)
           (java.time OffsetDateTime LocalDate LocalTime ZoneOffset)
           (java.util Date)
           org.h2.api.TimestampWithTimeZone))

(set! *warn-on-reflection* true)

(defn process-tx [{:keys [tx-ingester] :as node}
                  ^AppServiceHub service-hub
                  ^SignedTransaction corda-tx]
  (let [corda-tx-id (str (.getId corda-tx))
        tx-row (jdbc/execute-one! (.jdbcSession service-hub)
                                  ["SELECT * FROM crux_txs WHERE corda_tx_id = ?" corda-tx-id]
                                  {:builder-fn jdbcr/as-unqualified-lower-maps})
        ^TimestampWithTimeZone
        h2-tx-time (:crux_tx_time tx-row)
        tx-time (-> (OffsetDateTime/of (LocalDate/of (.getYear h2-tx-time)
                                                     (.getMonth h2-tx-time)
                                                     (.getDay h2-tx-time))
                                       (LocalTime/ofNanoOfDay (.getNanosSinceMidnight h2-tx-time))
                                       (ZoneOffset/ofTotalSeconds (* 60 (.getTimeZoneOffsetMins h2-tx-time))))
                    .toInstant
                    Date/from)

        in-flight-tx (db/begin-tx tx-ingester {::tx/tx-id (:crux_tx_id tx-row)
                                               ::tx/tx-time tx-time})]

    (db/commit in-flight-tx)))

(defn start-node [^AppServiceHub service-hub]
  (crux/start-node {}))
