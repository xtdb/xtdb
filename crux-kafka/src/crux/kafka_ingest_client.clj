(ns crux.kafka-ingest-client
  (:require [crux.node :as n]
            [crux.db :as db]
            [crux.kafka :as k])
  (:import crux.api.ICruxAsyncIngestAPI
           java.io.Closeable))

(defrecord CruxKafkaIngestClient [tx-log close-fn]
  ICruxAsyncIngestAPI
  (submitTxAsync [_ tx-ops]
    (db/submit-tx tx-log tx-ops))

  (submitTx [_ tx-ops]
    @(db/submit-tx tx-log tx-ops))

  (open-tx-log-iterator [_ from-tx-id with-ops?]
    (when with-documents?
      (throw (IllegalArgumentException. "with-ops? not supported")))
    (db/open-tx-log-iterator tx-log from-tx-id))

  Closeable
  (close [_]
    (when close-fn (close-fn))))

(def topology {:crux.node/tx-log k/tx-log
               :crux.kafka/admin-client k/admin-client
               :crux.kafka/admin-wrapper k/admin-wrapper
               :crux.kafka/producer k/producer
               :crux.kafka/latest-submitted-tx-consumer k/latest-submitted-tx-consumer})

(defn new-ingest-client ^ICruxAsyncIngestAPI [options]
  (let [[{:keys [crux.node/tx-log]} close-fn] (n/start-components topology options)]
    (map->CruxKafkaIngestClient {:tx-log tx-log :close-fn close-fn})))
