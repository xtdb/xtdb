(ns crux.kafka-ingest-client
  (:require [crux.db :as db]
            [crux.kafka :as k]
            [crux.topology :as topo]
            [crux.node :as n]
            [crux.tx :as tx])
  (:import crux.api.ICruxAsyncIngestAPI
           java.io.Closeable))

(defrecord CruxKafkaIngestClient [tx-log document-store close-fn]
  ICruxAsyncIngestAPI
  (submitTxAsync [_ tx-ops]
    (db/submit-docs document-store (tx/tx-ops->id-and-docs tx-ops))
    (db/submit-tx tx-log tx-ops))

  (submitTx [this tx-ops]
    @(.submitTxAsync this tx-ops))

  (openTxLog ^crux.api.ITxLog [_ from-tx-id with-ops?]
    (when with-ops?
      (throw (IllegalArgumentException. "with-ops? not supported")))
    (db/open-tx-log tx-log from-tx-id))

  Closeable
  (close [_]
    (when close-fn (close-fn))))

(def topology {:crux.node/tx-log k/tx-log
               :crux.node/document-store k/document-store
               :crux.kafka/admin-client k/admin-client
               :crux.kafka/admin-wrapper k/admin-wrapper
               :crux.kafka/producer k/producer
               :crux.kafka/latest-submitted-tx-consumer k/latest-submitted-tx-consumer})

(defn new-ingest-client ^ICruxAsyncIngestAPI [options]
  (let [[{:keys [crux.node/tx-log crux.node/document-store]} close-fn]
        (topo/start-topology (merge {:crux.node/topology topology}
                                    options))]
    (map->CruxKafkaIngestClient {:tx-log tx-log :document-store document-store :close-fn close-fn})))
