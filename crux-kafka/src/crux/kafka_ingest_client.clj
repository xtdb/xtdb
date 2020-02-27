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

  (openTxLog ^crux.api.ITxLog [_ after-tx-id with-ops?]
    (when with-ops?
      (throw (IllegalArgumentException. "with-ops? not supported")))
    (db/open-tx-log tx-log after-tx-id))

  Closeable
  (close [_]
    (when close-fn (close-fn))))

(def topology
  (merge (select-keys n/base-topology [::n/kv-store ::n/object-store])
         {::n/tx-log k/tx-log
          ::n/document-store k/document-store
          ::k/admin-client k/admin-client
          ::k/producer k/producer
          ::k/latest-submitted-tx-consumer k/latest-submitted-tx-consumer}))

(defn new-ingest-client ^ICruxAsyncIngestAPI [options]
  (let [[{::n/keys [tx-log document-store kv-store object-store]} close-fn]
        (topo/start-topology (merge {::n/topology topology}
                                    options))]
    (map->CruxKafkaIngestClient {:tx-log tx-log
                                 :document-store document-store
                                 :kv-store kv-store
                                 :object-store object-store
                                 :close-fn close-fn})))
