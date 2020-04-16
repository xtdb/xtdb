(ns crux.kafka-ingest-client
  (:require [crux.db :as db]
            [crux.kafka :as k]
            [crux.topology :as topo]
            [crux.node :as n]
            [crux.tx :as tx])
  (:import crux.api.ICruxAsyncIngestAPI
           java.io.Closeable
           (org.apache.kafka.clients.producer KafkaProducer)))

(defrecord CruxKafkaIngestClient [tx-log document-store close-fn]
  ICruxAsyncIngestAPI
  (submitTxAsync [_ tx-ops]
    (db/submit-docs document-store (tx/tx-ops->id-and-docs tx-ops))
    (db/submit-tx tx-log (map tx/tx-op->tx-event tx-ops)))

  (submitTx [this tx-ops]
    @(.submitTxAsync this tx-ops))

  (openTxLog ^crux.api.ICursor [_ after-tx-id with-ops?]
    (when with-ops?
      (throw (IllegalArgumentException. "with-ops? not supported")))
    (db/open-tx-log tx-log after-tx-id))

  Closeable
  (close [_]
    (when close-fn (close-fn))))

(defrecord IngestOnlyDocumentStore [^KafkaProducer producer doc-topic]
  db/DocumentStore
  (submit-docs [this id-and-docs]
    (k/submit-docs id-and-docs this))

  (fetch-docs [this ids]
    (throw (UnsupportedOperationException. "Can't fetch docs from ingest-only Kafka document store"))))

(def topology
  {::n/tx-log k/tx-log
   ::n/document-store {:start-fn (fn [{:keys [::k/producer]} {:keys [::k/doc-topic]}]
                                   (->IngestOnlyDocumentStore producer doc-topic))
                       :deps [::k/producer]}
   ::k/admin-client k/admin-client
   ::k/producer k/producer
   ::k/latest-submitted-tx-consumer k/latest-submitted-tx-consumer})

(defn new-ingest-client ^ICruxAsyncIngestAPI [options]
  (let [[{::n/keys [tx-log document-store]} close-fn]
        (topo/start-topology (merge {::n/topology topology}
                                    options))]
    (map->CruxKafkaIngestClient {:tx-log tx-log
                                 :document-store document-store
                                 :close-fn close-fn})))
