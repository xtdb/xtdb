(ns crux.bootstrap.kafka-ingest-client
  (:require [crux.bootstrap :as b]
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

  (newTxLogContext [_]
    (db/new-tx-log-context tx-log))

  (txLog [_ tx-log-context from-tx-id with-documents?]
    (when with-documents?
      (throw (IllegalArgumentException. "with-documents? not supported")))
    (db/tx-log tx-log tx-log-context from-tx-id))

  Closeable
  (close [_]
    (when close-fn (close-fn))))

(def ingest-client-config {:tx-log k/tx-log
                           :admin-client k/admin-client
                           :admin-wrapper k/admin-wrapper
                           :producer k/producer})

(defn new-ingest-client ^ICruxAsyncIngestAPI [options]
  (let [options (merge b/default-options options)
        [node-modules close-fn] (b/start-modules ingest-client-config options)]
    (map->CruxKafkaIngestClient (assoc node-modules :close-fn close-fn :options options))))
