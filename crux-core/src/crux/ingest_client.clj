(ns crux.ingest-client
  (:require [crux.db :as db]
            [crux.tx :as tx]
            [crux.tx.conform :as txc]
            [crux.system :as sys])
  (:import crux.api.ICruxAsyncIngestAPI
           java.lang.AutoCloseable
           (java.io Closeable Writer)))

(defrecord CruxIngestClient [tx-log document-store close-fn]
  ICruxAsyncIngestAPI
  (submitTxAsync [_ tx-ops]
    (let [conformed-tx-ops (mapv txc/conform-tx-op tx-ops)]
      (db/submit-docs document-store (into {} (mapcat :docs) conformed-tx-ops))
      (db/submit-tx tx-log (mapv txc/->tx-event conformed-tx-ops))))

  (submitTx [this tx-ops]
    @(.submitTxAsync this tx-ops))

  (openTxLog ^crux.api.ICursor [_ after-tx-id with-ops?]
    (when with-ops?
      (throw (IllegalArgumentException. "with-ops? not supported")))
    (db/open-tx-log tx-log after-tx-id))

  Closeable
  (close [_]
    (when close-fn (close-fn))))

(defmethod print-method CruxIngestClient [_ ^Writer w] (.write w "#<CruxIngestClient>"))

(defn ->ingest-client {::sys/deps {:tx-log :crux/tx-log
                                   :document-store :crux/document-store}}
  [{:keys [tx-log document-store]}]
  (->CruxIngestClient tx-log document-store nil))

(defn open-ingest-client ^ICruxAsyncIngestAPI [options]
  (let [system (-> (sys/prep-system [{:crux/ingest-client `->ingest-client
                                      :crux/bus 'crux.bus/->bus
                                      :crux/document-store 'crux.kv.document-store/->document-store
                                      :crux/tx-log 'crux.kv.tx-log/->ingest-only-tx-log}
                                     options])
                   (sys/start-system))]
    (-> (:crux/ingest-client system)
        (assoc :close-fn #(.close ^AutoCloseable system)))))
