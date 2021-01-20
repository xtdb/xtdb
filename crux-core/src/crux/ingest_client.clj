(ns crux.ingest-client
  (:require [crux.db :as db]
            [crux.error :as err]
            [crux.system :as sys]
            [crux.tx.conform :as txc]
            [clojure.pprint :as pp]
            [crux.api :as api])
  (:import crux.api.ICruxAsyncIngestAPI
           [java.io Closeable Writer]
           java.lang.AutoCloseable))

(defrecord CruxIngestClient [tx-log document-store close-fn]
  ICruxAsyncIngestAPI
  (submitTxAsync [this tx-ops] (api/submit-tx-async this tx-ops))
  (submitTx [this tx-ops] (api/submit-tx this tx-ops))
  (openTxLog ^crux.api.ICursor [this after-tx-id with-ops?] (api/open-tx-log this after-tx-id with-ops?))

  Closeable
  (close [_]
    (when close-fn (close-fn))))

(extend-protocol api/PCruxIngestClient
  CruxIngestClient
  (submit-tx [this tx-ops]
    @(api/submit-tx-async this tx-ops))

  (open-tx-log ^crux.api.ICursor [this after-tx-id with-ops?]
    (when with-ops?
      (throw (err/illegal-arg :with-opts-not-supported
                              {::err/message "with-ops? not supported"})))
    (db/open-tx-log (:tx-log this) after-tx-id)))

(extend-protocol api/PCruxAsyncIngestClient
  CruxIngestClient
  (submit-tx-async [this tx-ops]
    (let [conformed-tx-ops (mapv txc/conform-tx-op tx-ops)]
      (db/submit-docs (:document-store this) (into {} (mapcat :docs) conformed-tx-ops))
      (db/submit-tx (:tx-log this) (mapv txc/->tx-event conformed-tx-ops)))))


(defmethod print-method CruxIngestClient [_ ^Writer w] (.write w "#<CruxIngestClient>"))
(defmethod pp/simple-dispatch CruxIngestClient [it] (print-method it *out*))

(defn ->ingest-client {::sys/deps {:tx-log :crux/tx-log
                                   :document-store :crux/document-store}}
  [{:keys [tx-log document-store]}]
  (->CruxIngestClient tx-log document-store nil))

(defn open-ingest-client ^ICruxAsyncIngestAPI [options]
  (let [system (-> (sys/prep-system (into [{:crux/ingest-client `->ingest-client
                                            :crux/bus 'crux.bus/->bus
                                            :crux/document-store 'crux.kv.document-store/->document-store
                                            :crux/tx-log 'crux.kv.tx-log/->ingest-only-tx-log}]
                                          (cond-> options (not (vector? options)) vector)))
                   (sys/start-system))]
    (-> (:crux/ingest-client system)
        (assoc :close-fn #(.close ^AutoCloseable system)))))
