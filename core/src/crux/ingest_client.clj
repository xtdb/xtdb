(ns crux.ingest-client
  (:require [crux.db :as db]
            [crux.error :as err]
            [crux.system :as sys]
            [crux.tx.conform :as txc]
            [clojure.pprint :as pp]
            [crux.api :as xt])
  (:import [java.io Closeable Writer]
           java.lang.AutoCloseable))

(defrecord CruxIngestClient [tx-log document-store close-fn]
  xt/PCruxAsyncIngestClient
  (submit-tx-async [_ tx-ops]
    (let [conformed-tx-ops (mapv txc/conform-tx-op tx-ops)]
      (db/submit-docs document-store (into {} (mapcat :docs) conformed-tx-ops))
      (db/submit-tx tx-log (mapv txc/->tx-event conformed-tx-ops))))

  xt/PCruxIngestClient
  (submit-tx [this tx-ops]
    @(xt/submit-tx-async this tx-ops))

  (open-tx-log [this after-tx-id with-ops?]
    (when with-ops?
      (throw (err/illegal-arg :with-opts-not-supported
                              {::err/message "with-ops? not supported"})))
    (db/open-tx-log (:tx-log this) after-tx-id))

  Closeable
  (close [_]
    (when close-fn (close-fn))))

(defmethod print-method CruxIngestClient [_ ^Writer w] (.write w "#<CruxIngestClient>"))
(defmethod pp/simple-dispatch CruxIngestClient [it] (print-method it *out*))

(defn ->ingest-client {::sys/deps {:tx-log :xt/tx-log
                                   :document-store :xt/document-store}}
  [{:keys [tx-log document-store]}]
  (->CruxIngestClient tx-log document-store nil))

(defn open-ingest-client ^crux.ingest_client.CruxIngestClient [options]
  (let [system (-> (sys/prep-system (into [{:xt/ingest-client `->ingest-client
                                            :xt/bus 'crux.bus/->bus
                                            :xt/document-store 'crux.kv.document-store/->document-store
                                            :xt/tx-log 'crux.kv.tx-log/->tx-log}]
                                          (cond-> options (not (vector? options)) vector)))
                   (sys/start-system))]
    (-> (:xt/ingest-client system)
        (assoc :close-fn #(.close ^AutoCloseable system)))))
