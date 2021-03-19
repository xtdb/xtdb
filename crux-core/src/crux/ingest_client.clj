(ns crux.ingest-client
  (:require [clojure.pprint :as pp]
            [crux.api :as api]
            [crux.db :as db]
            [crux.error :as err]
            [crux.system :as sys]
            [crux.tx.conform :as txc])
  (:import [java.io Closeable Writer]
           java.lang.AutoCloseable
           java.util.concurrent.CompletableFuture
           [java.util.function Function Supplier]))

(defrecord CruxIngestClient [tx-log document-store close-fn]
  api/PCruxAsyncIngestClient
  (submit-tx-async [_ tx-ops]
    (-> (CompletableFuture/supplyAsync
         (reify Supplier
           (get [_]
             (mapv txc/conform-tx-op (api/conform-tx-ops tx-ops)))))
        (.thenCompose (reify Function
                        (apply [_ conformed-tx-ops]
                          (-> (db/submit-docs-async document-store (into {} (mapcat :docs) conformed-tx-ops))
                              (.thenCompose (reify Function
                                              (apply [_ _]
                                                (db/submit-tx-async tx-log (mapv txc/->tx-event conformed-tx-ops)))))))))))

  api/PCruxIngestClient
  (submit-tx [this tx-ops]
    @(api/submit-tx-async this tx-ops))

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

(defn ->ingest-client {::sys/deps {:tx-log :crux/tx-log
                                   :document-store :crux/document-store}}
  [{:keys [tx-log document-store]}]
  (->CruxIngestClient tx-log document-store nil))

(defn open-ingest-client [options]
  (let [system (-> (sys/prep-system (into [{:crux/ingest-client `->ingest-client
                                            :crux/bus 'crux.bus/->bus
                                            :crux/document-store 'crux.kv.document-store/->document-store
                                            :crux/tx-log 'crux.kv.tx-log/->ingest-only-tx-log}]
                                          (cond-> options (not (vector? options)) vector)))
                   (sys/start-system))]
    (-> (:crux/ingest-client system)
        (assoc :close-fn #(.close ^AutoCloseable system)))))
