(ns xtdb.submit-client
  (:require [xtdb.db :as db]
            [xtdb.error :as err]
            [xtdb.system :as sys]
            [xtdb.tx.conform :as txc]
            [clojure.pprint :as pp]
            [xtdb.api :as xt]
            [xtdb.io :as xio]
            [xtdb.codec :as c])
  (:import [java.io Closeable Writer]
           java.lang.AutoCloseable))

(defrecord XtdbSubmitClient [tx-log document-store close-fn]
  xt/PXtdbSubmitClient
  (submit-tx-async [_ tx-ops]
    (let [conformed-tx-ops (mapv txc/conform-tx-op tx-ops)]
      (db/submit-docs document-store (->> conformed-tx-ops
                                          (into {} (comp (mapcat :docs)
                                                         (xio/map-vals c/xt->crux)))))
      (db/submit-tx tx-log (mapv txc/->tx-event conformed-tx-ops))))

  (submit-tx [this tx-ops]
    @(xt/submit-tx-async this tx-ops))

  (open-tx-log [this after-tx-id with-ops?]
    (when with-ops?
      (throw (err/illegal-arg :with-opts-not-supported
                              {::err/message "with-ops? not supported"})))
    (db/open-tx-log (:tx-log this) after-tx-id {}))

  Closeable
  (close [_]
    (when close-fn (close-fn))))

(defmethod print-method XtdbSubmitClient [_ ^Writer w] (.write w "#<XtdbSubmitClient>"))
(defmethod pp/simple-dispatch XtdbSubmitClient [it] (print-method it *out*))

(defn ->submit-client {::sys/deps {:tx-log :xtdb/tx-log
                                   :document-store :xtdb/document-store}
                       ::sys/before [[:xtdb.bus/bus-stop]]}
  [{:keys [tx-log document-store]}]
  (->XtdbSubmitClient tx-log document-store nil))

(defn open-submit-client ^xtdb.submit_client.XtdbSubmitClient [options]
  (let [system (-> (sys/prep-system (into [{:xtdb/submit-client `->submit-client
                                            :xtdb/bus 'xtdb.bus/->bus
                                            :xtdb.bus/bus-stop 'xtdb.bus/->bus-stop
                                            :xtdb/document-store 'xtdb.kv.document-store/->document-store
                                            :xtdb/tx-log 'xtdb.kv.tx-log/->tx-log}]
                                          (cond-> options (not (vector? options)) vector)))
                   (sys/start-system))]
    (-> (:xtdb/submit-client system)
        (assoc :close-fn #(.close ^AutoCloseable system)))))
