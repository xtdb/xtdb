(ns xtdb.log.memory-log
  (:require [xtdb.api :as xt]
            [xtdb.log :as log]
            [xtdb.node :as xtn])
  (:import java.time.InstantSource
           java.time.temporal.ChronoUnit
           java.util.concurrent.CompletableFuture
           (xtdb.api TransactionKey XtdbSubmitClient$Config)
           (xtdb.api.log InMemoryLogFactory Log LogRecord)
           xtdb.log.INotifyingSubscriberHandler))

(deftype InMemoryLog [!records, ^INotifyingSubscriberHandler subscriber-handler, ^InstantSource instant-src]
  Log
  (appendRecord [_ record]
    (CompletableFuture/completedFuture
     (let [^LogRecord record (-> (swap! !records (fn [records]
                                                   (let [system-time (-> (.instant instant-src) (.truncatedTo ChronoUnit/MICROS))]
                                                     (conj records (LogRecord. (TransactionKey. (count records) system-time) record)))))
                                 peek)
           tx-key (.getTxKey record)]
       (.notifyTx subscriber-handler tx-key)
       tx-key)))

  (readRecords [_ after-tx-id limit]
    (let [records @!records
          offset (if after-tx-id
                   (inc ^long after-tx-id)
                   0)]
      (subvec records offset (min (+ offset limit) (count records)))))

  (subscribe [this after-tx-id subscriber]
    (.subscribe subscriber-handler this after-tx-id subscriber)))

(defmethod xtn/apply-config! :xtdb.log/memory-log [^XtdbSubmitClient$Config config _ {:keys [instant-src]}]
  (doto config
    (.setTxLog (cond-> (InMemoryLogFactory.)
                 instant-src (.instantSource instant-src)))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-log [^InMemoryLogFactory factory]
  (InMemoryLog. (atom []) (log/->notifying-subscriber-handler nil) (.getInstantSource factory)))
