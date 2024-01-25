(ns xtdb.log.memory-log
  (:require [xtdb.api :as xt]
            [xtdb.log :as log]
            [xtdb.node :as xtn])
  (:import java.time.InstantSource
           java.time.temporal.ChronoUnit
           java.util.concurrent.CompletableFuture
           (xtdb.api AConfig TransactionKey)
           (xtdb.api.log Log Log$Record Logs Logs$InMemoryLogFactory)
           xtdb.log.INotifyingSubscriberHandler))

(deftype InMemoryLog [!records, ^INotifyingSubscriberHandler subscriber-handler, ^InstantSource instant-src]
  Log
  (appendRecord [_ record]
    (CompletableFuture/completedFuture
     (let [^Log$Record record (-> (swap! !records (fn [records]
                                                   (let [system-time (-> (.instant instant-src) (.truncatedTo ChronoUnit/MICROS))]
                                                     (conj records (Log$Record. (TransactionKey. (count records) system-time) record)))))
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

(defmethod xtn/apply-config! :xtdb.log/memory-log [^AConfig config _ {:keys [instant-src]}]
  (doto config
    (.setTxLog (cond-> (Logs/inMemoryLog)
                 instant-src (.instantSource instant-src)))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-log [^Logs$InMemoryLogFactory factory]
  (InMemoryLog. (atom []) (log/->notifying-subscriber-handler nil) (.getInstantSource factory)))
