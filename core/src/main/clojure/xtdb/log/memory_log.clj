(ns xtdb.log.memory-log
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.log :as log])
  (:import java.time.InstantSource
           java.time.temporal.ChronoUnit
           java.util.concurrent.CompletableFuture
           (xtdb.api TransactionKey)
           (xtdb.api.log Log LogRecord)
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

(derive :xtdb.log/memory-log :xtdb/log)

(defmethod ig/prep-key :xtdb.log/memory-log [_ opts]
  (merge {:instant-src (InstantSource/system)} opts))

(defmethod ig/init-key :xtdb.log/memory-log [_ {:keys [instant-src]}]
  (InMemoryLog. (atom []) (log/->notifying-subscriber-handler nil) instant-src))
