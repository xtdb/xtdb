(ns xtdb.log.memory-log
  (:require [xtdb.api :as xt]
            [xtdb.log :as log]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import xtdb.InstantSource
           [xtdb.log INotifyingSubscriberHandler Log LogRecord]
           java.time.temporal.ChronoUnit
           java.util.concurrent.CompletableFuture))

(deftype InMemoryLog [!records, ^INotifyingSubscriberHandler subscriber-handler, ^InstantSource instant-src]
  Log
  (appendRecord [_ record]
    (CompletableFuture/completedFuture
     (let [^LogRecord record (-> (swap! !records (fn [records]
                                                   (let [sys-time (-> (.instant instant-src) (.truncatedTo ChronoUnit/MICROS))]
                                                     (conj records (log/->LogRecord (xt/->TransactionInstant (count records) sys-time) record)))))
                                 peek)]
       (.notifyTx subscriber-handler (.tx record))
       record)))

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
  (merge {:instant-src InstantSource/SYSTEM} opts))

(defmethod ig/init-key :xtdb.log/memory-log [_ {:keys [instant-src]}]
  (InMemoryLog. (atom []) (log/->notifying-subscriber-handler nil) instant-src))
