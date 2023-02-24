(ns core2.log.memory-log
  (:require [core2.api :as c2]
            [core2.log :as log]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.InstantSource
           [core2.log INotifyingSubscriberHandler Log LogRecord]
           java.time.temporal.ChronoUnit
           java.util.concurrent.CompletableFuture))

(deftype InMemoryLog [!records, ^INotifyingSubscriberHandler subscriber-handler, ^InstantSource instant-src]
  Log
  (appendRecord [_ record]
    (CompletableFuture/completedFuture
     (let [^LogRecord record (-> (swap! !records (fn [records]
                                                   (let [sys-time (-> (.instant instant-src) (.truncatedTo ChronoUnit/MICROS))]
                                                     (conj records (log/->LogRecord (c2/->TransactionInstant (count records) sys-time) record)))))
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

(derive :core2.log/memory-log :core2/log)

(defmethod ig/prep-key :core2.log/memory-log [_ opts]
  (merge {:instant-src InstantSource/SYSTEM} opts))

(defmethod ig/init-key :core2.log/memory-log [_ {:keys [instant-src]}]
  (InMemoryLog. (atom []) (log/->notifying-subscriber-handler nil) instant-src))
