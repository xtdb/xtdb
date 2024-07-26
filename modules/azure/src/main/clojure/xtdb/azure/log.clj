(ns xtdb.azure.log
  (:require [xtdb.api :as xt]
            [xtdb.log :as log]
            [xtdb.util :as util]
            [xtdb.serde :as serde])
  (:import [com.azure.messaging.eventhubs EventData EventHubConsumerClient EventHubProducerAsyncClient]
           [com.azure.messaging.eventhubs.models EventPosition PartitionEvent SendOptions]
           java.io.Closeable
           java.nio.ByteBuffer
           java.time.Duration
           java.util.concurrent.CompletableFuture
           [xtdb.api TransactionKey]
           (xtdb.api.log Log Log$Record)
           [xtdb.log INotifyingSubscriberHandler]))

(defn ->consumer [f]
  (reify java.util.function.Consumer
    (accept
     [_ val]
     (f val))))

(defn event-data->tx-instant [^EventData data]
  (serde/->TxKey (.getSequenceNumber data) (.getEnqueuedTime data)))

(def producer-send-options
  (.setPartitionId (SendOptions.) "0"))

(defn get-partition-properties [^EventHubConsumerClient consumer]
  (let [partition-properties (.getPartitionProperties consumer "0")]
    {:sequence-number (.getLastEnqueuedSequenceNumber partition-properties)
     :timestamp (.getLastEnqueuedTime partition-properties)}))

(defn- ->log-record [^PartitionEvent event]
  (let [data ^EventData (.getData event)]
    (Log$Record. (event-data->tx-instant data) (ByteBuffer/wrap (.getBody data)))))

;; consumer class: https://learn.microsoft.com/en-us/java/api/com.azure.messaging.eventhubs.eventhubconsumerclient?view=azure-java-stable
(defrecord EventHubLog [^INotifyingSubscriberHandler subscriber-handler
                        ^EventHubProducerAsyncClient async-producer
                        ^EventHubConsumerClient consumer
                        ^Duration max-wait-time
                        ^Duration poll-sleep-duration]
  Log
  (appendRecord [_ record]
    (let [fut (CompletableFuture.)
          event-data (EventData. record)]
      (-> (.send async-producer [event-data] producer-send-options)
          (.subscribe nil
                      (->consumer (fn [e]
                                    (.completeExceptionally fut e)))
                      (fn [] (let [{:keys [sequence-number timestamp]} (get-partition-properties consumer)]
                               (.complete fut (serde/->TxKey sequence-number timestamp))))))
      fut))

  (readRecords [_ after-tx-id limit]
    (let [event-position (if after-tx-id
                           (EventPosition/fromSequenceNumber after-tx-id)
                           (EventPosition/earliest))]
      (try
        (->> (.receiveFromPartition consumer "0" limit event-position max-wait-time)
             (mapv ->log-record))
        (catch RuntimeException e
          (if-let [cause (.getCause e)]
            (throw cause)
            (throw e))))))

  (subscribe [this after-tx-id subscriber]
    (log/handle-polling-subscription this after-tx-id {:poll-sleep-duration poll-sleep-duration} subscriber))

  Closeable
  (close [_]
    (util/try-close consumer)
    (util/try-close async-producer)))
