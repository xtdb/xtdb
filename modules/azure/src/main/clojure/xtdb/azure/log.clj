(ns xtdb.azure.log
  (:require [xtdb.api.protocols :as xtp]
            [xtdb.log :as log]
            [xtdb.util :as util])
  (:import java.util.concurrent.CompletableFuture
           java.nio.ByteBuffer
           java.time.Duration
           java.io.Closeable
           [xtdb.log INotifyingSubscriberHandler Log LogRecord]
           [com.azure.messaging.eventhubs EventData EventHubProducerAsyncClient EventHubConsumerClient]
           [com.azure.messaging.eventhubs.models SendOptions EventPosition PartitionEvent]))

(defn ->consumer [f]
  (reify java.util.function.Consumer
    (accept
     [_ val]
     (f val))))

(defn event-data->tx-instant [data]
  (xtp/->TransactionInstant (.getOffset data) (.getEnqueuedTime data)))

(def producer-send-options
  (.setPartitionId (SendOptions.) "0"))

(defn get-partition-properties [consumer]
  (let [partition-properties (.getPartitionProperties consumer "0")]
    {:offset (Long/parseLong (.getLastEnqueuedOffset partition-properties))
     :timestamp (.getLastEnqueuedTime partition-properties)}))

(defn- ->log-record [^PartitionEvent event]
  (let [data ^EventData (.getData event)]
    (log/->LogRecord (event-data->tx-instant data) (ByteBuffer/wrap (.getBody data)))))

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
                      (fn [] (let [{:keys [offset timestamp]} (get-partition-properties consumer)]
                               (.complete fut (log/->LogRecord (xtp/->TransactionInstant offset timestamp) record))))))
      fut))

  (readRecords [_ after-tx-id limit]
    (let [event-position (if after-tx-id
                           (EventPosition/fromOffset after-tx-id)
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
