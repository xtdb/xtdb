(ns crux.follower
  (:require [clojure.tools.logging :as log]
            [crux.kafka :as k])
  (:import java.io.Closeable
           java.time.Duration
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.common.serialization.ByteArrayDeserializer))

(defrecord Follower [producer
                     indexer
                     running?
                     consumer
                     options
                     ^Thread worker-thread]
  Closeable
  (close [_]
    (reset! running? false)))

(defn poll-and-process-records
  [{:keys [options]} ^KafkaConsumer consumer]
  (let [records (.poll consumer (Duration/ofMillis (get options :timeout 100)))]
    (println "processing records: " (count (seq records)))))

(defn main-loop
  [{:keys [running? indexer options] :as follower}]
  (with-open [consumer (k/create-consumer
                         {"bootstrap.servers" (:bootstrap-servers options)
                          "group.id" (:group-id options)
                          "key.deserializer" (.getName ByteArrayDeserializer)
                          "value.deserializer" (.getName ByteArrayDeserializer)})]
    (when-let [follow-topics (:follow-topics options)]
      (k/subscribe-from-stored-offsets indexer consumer follow-topics)
      (while @running?
        (try
          (poll-and-process-records follower consumer)
          (catch Throwable t
            (log/error t "Error reading from follower topics")
            (throw t)))))))

(defn ^Closeable create-follower
  [indexer producer {:keys [follow-topics bootstrap-servers group-id] :as options}]
  (let [follower (map->Follower
                   {:running? (atom true)
                    :producer producer
                    :indexer indexer
                    :options options})]
    (assoc
      follower
      :worker-thread
      (doto (Thread. ^Runnable (partial main-loop follower))
        (.start)))))
