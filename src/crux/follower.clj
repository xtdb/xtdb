(ns crux.follower
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.kafka :as k])
  (:import java.io.Closeable
           java.time.Duration
           java.util.UUID
           [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           org.apache.kafka.common.serialization.ByteArrayDeserializer))

(defmulti process-format
  (fn [record format] format))

(defmethod process-format "json"
  [^ConsumerRecord record _]
  (let [id (keyword (str (UUID/randomUUID)))]
    [[:crux.tx/put
      id
      (assoc
        (json/parse-string (String. ^bytes (.value record)) keyword)
        :crux.db/id id)]]))

(defrecord Follower [tx-log
                     indexer
                     running?
                     consumer
                     options
                     ^Thread worker-thread]
  Closeable
  (close [_]
    (reset! running? false)))

(defn poll-and-process-records
  [{:keys [tx-log options]} ^KafkaConsumer consumer]
  (let [records (.poll consumer (Duration/ofMillis (get options :timeout 100)))]
    (doseq [^ConsumerRecord record records
            :let [tx-ops (process-format
                           record
                           (get-in options [:follow-topics (.topic record)]))]]
      @(db/submit-tx tx-log tx-ops))))

(defn main-loop
  [{:keys [running? indexer options] :as follower}]
  (with-open [consumer (k/create-consumer
                         {"bootstrap.servers" (:bootstrap-servers options)
                          "group.id" (:group-id options)
                          "key.deserializer" (.getName ByteArrayDeserializer)
                          "value.deserializer" (.getName ByteArrayDeserializer)})]
    (when-let [follow-topics (:follow-topics options)]
      (k/subscribe-from-stored-offsets indexer consumer (keys follow-topics))
      (while @running?
        (try
          (poll-and-process-records follower consumer)
          (catch Throwable t
            (log/error t "Error reading from follower topics")
            (throw t)))))))

(defn ^Closeable create-follower
  [indexer tx-log {:keys [follow-topics bootstrap-servers group-id] :as options}]
  (let [follower (map->Follower
                   {:running? (atom true)
                    :tx-log tx-log
                    :indexer indexer
                    :options options})]
    (assoc
      follower
      :worker-thread
      (doto (Thread. ^Runnable (partial main-loop follower))
        (.start)))))
