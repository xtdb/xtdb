(ns crux.follower
  (:require [clojure.tools.logging :as log]
            [crux.kafka :as k])
  (:import java.io.Closeable
           java.time.Duration
           org.apache.kafka.clients.consumer.KafkaConsumer))

(defrecord Follower [producer
                     running?
                     consumer
                     options]
  Closeable
  (close [_] (reset! running? false)))

(defn poll-and-process-records
  [{:keys [^KafkaConsumer consumer options]}]
  (let [records (.poll consumer (Duration/ofMillis (get options :timeout 1000)))]
    (println "processing records: " (map bean records))))

(defn start-worker
  [{:keys [running?] :as follower}]
  (doto (Thread.
          (fn []
            (while @running?
              (try
                #_(poll-and-process-records follower)
                (catch Throwable t
                  (log/error t "Error reading from follower topics")
                  (Thread/sleep 500))))))
    (.start)))

(defn ^Closeable create-follower
  [indexer producer {:keys [follow-topics bootstrap-servers group-id] :as options}]
  (let [consumer (k/create-consumer {"bootstrap.servers" bootstrap-servers
                                     "group.id" group-id})]
    (when follow-topics
      (k/subscribe-from-stored-offsets indexer consumer follow-topics))
    (doto (map->Follower
            {:running? (atom true)
             :producer producer
             :options options
             :consumer consumer})
      (start-worker))))
