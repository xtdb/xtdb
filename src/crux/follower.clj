(ns crux.follower
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.kafka :as k]
            [clojure.string :as str])
  (:import java.io.Closeable
           java.time.Duration
           java.util.UUID
           [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           org.apache.kafka.common.serialization.ByteArrayDeserializer))

(defmulti process-format
  (fn [record format] format))

(defn flatten-keys
  "transforms a map with nested objects to
   encode the nested path with compound keys instead

   so a map like {:a {:b 2}} becomes {:a.b 2} instead"
  [input-map]
  (letfn [(paths [position]
            (mapcat
              (fn [key]
                (if (map? (get position key))
                  (for [p (paths (get position key))]
                    (cons key p))
                  [[key]]))
              (keys position)))]
    (reduce
      (fn [new-map path]
        (let [new-path (keyword (str/join "." (map name path)))]
          (assoc new-map new-path (get-in input-map path))))
      {}
      (paths input-map))))

(defmethod process-format "json"
  [^ConsumerRecord record _]
  (let [id (keyword (str (UUID/randomUUID)))]
    [[:crux.tx/put
      id
      (assoc
        (-> (String. ^bytes (.value record))
            (json/parse-string  keyword)
            (flatten-keys))
        :crux.db/id id
        :crux.follower/topic (.topic record)
        :crux.follower/partition (.partition record)
        :crux.follower/key (String. (.value record)))]]))

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
                           (get-in options [:follow-topics (.topic record) :format]))]]
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
