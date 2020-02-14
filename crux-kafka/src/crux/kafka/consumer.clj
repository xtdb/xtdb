(ns crux.kafka.consumer
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.tx.consumer :as tc])
  (:import crux.kafka.nippy.NippyDeserializer
           java.io.Closeable
           java.time.Duration
           [java.util List Map]
           [org.apache.kafka.clients.consumer ConsumerRebalanceListener ConsumerRecord KafkaConsumer]
           org.apache.kafka.common.TopicPartition))

(def default-consumer-config
  {"enable.auto.commit" "false"
   "isolation.level" "read_committed"
   "auto.offset.reset" "earliest"
   "key.deserializer" (.getName NippyDeserializer)
   "value.deserializer" (.getName NippyDeserializer)})

(defn create-consumer
  ^org.apache.kafka.clients.consumer.KafkaConsumer [config]
  (KafkaConsumer. ^Map (merge default-consumer-config config)))

(defprotocol Offsets
  (read-offsets [this])
  (store-offsets [this offsets]))

(defrecord TxOffset [indexer]
  Offsets
  (read-offsets [this]
    {:next-offset (inc (get (db/read-index-meta indexer :crux.tx/latest-completed-tx) :crux.tx/tx-id 0))})

  ;; no-op - this is stored by the indexer itself
  (store-offsets [this offsets]))

(defrecord ConsumerOffsets [indexer k]
  Offsets
  (read-offsets [this]
    (db/read-index-meta indexer k))
  (store-offsets [this offsets]
    (db/store-index-meta indexer k offsets)))

(defn- topic-partition-meta-key [^TopicPartition partition]
  (keyword "crux.kafka.topic-partition" (str partition)))

(defn seek-to-stored-offsets [offsets ^KafkaConsumer consumer partitions]
  (let [consumer-state (read-offsets offsets)]
    (doseq [^TopicPartition partition partitions]
      (if-let [offset (get-in consumer-state [(topic-partition-meta-key partition) :next-offset])]
        (.seek consumer partition (long offset))
        (.seekToBeginning consumer [partition])))))

(defn- update-stored-consumer-state [offsets ^KafkaConsumer consumer records]
  (when (seq records)
    (let [partition->records (group-by (fn [^ConsumerRecord r]
                                         (TopicPartition. (.topic r)
                                                          (.partition r))) records)
          partitions (vec (keys partition->records))
          stored-consumer-state (or (read-offsets offsets) {})
          consumer-state (->> (for [^TopicPartition partition partitions
                                    :let [^ConsumerRecord last-record-in-batch (->> (get partition->records partition)
                                                                                    (sort-by #(.offset ^ConsumerRecord %))
                                                                                    (last))
                                          next-offset (inc (.offset last-record-in-batch))]]
                                [(topic-partition-meta-key partition)
                                 {:next-offset next-offset}])
                              (into stored-consumer-state))]
      (store-offsets offsets consumer-state))))

(defn- prune-consumer-state [offsets ^KafkaConsumer consumer partitions]
  (let [consumer-state (read-offsets offsets)]
    (->> (for [^TopicPartition partition partitions
               :let [partition-key (topic-partition-meta-key partition)
                     next-offset (or (get-in consumer-state [partition-key :next-offset]) 0)]]
           [partition-key {:next-offset next-offset}])
         (into {})
         (store-offsets offsets))))

;; TODO: This works as long as each node has a unique consumer group
;; id, if not the node will only get a subset of the doc-topic. The
;; tx-topic is always only one partition.
(defn subscribe-from-stored-offsets
  [offsets ^KafkaConsumer consumer ^List topics]
  (.subscribe consumer
              topics
              (reify ConsumerRebalanceListener
                (onPartitionsRevoked [_ partitions]
                  (log/info "Partitions revoked:" (str partitions)))
                (onPartitionsAssigned [_ partitions]
                  (log/info "Partitions assigned:" (str partitions))
                  (prune-consumer-state offsets consumer partitions)
                  (seek-to-stored-offsets offsets consumer partitions)))))

(defn consume
  [{:keys [offsets indexer timeout topic ^KafkaConsumer consumer index-fn]}]
  (let [records (.poll consumer (Duration/ofMillis timeout))
        records (vec (.records records (str topic)))]
    (index-fn records)
    (update-stored-consumer-state offsets consumer records)
    (count records)))

(defn consume-and-block
  [{:keys [offsets indexer pending-records-state timeout topic ^KafkaConsumer consumer accept-fn index-fn]}]
  (let [_ (when (empty? @pending-records-state)
            (reset! pending-records-state (let [records (.poll consumer (Duration/ofMillis timeout))]
                                            (vec (.records records (str topic))))))
        records (->> @pending-records-state
                     (take-while accept-fn)
                     (vec))]
    (index-fn records)
    (update-stored-consumer-state offsets consumer records)
    (swap! pending-records-state (comp vec (partial drop (count records))))
    (when (seq @pending-records-state)
      (log/debug "Blocked processing" (count pending-records-state) topic "records"))
    (count records)))

(defn start-indexing-consumer
  ^java.io.Closeable
  [{:keys [indexer offsets kafka-config group-id topic accept-fn index-fn]}]
  (let [consumer-config (merge {"group.id" group-id} kafka-config)
        pending-records (atom [])
        consumer (create-consumer consumer-config)
        _ (subscribe-from-stored-offsets offsets consumer [topic])
        index-fn (fn []
                   (try
                     (let [opts {:indexer indexer
                                 :consumer consumer
                                 :topic topic
                                 :offsets offsets
                                 :timeout 1000
                                 :index-fn index-fn}]
                       (if accept-fn
                         (consume-and-block (merge opts {:pending-records-state pending-records
                                                         :accept-fn accept-fn}))
                         (consume opts)))
                     (catch Exception e
                       (log/error e "Error while consuming and indexing from Kafka:")
                       (Thread/sleep 500))))
        t (tc/start-indexing-consumer {:index-fn index-fn})]
    (reify Closeable
      (close [_]
        (.close t)
        (.close consumer)))))
