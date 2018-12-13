(ns crux.kafka
  (:require [crux.db :as db]
            [clojure.spec.alpha :as s]
            [crux.codec :as c]
            [crux.tx :as tx]
            [clojure.tools.logging :as log])
  (:import [crux.kafka.nippy NippyDeserializer NippySerializer]
           java.io.Closeable
           java.time.Duration
           [java.util Date List Map]
           java.util.concurrent.ExecutionException
           [org.apache.kafka.clients.admin AdminClient NewTopic TopicDescription]
           [org.apache.kafka.clients.consumer ConsumerRebalanceListener ConsumerRecord ConsumerRecords KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord RecordMetadata]
           org.apache.kafka.common.errors.TopicExistsException
           org.apache.kafka.common.TopicPartition))

(s/def ::bootstrap-servers (s/and string? #(re-matches #"\w+:\d+(,\w+:\d+)*" %)))
(s/def ::group-id string?)
(s/def ::topic string?)
(s/def ::partitions pos-int?)
(s/def ::replication-factor pos-int?)

(s/def ::tx-topic ::topic)
(s/def ::doc-topic ::topic)
(s/def ::doc-partitions ::partitions)
(s/def ::create-topics boolean?)

(def default-producer-config
  {"enable.idempotence" "true"
   "acks" "all"
   "key.serializer" (.getName NippySerializer)
   "value.serializer" (.getName NippySerializer)})

(def default-consumer-config
  {"enable.auto.commit" "false"
   "isolation.level" "read_committed"
   "auto.offset.reset" "earliest"
   "key.deserializer" (.getName NippyDeserializer)
   "value.deserializer" (.getName NippyDeserializer)})

(def default-topic-config
  {"message.timestamp.type" "LogAppendTime"})

(def tx-topic-config
  {"retention.ms" (str Long/MAX_VALUE)})

(def doc-topic-config
  {"cleanup.policy" "compact"})

(defn create-producer
  ^org.apache.kafka.clients.producer.KafkaProducer [config]
  (KafkaProducer. ^Map (merge default-producer-config config)))

(defn create-consumer
  ^org.apache.kafka.clients.consumer.KafkaConsumer [config]
  (KafkaConsumer. ^Map (merge default-consumer-config config)))

(defn create-admin-client
  ^org.apache.kafka.clients.admin.AdminClient [config]
  (AdminClient/create ^Map config))

(defn create-topic [^AdminClient admin-client topic num-partitions replication-factor config]
  (let [new-topic (doto (NewTopic. topic num-partitions replication-factor)
                    (.configs (merge default-topic-config config)))]
    (try
      @(.all (.createTopics admin-client [new-topic]))
      (catch ExecutionException e
        (let [cause (.getCause e)]
          (when-not (instance? TopicExistsException cause)
            (throw e)))))))

(defn zk-status [consumer-config]
  {:crux.zk/zk-active?
   (if consumer-config
     (try
       (with-open [^KafkaConsumer consumer
                   (create-consumer
                    (merge consumer-config {"default.api.timeout.ms" (int 1000)}))]
         (boolean (.listTopics consumer)))
       (catch Exception e
         (log/debug e "Could not list Kafka topics:")
         false))
     false)})

(defn tx-record->tx-log-entry [^ConsumerRecord record]
  {:crux.tx/tx-time (Date. (.timestamp record))
   :crux.tx/tx-ops (.value record)
   :crux.tx/tx-id (.offset record)})

;;; Transacting Producer

(defrecord KafkaTxLog [^KafkaProducer producer tx-topic doc-topic kafka-config]
  Closeable
  (close [_])

  db/TxLog
  (submit-doc [this content-hash doc]
    (->> (ProducerRecord. doc-topic content-hash doc)
         (.send producer)))

  (submit-tx [this tx-ops]
    (let [conformed-tx-ops (tx/conform-tx-ops tx-ops)]
      (doseq [doc (tx/tx-ops->docs tx-ops)]
        (db/submit-doc this (str (c/new-id doc)) doc))
      (let [tx-send-future (->> (ProducerRecord. tx-topic nil conformed-tx-ops)
                                (.send producer))]
        (delay
         (let [record-meta ^RecordMetadata @tx-send-future]
           {:crux.tx/tx-id (.offset record-meta)
            :crux.tx/tx-time (Date. (.timestamp record-meta))})))))

  (new-tx-log-context [this]
    (create-consumer (assoc kafka-config "enable.auto.commit" "false")))

  (tx-log [this tx-topic-consumer]
    (let [tx-topic-consumer ^KafkaConsumer tx-topic-consumer]
      (.assign tx-topic-consumer [(TopicPartition. tx-topic 0)])
      (.seekToBeginning tx-topic-consumer (.assignment tx-topic-consumer))
      ((fn step []
         (lazy-seq
          (when-let [records (seq (.poll tx-topic-consumer (Duration/ofMillis 1000)))]
            (concat (map tx-record->tx-log-entry records)
                    (step)))))))))

;;; Indexing Consumer

(defn consumer-record->value [^ConsumerRecord record]
  (.value record))

(defn- topic-partition-meta-key [^TopicPartition partition]
  (keyword "crux.kafka.topic-partition" (str partition)))

(defn consumer-status [indexer]
  {:crux.tx-log/consumer-state (db/read-index-meta indexer :crux.tx-log/consumer-state)})

(defn- update-stored-consumer-state [indexer ^KafkaConsumer consumer ^ConsumerRecords records]
  (let [partitions (.partitions records)
        end-offsets (.endOffsets consumer partitions)
        stored-consumer-state (or (db/read-index-meta indexer :crux.tx-log/consumer-state) {})
        consumer-state (->> (for [^TopicPartition partition partitions
                                  :let [position (.position consumer partition)
                                        latest-position (get end-offsets partition)
                                        lag (- latest-position position)]]
                              (do (when-not (zero? lag)
                                    (log/warn "Falling behind" (str partition) "at:" position "latest:" latest-position))
                                  [(topic-partition-meta-key partition)
                                   {:offset position
                                    :time (->>  (.records records partition)
                                                (map #(.timestamp ^ConsumerRecord %))
                                                (reduce max)
                                                (long)
                                                (Date.))
                                    :lag lag}]))
                            (into stored-consumer-state))]
    (db/store-index-meta indexer :crux.tx-log/consumer-state consumer-state)))

(defn- prune-consumer-state [indexer ^KafkaConsumer consumer partitions]
  (when-let [consumer-state (db/read-index-meta indexer :crux.tx-log/consumer-state)]
    (->> (for [^TopicPartition partition partitions]
           (topic-partition-meta-key partition))
         (select-keys consumer-state)
         (db/store-index-meta indexer :crux.tx-log/consumer-state))))

(defn seek-to-stored-offsets [indexer ^KafkaConsumer consumer partitions]
  (let [consumer-state (db/read-index-meta indexer :crux.tx-log/consumer-state)]
    (doseq [^TopicPartition partition partitions]
      (if-let [offset (get-in consumer-state [(topic-partition-meta-key partition) :offset])]
        (.seek consumer partition offset)
        (.seekToBeginning consumer [partition])))))

(defn- index-doc-record [indexer ^ConsumerRecord record]
  (let [content-hash (.key record)
        doc (.value record)]
    (db/index-doc indexer content-hash doc)
    doc))

(defn- index-tx-record [indexer ^ConsumerRecord record]
  (let [{:crux.tx/keys [tx-time
                        tx-ops
                        tx-id]} (tx-record->tx-log-entry record)]
    (db/index-tx indexer tx-ops tx-time tx-id)
    tx-ops))

(defn consume-and-index-entities
  [{:keys [indexer ^KafkaConsumer consumer
           follower timeout tx-topic doc-topic]
    :or   {timeout 10000}}]
  (let [records (.poll consumer (Duration/ofMillis timeout))
        result
        (reduce
         (fn [state ^ConsumerRecord record]
           (condp = (.topic record)
             tx-topic
             (do (index-tx-record indexer record)
                 (update state :txs inc))

             doc-topic
             (do (index-doc-record indexer record)
                 (update state :docs inc))

             (throw (ex-info "Unkown topic" {:topic (.topic record)}))))
         {:txs 0
          :docs 0}
         records)]
    (when (seq records)
      (update-stored-consumer-state indexer consumer records))
    result))

;; TODO: This works as long as each node has a unique consumer group
;; id, if not the node will only get a subset of the doc-topic. The
;; tx-topic is always only one partition.
(defn subscribe-from-stored-offsets
  [indexer ^KafkaConsumer consumer ^List topics]
  (.subscribe consumer
              topics
              (reify ConsumerRebalanceListener
                (onPartitionsRevoked [_ partitions])
                (onPartitionsAssigned [_ partitions]
                  (prune-consumer-state indexer consumer partitions)
                  (seek-to-stored-offsets indexer consumer partitions)))))

(defrecord IndexingConsumer [running? ^Thread worker-thread consumer-config indexer options]
  Closeable
  (close [_]
    (reset! running? false)
    (.join worker-thread)))

(defn- indexing-consumer-thread-main-loop
  [{:keys [running? indexer consumer-config options]}]
  (with-open [consumer (create-consumer consumer-config)]
    (subscribe-from-stored-offsets
     indexer consumer [(:tx-topic options) (:doc-topic options)])
    (while @running?
      (try
        (consume-and-index-entities
         {:indexer indexer
          :consumer consumer
          :timeout 100
          :tx-topic (:tx-topic options)
          :doc-topic (:doc-topic options)})
        (catch Exception e
          (log/error e "Error while consuming and indexing from Kafka:")
          (Thread/sleep 500))))))

(defn- ensure-tx-topic-has-single-partition [^AdminClient admin-client tx-topic]
  (let [name->description @(.all (.describeTopics admin-client [tx-topic]))]
    (assert (= 1 (count (.partitions ^TopicDescription (get name->description tx-topic)))))))

(defn start-indexing-consumer
  ^java.io.Closeable
  [admin-client consumer-config indexer
   {:keys [tx-topic
           replication-factor
           doc-partitions
           doc-topic
           create-topics] :as options}]
  (when create-topics
    (create-topic admin-client tx-topic 1 replication-factor tx-topic-config)
    (create-topic admin-client doc-topic doc-partitions
                  replication-factor doc-topic-config))
  (ensure-tx-topic-has-single-partition admin-client tx-topic)
  (let [indexing-consumer (map->IndexingConsumer {:running? (atom true)
                                                  :indexer indexer
                                                  :consumer-config consumer-config
                                                  :options options})]
    (assoc
     indexing-consumer
     :worker-thread
     (doto (Thread. ^Runnable (partial indexing-consumer-thread-main-loop indexing-consumer)
                    "crux.kafka.indexing-consumer-thread")
       (.start)))))
