(ns crux.kafka
  "Currently uses nippy to play nice with RDF IRIs that are not
  valid keywords. Uses one transaction per message."
  (:require [taoensso.nippy :as nippy]
            [crux.kv :as cr])
  (:import [java.util List Map Date]
           [java.util.concurrent ExecutionException]
           [org.apache.kafka.clients.admin
            AdminClient NewTopic]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer
            KafkaConsumer ConsumerRecord ConsumerRebalanceListener]
           [org.apache.kafka.clients.producer
            KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization
            Deserializer Serializer]
           [org.apache.kafka.common.errors
            TopicExistsException]))

(deftype NippySerializer []
  Serializer
  (close [_])
  (configure [_ _ _])
  (serialize [_ _ data]
    (nippy/freeze data)))

(deftype NippyDeserializer []
  Deserializer
  (close [_])
  (configure [_ _ _])
  (deserialize [_ _ data]
    (nippy/thaw data)))

(def default-producer-config
  {"enable.idempotence" "true"
   "acks" "all"
   "key.serializer" (.getName crux.kafka.NippySerializer)
   "value.serializer" (.getName crux.kafka.NippySerializer)})

(def default-consumer-config
  {"enable.auto.commit" "false"
   "isolation.level" "read_committed"
   "auto.offset.reset" "earliest"
   "key.deserializer" (.getName crux.kafka.NippyDeserializer)
   "value.deserializer" (.getName crux.kafka.NippyDeserializer)})

(def default-topic-config
  {"message.timestamp.type" "LogAppendTime"})

(defn ^KafkaProducer create-producer [config]
  (KafkaProducer. ^Map (merge default-producer-config config)))

(defn ^KafkaConsumer create-consumer [config]
  (KafkaConsumer. ^Map (merge default-consumer-config config)))

(defn ^AdminClient create-admin-client [config]
  (AdminClient/create ^Map config))

(defn create-topic [^AdminClient admin-client topic num-partitions replication-factor config]
  (let [new-topic (doto (NewTopic. topic num-partitions replication-factor)
                    (.configs (merge default-topic-config config)))]
    (try
      @(.all (.createTopics admin-client [new-topic]))
      (catch ExecutionException e
        (let [cause (.getCause e)]
          (when-not (instance? TopicExistsException cause)
            (throw cause)))))))

(defn consumer-record->value [^ConsumerRecord record]
  (.value record))

;;; Transacting Producer

(defn transact [^KafkaProducer producer ^String topic entities]
  (->> (ProducerRecord. topic nil entities)
       (.send producer)))

;;; Indexing Consumer

(defn entities->txs [entities]
  (for [entity entities]
    (assoc entity :crux.kv/id (- (Math/abs (long (hash (:crux.rdf/iri entity))))))))

(defn topic-partition-meta-key [^TopicPartition partition]
  (keyword "crux.kafka.topic-partition" (str partition)))

(defn store-topic-partition-offsets [kv ^KafkaConsumer consumer partitions]
  (doseq [^TopicPartition partition partitions]
    (cr/store-meta kv
                   (topic-partition-meta-key partition)
                   (.position consumer partition))))

(defn seek-to-stored-offsets [kv ^KafkaConsumer consumer partitions]
  (doseq [^TopicPartition partition partitions]
    (if-let [offset (cr/get-meta kv (topic-partition-meta-key partition))]
      (.seek consumer partition offset)
      (.seekToBeginning consumer [partition]))))

(defn index-tx-record [kv ^ConsumerRecord record]
  (let [transact-time (Date. (.timestamp record))
        transact-id [(topic-partition-meta-key (.partition record))
                     (.offset record)]
        txs (for [tx (entities->txs (consumer-record->value record))]
              (assoc tx
                     :crux.tx/transact-id (pr-str transact-id)
                     :crux.tx/transact-time transact-time
                     :crux.tx/business-time (or (:crux.tx/business-time tx) transact-time)))]
    (cr/-put kv txs transact-time)
    txs))

(defn consume-and-index-entities [kv ^KafkaConsumer consumer]
  (let [records (.poll consumer 10000)
        entities (->> (for [^ConsumerRecord record records]
                        (index-tx-record kv record))
                      (reduce into []))]
    (store-topic-partition-offsets kv consumer (.partitions records))
    entities))

(defn subscribe-from-stored-offsets [kv ^KafkaConsumer consumer topic]
  (let [topics [topic]]
    (.subscribe consumer
                ^List topics
                (reify ConsumerRebalanceListener
                  (onPartitionsRevoked [_ partitions]
                    (store-topic-partition-offsets kv consumer partitions))
                  (onPartitionsAssigned [_ partitions]
                    (seek-to-stored-offsets kv consumer partitions))))))
