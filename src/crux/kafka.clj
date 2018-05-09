(ns crux.kafka
  "Currently uses nippy to play nice with RDF IRIs that are not
  valid keywords.

  Would rather use LogAppendTime, but this is not consistent across a
  transaction. Alternative is to make each transaction a single
  message?"
  (:require [taoensso.nippy :as nippy]
            [crux.kv :as cr])
  (:import [java.util Map Date UUID]
           [org.apache.kafka.clients.admin
            AdminClient NewTopic]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer
            KafkaConsumer ConsumerRecord ConsumerRecords]
           [org.apache.kafka.clients.producer
            KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization
            Deserializer Serializer]
           [org.apache.kafka.streams.kstream
            ValueMapper KeyValueMapper]))

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

(defn ^KafkaProducer create-producer [config]
  (KafkaProducer. ^Map (merge default-producer-config config)))

(defn ^KafkaConsumer create-consumer [config]
  (KafkaConsumer. ^Map (merge default-consumer-config config)))

(defn ^AdminClient create-admin-client [config]
  (AdminClient/create ^Map config))

(defn create-topic [^AdminClient admin-client topic num-partitions replication-factor config]
  (let [new-topic (doto (NewTopic. topic num-partitions replication-factor)
                    (.configs config))]
    @(.all (.createTopics admin-client [new-topic]))))

(defn consumer-record->value [^ConsumerRecord record]
  (.value record))

;;; Streams

(defn ^ValueMapper value-mapper [f]
  (reify ValueMapper
    (apply [_ v]
      (f v))))

(defn ^KeyValueMapper key-value-mapper [f]
  (reify KeyValueMapper
    (apply [_ k v]
      (f k v))))

;;; Transacting Producer

(defn transact [^KafkaProducer producer ^String topic entities]
  (try
    (.beginTransaction producer)
    (let [transact-time (Date.)
          transact-time-ms ^Long (.getTime transact-time)
          transact-id (UUID/randomUUID)]
      (doseq [entity entities]
        (->> (assoc entity
                    :crux.tx/transact-id transact-id
                    :crux.tx/transact-time transact-time
                    :crux.tx/business-time (or (:crux.tx/business-time entity) transact-time))
             (ProducerRecord. topic nil transact-time-ms (:crux.rdf/iri entity))
             (.send producer))))
    (.commitTransaction producer)
    (catch Throwable t
      (.abortTransaction producer)
      (throw t))))

;;; Indexing Consumer

(defn entities->txs [entities]
  (for [entity entities]
    (-> entity
        (assoc :crux.kv/id (- (Math/abs (long (hash (:crux.rdf/iri entity))))))
        (dissoc :crux.tx/transact-id
                :crux.tx/transact-time
                :crux.tx/business-time))))

(defn topic-partition-meta-key [^TopicPartition partition]
  (keyword "crux.kafka.topic-partition"
           (str (.topic partition) "_" (.partition partition))))

(defn store-topic-partition-offsets [kv ^ConsumerRecords records]
  (doseq [^TopicPartition partition (.partitions records)
          :let [^ConsumerRecord record (last (.records records partition))]]
    (cr/store-meta kv
                   (topic-partition-meta-key partition)
                   (inc (.offset record)))))

(defn seek-to-stored-offsets [kv ^KafkaConsumer consumer]
  (doseq [^TopicPartition partition (.assignment consumer)
          :let [offset (cr/get-meta kv (topic-partition-meta-key partition))]]
    (if offset
      (.seek consumer partition offset)
      (.seekToBeginning consumer [partition]))))

(defn index-entities [kv entities]
  (doseq [[tx-id entities] (group-by :crux.tx/transact-id entities)]
    (cr/-put kv
             (entities->txs entities)
             (:crux.tx/transact-time (first entities)))))

(defn consume-and-index-entities [kv ^KafkaConsumer consumer]
  (let [records (.poll consumer 1000)
        entities (map consumer-record->value records)]
    (index-entities kv entities)
    (store-topic-partition-offsets kv records)
    entities))
