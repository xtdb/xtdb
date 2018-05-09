(ns crux.kafka
  "Currently uses nippy to play nice with RDF IRIs that are not
  valid keywords.

  Would rather use LogAppendTime, but this is not consistent across a
  transaction. Alternative is to make each transaction a single
  message?"
  (:require [taoensso.nippy :as nippy]
            [crux.kv :as kv])
  (:import [java.util Map Date UUID]
           [org.apache.kafka.clients.admin
            AdminClient NewTopic]
           [org.apache.kafka.clients.consumer
            KafkaConsumer ConsumerRecord]
           [org.apache.kafka.clients.producer
            KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization
            ByteArrayDeserializer ByteArraySerializer]
           [org.apache.kafka.streams.kstream
            ValueMapper KeyValueMapper]))

(def default-producer-config
  {"enable.idempotence" "true"
   "acks" "all"
   "key.serializer" (.getName ByteArraySerializer)
   "value.serializer" (.getName ByteArraySerializer)})

(def default-consumer-config
  {"enable.auto.commit" "false"
   "isolation.level" "read_committed"
   "auto.offset.reset" "earliest"
   "key.deserializer" (.getName ByteArrayDeserializer)
   "value.deserializer" (.getName ByteArrayDeserializer)})

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
      (doseq [{:crux.kv/keys [id business-time] :as entity} entities]
        (.send producer (->> (assoc entity
                                    :crux.kv/transact-id transact-id
                                    :crux.kv/transact-time transact-time
                                    :crux.kv/business-time (or business-time transact-time))
                             nippy/freeze
                             (ProducerRecord. topic nil transact-time-ms (nippy/freeze id))))))
    (.commitTransaction producer)
    (catch Throwable t
      (.abortTransaction producer)
      (throw t))))

;;; Indexing Consumer

(defn consumer-record->entity [^ConsumerRecord record]
  (nippy/thaw (.value record)))

(defn entities->txs [entities]
  (for [entity entities]
    (-> entity
        (assoc :crux.kv/id (- (Math/abs (long (hash (:crux.rdf/iri entity))))))
        (dissoc :crux.kv/transact-id
                :crux.kv/transact-time
                :crux.kv/business-time))))

(defn index-entities [db entities]
  (doseq [[tx entities] (group-by :crux.kv/transact-id entities)]
    (kv/-put db
             (entities->txs entities)
             (:crux.kv/transact-time (first entities)))))

(defn consume-and-index-entities [db ^KafkaConsumer consumer]
  (let [entities (map consumer-record->entity (.poll consumer 1000))]
    (index-entities db entities)
    ;; TODO: this offsets should be written to the db so it can be
    ;; backed up and reused.
    (.commitSync consumer)
    entities))
