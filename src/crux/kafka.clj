(ns crux.kafka
  "Currently uses nippy to play nice with RDF IRIs that are not
  valid keywords.

  Would rather use LogAppendTime, but this is not consistent across a
  transaction. Alternative is to make each transaction a single
  message?"
  (:require [taoensso.nippy :as nippy])
  (:import [java.util Map Date UUID]
           [org.apache.kafka.clients.consumer
            KafkaConsumer ConsumerRecord]
           [org.apache.kafka.clients.producer
            KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization
            ByteArrayDeserializer ByteArraySerializer]))

(def default-producer-config
  {"key.serializer" (.getName ByteArraySerializer)
   "value.serializer" (.getName ByteArraySerializer)})

(def default-consumer-config
  {"enable.auto.commit" "false"
   "isolation.level" "read_committed"
   "key.deserializer" (.getName ByteArrayDeserializer)
   "value.deserializer" (.getName ByteArrayDeserializer)})

(defn ^KafkaProducer create-producer [config]
  (KafkaProducer. ^Map (merge default-producer-config config)))

(defn ^KafkaConsumer create-consumer [config]
  (KafkaConsumer. ^Map (merge default-consumer-config config)))

(defn consumer-record->entity [^ConsumerRecord record]
  (nippy/thaw (.value record)))

(defn transact [^KafkaProducer producer ^String topic entities]
  (try
    (.beginTransaction producer)
    (let [transact-time (Date.)
          transact-time-ms ^Long(.getTime transact-time)
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
