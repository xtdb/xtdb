(ns crux.kafka
  "Currently uses JSON to play nice with RDF IRIs that are not
  valid keywords.

  Would rather use LogAppendTime, but this is not consistent across a
  transaction. Alternative is to make each transaction a single
  message?"
  (:require [cheshire.core :as json])
  (:import [java.util Map Date UUID]
           [java.time OffsetDateTime]
           [org.apache.kafka.clients.consumer
            KafkaConsumer ConsumerRecord]
           [org.apache.kafka.clients.producer
            KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization
            StringDeserializer
            StringSerializer]))

(def default-producer-config
  {"key.serializer" (.getName StringSerializer)
   "value.serializer" (.getName StringSerializer)})

(def default-consumer-config
  {"enable.auto.commit" "false"
   "isolation.level" "read_committed"
   "key.deserializer" (.getName StringDeserializer)
   "value.deserializer" (.getName StringDeserializer)})

(defn ^KafkaProducer create-producer [config]
  (KafkaProducer. ^Map (merge default-producer-config config)))

(defn ^KafkaConsumer create-consumer [config]
  (KafkaConsumer. ^Map (merge default-consumer-config config)))

(defn parse-iso-datetime [s]
  (some->> s
           OffsetDateTime/parse
           .toInstant
           Date/from))

(defn consumer-record->entity [^ConsumerRecord record]
  (-> (json/parse-string (.value record) true)
      (update :crux.db/business-time parse-iso-datetime)
      (update :crux.db/transact-time parse-iso-datetime)
      (update :crux.db/transact-id #(UUID/fromString %))))

(defn transact [^KafkaProducer producer topic entities]
  (try
    (.beginTransaction producer)
    (let [transact-time (Date.)
          transact-id (UUID/randomUUID)]
      (doseq [entity entities
              :let [entity (assoc entity
                                  :crux.db/transact-id transact-id
                                  :crux.db/transact-time transact-time
                                  :crux.db/business-time (:crux.db/business-time entity transact-time))]]
        (.send producer (ProducerRecord. ^String topic
                                         nil
                                         ^Long (.getTime transact-time)
                                         (:crux.db/id entity)
                                         (json/generate-string entity)))))
    (.commitTransaction producer)
    (catch Throwable t
      (.abortTransaction producer)
      (throw t))))
