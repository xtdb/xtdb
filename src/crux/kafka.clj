(ns crux.kafka
  "Currently uses nippy to play nice with RDF IRIs that are not
  valid keywords. Uses one transaction per message."
  (:require [crux.db :as db]
            [crux.rdf :as rdf]
            [crux.kafka.nippy])
  (:import [java.util List Map Date]
           [java.util.concurrent ExecutionException]
           [org.apache.kafka.clients.admin
            AdminClient NewTopic]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer
            KafkaConsumer ConsumerRecord ConsumerRebalanceListener]
           [org.apache.kafka.clients.producer
            KafkaProducer ProducerRecord]
           [org.apache.kafka.common.errors
            TopicExistsException]
           [crux.kafka.nippy
            NippySerializer NippyDeserializer]))

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
            (throw e)))))))

;;; Transacting Producer

(defn transact [^KafkaProducer producer ^String topic entities]
  (->> (ProducerRecord. topic nil entities)
       (.send producer)))

(defn transact-ntriples [producer in topic tx-size]
  (->> (rdf/ntriples-seq in)
       (rdf/statements->maps)
       (map #(rdf/use-default-language % rdf/*default-language*))
       (map rdf/use-iri-as-id)
       (partition-all tx-size)
       (reduce (fn [^long n entities]
                 (transact producer topic entities)
                 (+ n (count entities)))
               0)))

;;; Indexing Consumer

(defn consumer-record->value [^ConsumerRecord record]
  (.value record))

(defn topic-partition-meta-key [^TopicPartition partition]
  (keyword "crux.kafka.topic-partition" (str partition)))

(defn store-topic-partition-offsets [indexer ^KafkaConsumer consumer partitions]
  (doseq [^TopicPartition partition partitions]
    (db/store-index-meta indexer
                         (topic-partition-meta-key partition)
                         (.position consumer partition))))

(defn seek-to-stored-offsets [indexer ^KafkaConsumer consumer partitions]
  (doseq [^TopicPartition partition partitions]
    (if-let [offset (db/read-index-meta indexer (topic-partition-meta-key partition))]
      (.seek consumer partition offset)
      (.seekToBeginning consumer [partition]))))

(defn index-tx-record [indexer ^ConsumerRecord record]
  (let [transact-time (Date. (.timestamp record))
        txs (consumer-record->value record)]
    (db/index indexer txs transact-time)
    txs))

(defn consume-and-index-entities
  ([indexer consumer]
   (consume-and-index-entities indexer consumer 10000))
  ([indexer ^KafkaConsumer consumer timeout]
   (let [records (.poll consumer timeout)
         txs (->> (for [^ConsumerRecord record records]
                    (index-tx-record indexer record))
                  (reduce into []))]
     (store-topic-partition-offsets indexer consumer (.partitions records))
     txs)))

(defn subscribe-from-stored-offsets [indexer ^KafkaConsumer consumer topic]
  (let [topics [topic]]
    (.subscribe consumer
                ^List topics
                (reify ConsumerRebalanceListener
                  (onPartitionsRevoked [_ partitions]
                    (store-topic-partition-offsets indexer consumer partitions))
                  (onPartitionsAssigned [_ partitions]
                    (seek-to-stored-offsets indexer consumer partitions))))))
