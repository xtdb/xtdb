(ns crux.kafka
  "Currently uses nippy to play nice with RDF IRIs that are not
  valid keywords. Uses one transaction per message."
  (:require [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [crux.db :as db]
            [crux.doc :as doc]
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

(def tx-topic-config
  {"retention.ms" (str Long/MAX_VALUE)})

(def doc-topic-config
  {"cleanup.policy" "compact"})

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

(defn transact [^KafkaProducer producer ^String tx-topic ^String doc-topic tx-ops]
  (let [record-tx-ops (s/conform :crux.doc/tx-ops tx-ops)]
    (when (s/invalid? record-tx-ops)
      (throw (ex-info "Invalid input" (s/explain-data :crux.doc/tx-ops tx-ops))))
    (doseq [tx-op tx-ops
            doc (filter map? tx-op)]
      (->> (ProducerRecord. doc-topic (str (doc/doc->content-hash doc)) doc)
           (.send producer)))
    (->> (ProducerRecord. tx-topic nil record-tx-ops)
         (.send producer))))

(def ^:dynamic *ntriples-log-size* 100000)

(defn transact-ntriples [producer in tx-topic doc-topic tx-size]
  (->> (rdf/ntriples-seq in)
       (rdf/statements->maps)
       (map #(rdf/use-default-language % rdf/*default-language*))
       (map rdf/use-iri-as-id)
       (partition-all tx-size)
       (reduce (fn [^long n entities]
                 (when (zero? (long (mod n *ntriples-log-size*)))
                   (log/debug "transacted" n))
                 (let [tx-ops (for [entity entities]
                                [:crux.tx/put (:crux.rdf/iri entity) entity])]
                   (transact producer tx-topic doc-topic tx-ops))
                 (+ n (count entities)))
               0)))

;;; Indexing Consumer

(defn consumer-record->value [^ConsumerRecord record]
  (.value record))

(defn- topic-partition-meta-key [^TopicPartition partition]
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

(defn- index-doc-record [indexer ^ConsumerRecord record]
  (let [content-hash (doc/id->bytes (.key record))
        doc (consumer-record->value record)]
    (db/index-doc indexer content-hash doc)
    doc))

(defn- index-tx-record [indexer ^ConsumerRecord record]
  (let [tx-time (Date. (.timestamp record))
        tx-ops (consumer-record->value record)
        tx-id (inc (.offset record))]
    (db/index-tx indexer tx-ops tx-time tx-id)
    tx-ops))

(defn- tx-record? [^ConsumerRecord record]
  (nil? (.key record)))

(defn consume-and-index-entities
  ([indexer consumer]
   (consume-and-index-entities indexer consumer 10000))
  ([indexer ^KafkaConsumer consumer timeout]
   (let [records (.poll consumer timeout)
         txs (->> (for [record records]
                    (if (tx-record? record)
                      (index-tx-record indexer record)
                      (index-doc-record indexer record)))
                  (reduce into []))]
     (store-topic-partition-offsets indexer consumer (.partitions records))
     txs)))

(defn subscribe-from-stored-offsets [indexer ^KafkaConsumer consumer ^List topics]
  (.subscribe consumer
              topics
              (reify ConsumerRebalanceListener
                (onPartitionsRevoked [_ partitions]
                  (store-topic-partition-offsets indexer consumer partitions))
                (onPartitionsAssigned [_ partitions]
                  (seek-to-stored-offsets indexer consumer partitions)))))
