(ns crux.kafka
  "Currently uses nippy to play nice with RDF IRIs that are not
  valid keywords. Uses one transaction per message."
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.tx :as tx]
            [crux.kafka.nippy])
  (:import [java.util List Map Date]
           [java.util.concurrent ExecutionException]
           [java.time Duration]
           [org.apache.kafka.clients.admin
            AdminClient NewTopic]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer
            KafkaConsumer ConsumerRecord ConsumerRebalanceListener]
           [org.apache.kafka.clients.producer
            KafkaProducer ProducerRecord RecordMetadata]
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

(defrecord KafkaTxLog [^KafkaProducer producer tx-topic doc-topic]
  db/TxLog
  (submit-doc [this content-hash doc]
    (->> (ProducerRecord. doc-topic content-hash doc)
         (.send producer)))

  (submit-tx [this tx-ops]
    (let [conformed-tx-ops (tx/conform-tx-ops tx-ops)]
      (doseq [doc (tx/tx-ops->docs tx-ops)]
        (db/submit-doc this (str (idx/new-id doc)) doc))
      (let [tx-send-future (->> (ProducerRecord. tx-topic nil conformed-tx-ops)
                                (.send producer))]
        (delay
         (let [record-meta ^RecordMetadata @tx-send-future]
           {:tx-id (.offset record-meta)
            :transact-time (Date. (.timestamp record-meta))}))))))

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
  (let [content-hash (.key record)
        doc (consumer-record->value record)]
    (db/index-doc indexer content-hash doc)
    doc))

(defn- index-tx-record [indexer ^ConsumerRecord record]
  (let [tx-time (Date. (.timestamp record))
        tx-ops (consumer-record->value record)
        tx-id (.offset record)]
    (db/index-tx indexer tx-ops tx-time tx-id)
    tx-ops))

(defn- tx-record? [^ConsumerRecord record]
  (nil? (.key record)))

(defn- store-tx-log-time [indexer records]
  (when-let [tx-time (->> (filter tx-record? records)
                          (map #(.timestamp ^ConsumerRecord %))
                          (sort)
                          (last))]
    (db/store-index-meta indexer :crux.tx-log/tx-time (Date. ^long tx-time))))

(defn consume-and-index-entities
  ([indexer consumer]
   (consume-and-index-entities indexer consumer 10000))
  ([indexer ^KafkaConsumer consumer timeout]
   (let [records (.poll consumer (Duration/ofMillis timeout))
         txs (->> (for [record records]
                    (if (tx-record? record)
                      (index-tx-record indexer record)
                      (index-doc-record indexer record)))
                  (reduce into []))]
     (store-topic-partition-offsets indexer consumer (.partitions records))
     (store-tx-log-time indexer records)
     (when-let [{txs true
                 docs false} (not-empty (group-by tx-record? records))]
       {:txs (count txs)
        :docs (count docs)}))))

(defn subscribe-from-stored-offsets [indexer ^KafkaConsumer consumer ^List topics]
  (.subscribe consumer
              topics
              (reify ConsumerRebalanceListener
                (onPartitionsRevoked [_ partitions]
                  (store-topic-partition-offsets indexer consumer partitions))
                (onPartitionsAssigned [_ partitions]
                  (seek-to-stored-offsets indexer consumer partitions)))))
