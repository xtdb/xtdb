(ns xtdb.kafka
  (:require [clojure.java.io :as io] 
            [xtdb.api :as xt]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import java.io.Closeable
           java.lang.AutoCloseable
           java.nio.file.Path
           [java.time Duration Instant]
           [java.util List Map Properties]
           [java.util.concurrent CompletableFuture]
           [org.apache.kafka.clients.admin AdminClient NewTopic TopicDescription]
           [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord]
           [org.apache.kafka.common.errors InterruptException TopicExistsException UnknownTopicOrPartitionException]
           org.apache.kafka.common.TopicPartition
           [xtdb.api AConfig TransactionKey]
           [xtdb.api.log Log LogRecord LogSubscriber KafkaLogFactory]))

(defn ->kafka-config [{:keys [bootstrap-servers ^Path properties-file properties-map]}]
  (merge {"bootstrap.servers" bootstrap-servers}
         (when properties-file
           (with-open [in (io/reader (.toFile properties-file))]
             (->> (doto (Properties.)
                    (.load in))
                  (into {}))))
         properties-map))

(defn ->producer [kafka-config]
  (KafkaProducer. ^Map (merge {"enable.idempotence" "true"
                               "acks" "all"
                               "compression.type" "snappy"
                               "key.serializer" "org.apache.kafka.common.serialization.ByteBufferSerializer"
                               "value.serializer" "org.apache.kafka.common.serialization.ByteBufferSerializer"}
                              kafka-config)))

(defn ->consumer ^org.apache.kafka.clients.consumer.KafkaConsumer [kafka-config]
  (KafkaConsumer. ^Map (merge {"enable.auto.commit" "false"
                               "isolation.level" "read_committed"
                               "auto.offset.reset" "earliest"
                               "key.deserializer" "org.apache.kafka.common.serialization.ByteBufferDeserializer"
                               "value.deserializer" "org.apache.kafka.common.serialization.ByteBufferDeserializer"}
                              kafka-config)))

(defn- seek-consumer [^KafkaConsumer consumer, ^TopicPartition tp, after-tx-id]
  (if after-tx-id
    (.seek consumer tp (inc ^long after-tx-id))
    (.seekToBeginning consumer [tp])))

(defn- poll-consumer [^KafkaConsumer consumer, ^Duration poll-duration]
  (try
    (.poll consumer poll-duration)
    (catch InterruptException e
      (Thread/interrupted)
      (throw (.getCause e)))))

(defn- ->log-record [^ConsumerRecord record]
  (LogRecord. (TransactionKey. (.offset record) (Instant/ofEpochMilli (.timestamp record)))
              (.value record)))

(defn- handle-subscriber [{:keys [poll-duration tp kafka-config]} after-tx-id ^LogSubscriber subscriber]
  (doto (.newThread log/subscription-thread-factory
                    (fn []
                      (let [thread (Thread/currentThread)]
                        (.onSubscribe subscriber (reify AutoCloseable
                                                   (close [_]
                                                     (.interrupt thread)
                                                     (.join thread)))))

                      (with-open [consumer (->consumer kafka-config)]
                        (.assign consumer #{tp})
                        (seek-consumer consumer tp after-tx-id)
                        (try
                          (loop []
                            (doseq [record (poll-consumer consumer poll-duration)]
                              (when (Thread/interrupted)
                                (throw (InterruptedException.)))
                              (.acceptRecord subscriber (->log-record record)))

                            (when-not (Thread/interrupted)
                              (recur)))

                          (catch InterruptedException _)))))
    (.start)))

(defrecord KafkaLog [kafka-config
                     ^KafkaProducer producer
                     ^KafkaConsumer consumer
                     ^TopicPartition tp
                     ^Duration poll-duration]
  Log
  (appendRecord [_ record]
    (let [fut (CompletableFuture.)]
      (.send producer (ProducerRecord. (.topic tp) nil record)
             (reify Callback
               (onCompletion [_ record-metadata e]
                 (if e
                   (.completeExceptionally fut e)
                   (.complete fut (TransactionKey. (.offset record-metadata)
                                                   (Instant/ofEpochMilli (.timestamp record-metadata))))))))
      fut))

  (readRecords [_ after-tx-id limit]
    (seek-consumer consumer tp after-tx-id)

    (->> (poll-consumer consumer poll-duration)
         (into [] (comp (take limit) (map ->log-record)))))

  (subscribe [this after-tx-id subscriber]
    (handle-subscriber this after-tx-id subscriber))

  Closeable
  (close [_]
    (util/try-close consumer)
    (util/try-close producer)))

(defn ensure-topic-exists [kafka-config {:keys [topic-name replication-factor create-topic? topic-config]}]
  (with-open [admin-client (AdminClient/create ^Map kafka-config)]
    (or (when-let [^TopicDescription
                   desc (-> (try
                              (let [^List topics [topic-name]]
                                (-> @(.all (.describeTopics admin-client topics))
                                    (util/rethrowing-cause)))
                              (catch UnknownTopicOrPartitionException _))
                            (get topic-name))]
          (let [partition-count (count (.partitions desc))]
            (when-not (= 1 partition-count)
              (throw (IllegalStateException. (format "'%s' topic has %d partitions, required 1"
                                                     topic-name partition-count))))
            :exists))

        (when create-topic?
          (let [new-topic (doto (NewTopic. ^String topic-name
                                           1 ; num-partitions
                                           ^short (short replication-factor))
                            (.configs topic-config))]
            (try
              (-> @(.all (.createTopics admin-client [new-topic]))
                  (util/rethrowing-cause))
              :created
              (catch TopicExistsException _))))

        (throw (IllegalStateException. (format "Topic '%s' does not exist", topic-name))))))

(defmethod xtn/apply-config! ::log 
  [^AConfig config _ {:keys [topic-name bootstrap-servers create-topic? replication-factor
                             poll-duration topic-config properties-map properties-file]}]
  (doto config
      (.setTxLog (cond-> (KafkaLogFactory. bootstrap-servers topic-name)
                   create-topic? (.autoCreateTopic create-topic?)
                   replication-factor (.replicationFactor (int replication-factor))
                   poll-duration (.pollDuration (time/->duration poll-duration)) 
                   topic-config (.topicConfig topic-config)
                   properties-map (.propertiesMap properties-map)
                   properties-file (.propertiesFile (util/->path properties-file))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-log [^KafkaLogFactory factory] 
  (let [topic-name (.getTopicName factory)
        poll-duration (.getPollDuration factory)
        kafka-config (->kafka-config {:bootstrap-servers (.getBootstrapServers factory)
                                      :properties-file (.getPropertiesFile factory)
                                      :properties-map (.getPropertiesMap factory)})
        kafka-opts {:topic-name topic-name
                    :replication-factor (.getReplicationFactor factory)
                    :create-topic? (.getAutoCreateTopic factory)
                      ;; LogAppendTime required - convert topicconfig into clojure map and ensure it's present
                    :topic-config (into {"message.timestamp.type" "LogAppendTime"} (.getTopicConfig factory))}
        tp (TopicPartition. topic-name 0)]
    (ensure-topic-exists kafka-config kafka-opts)
    (KafkaLog. kafka-config
               (->producer kafka-config)
               (doto (->consumer kafka-config)
                 (.assign #{tp}))
               tp
               poll-duration)))
