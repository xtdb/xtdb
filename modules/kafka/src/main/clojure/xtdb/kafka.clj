(ns xtdb.kafka
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [xtdb.api.protocols :as xtp]
            [xtdb.log :as log]
            [xtdb.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import [xtdb.log Log LogSubscriber]
           java.io.Closeable
           java.lang.AutoCloseable
           java.nio.file.Path
           [java.time Duration Instant]
           java.time.Instant
           [java.util List Map Properties]
           [java.util.concurrent CompletableFuture]
           [org.apache.kafka.clients.admin AdminClient NewTopic TopicDescription]
           [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord]
           [org.apache.kafka.common.errors InterruptException TopicExistsException UnknownTopicOrPartitionException]
           org.apache.kafka.common.TopicPartition))

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
  (log/->LogRecord (xtp/->TransactionInstant (.offset record) (Instant/ofEpochMilli (.timestamp record)))
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
                   (.complete fut (log/->LogRecord (xtp/->TransactionInstant (.offset record-metadata)
                                                                             (Instant/ofEpochMilli (.timestamp record-metadata)))
                                                   record))))))
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

(s/def ::bootstrap-servers string?)
(s/def ::properties-file ::util/path)
(s/def ::properties-map ::util/string-map)
(s/def ::topic-name string?)
(s/def ::replication-factor pos-int?)
(s/def ::create-topic? boolean?)
(s/def ::topic-config ::util/string-map)

(s/def ::poll-duration ::util/duration)

(derive ::log :xtdb/log)

(defmethod ig/prep-key ::log [_ opts]
  (-> (merge {:bootstrap-servers "localhost:9092"
              :replication-factor 1
              :create-topic? true
              :poll-duration "PT1S"}
             opts)
      (update :topic-config (fn [config]
                              (into {"message.timestamp.type" "LogAppendTime"} config)))
      (util/maybe-update :properties-file util/->path)
      (util/maybe-update :poll-duration util/->duration)))

(defmethod ig/pre-init-spec ::log [_]
  (s/keys :req-un [::bootstrap-servers ::topic-name ::create-topic? ::replication-factor ::poll-duration]
          :opt-un [::properties-file ::properties-map ::topic-config]))

(defmethod ig/init-key ::log [_ {:keys [topic-name poll-duration] :as kafka-opts}]
  (let [kafka-config (->kafka-config kafka-opts)
        tp (TopicPartition. topic-name 0)]
    (ensure-topic-exists kafka-config kafka-opts)
    (KafkaLog. kafka-config
               (->producer kafka-config)
               (doto (->consumer kafka-config)
                 (.assign #{tp}))
               tp
               poll-duration)))

(defmethod ig/halt-key! ::log [_ log]
  (util/try-close log))
