(ns xtdb.kafka
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as ctl]
            [xtdb.api :as xt]
            [xtdb.file-list-cache :as flc]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import java.lang.AutoCloseable
           [java.nio.file Path]
           [java.time Duration Instant]
           [java.util List Map Properties]
           [java.util.concurrent CompletableFuture]
           [org.apache.kafka.clients.admin AdminClient NewTopic TopicDescription]
           [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord]
           [org.apache.kafka.common.errors InterruptException TopicAuthorizationException TopicExistsException UnknownTopicOrPartitionException]
           org.apache.kafka.common.TopicPartition
           [xtdb.api Xtdb$Config]
           [xtdb.api.log FileListCache FileListCache$Subscriber Kafka Kafka$Factory Log TxLog$Record TxLog$Subscriber]))

(defn ->kafka-config [{:keys [bootstrap-servers ^Path properties-file properties-map]}]
  (merge {"bootstrap.servers" bootstrap-servers}
         (when properties-file
           (with-open [in (io/reader (.toFile properties-file))]
             (->> (doto (Properties.)
                    (.load in))
                  (into {}))))
         properties-map))

(defn- ->producer [kafka-config]
  (KafkaProducer. ^Map (merge {"enable.idempotence" "true"
                               "acks" "all"
                               "compression.type" "snappy"
                               "key.serializer" "org.apache.kafka.common.serialization.ByteArraySerializer"
                               "value.serializer" "org.apache.kafka.common.serialization.ByteArraySerializer" }
                              kafka-config)))

(defn ->tx-producer ^org.apache.kafka.clients.producer.KafkaProducer [kafka-config]
  (->producer (merge kafka-config
                     {"key.serializer" "org.apache.kafka.common.serialization.ByteBufferSerializer"
                      "value.serializer" "org.apache.kafka.common.serialization.ByteBufferSerializer"})))

(defn ->consumer ^org.apache.kafka.clients.consumer.KafkaConsumer [kafka-config]
  (KafkaConsumer. ^Map (merge {"enable.auto.commit" "false"
                               "isolation.level" "read_committed"
                               "auto.offset.reset" "latest"
                               "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                               "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"}
                              kafka-config)))

(defn ->tx-consumer ^org.apache.kafka.clients.consumer.KafkaConsumer [kafka-config]
  (->consumer (merge {"auto.offset.reset" "earliest"
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
  (TxLog$Record. (.offset record) (Instant/ofEpochMilli (.timestamp record)) (.value record)))

(defn- handle-tx-subscriber [{:keys [poll-duration tp kafka-config]} after-tx-id ^TxLog$Subscriber subscriber]
  (doto (.newThread log/subscription-thread-factory
                    (fn []
                      (let [thread (Thread/currentThread)]
                        (.onSubscribe subscriber (reify AutoCloseable
                                                   (close [_]
                                                     (.interrupt thread)
                                                     (.join thread)))))

                      (with-open [consumer (->tx-consumer kafka-config)]
                        (.assign consumer #{tp})
                        (seek-consumer consumer tp after-tx-id)
                        (try
                          (loop []
                            (doseq [record (poll-consumer consumer poll-duration)]
                              (when (Thread/interrupted)
                                (throw (InterruptedException.)))
                              (.accept subscriber (->log-record record)))

                            (when-not (Thread/interrupted)
                              (recur)))

                          (catch InterruptedException _)))))
    (.start)))

(defrecord KafkaTxLog [kafka-config
                       ^KafkaProducer producer
                       ^KafkaConsumer consumer
                       ^TopicPartition tp
                       ^Duration poll-duration]
  Log
  (latestSubmittedTxId [_]
    (or (some-> (.endOffsets consumer #{tp}) (get tp) dec)
        -1))

  (appendTx [_ record]
    (let [fut (CompletableFuture.)]
      (.send producer (ProducerRecord. (.topic tp) nil record)
             (reify Callback
               (onCompletion [_ record-metadata e]
                 (if e
                   (.completeExceptionally fut e)
                   (.complete fut (.offset record-metadata))))))
      fut))

  (readTxs [_ after-tx-id limit]
    (seek-consumer consumer tp after-tx-id)

    (->> (poll-consumer consumer poll-duration)
         (into [] (comp (take limit) (map ->log-record)))))

  (subscribeTxs [this after-tx-id subscriber]
    (handle-tx-subscriber this after-tx-id subscriber))

  AutoCloseable
  (close [_]
    (util/try-close consumer)
    (util/try-close producer)))

(defn- handle-file-notif-subscriber [{:keys [poll-duration tp kafka-config]} ^FileListCache$Subscriber subscriber]
  (doto (.newThread log/subscription-thread-factory
                    (fn []
                      (let [thread (Thread/currentThread)]
                        (.onSubscribe subscriber (reify AutoCloseable
                                                   (close [_]
                                                     (.interrupt thread)
                                                     (.join thread)))))

                      (with-open [consumer (->consumer kafka-config)]
                        (.assign consumer #{tp})
                        (.seekToEnd consumer #{tp})
                        (try
                          (loop []
                            (doseq [^ConsumerRecord record (poll-consumer consumer poll-duration)]
                              (when (Thread/interrupted)
                                (throw (InterruptedException.)))
                              (.accept subscriber (flc/transit->file-notification (.value record))))

                            (when-not (Thread/interrupted)
                              (recur)))

                          (catch InterruptedException _)))))
    (.start)))

(defrecord KafkaFileListCache [kafka-config
                               ^KafkaProducer producer
                               ^TopicPartition tp
                               ^Duration poll-duration]
  FileListCache
  (appendFileNotification [_ n]
    (let [fut (CompletableFuture.)]
      (.send producer (ProducerRecord. (.topic tp) nil (flc/file-notification->transit n))
             (reify Callback
               (onCompletion [_ _record-metadata e]
                 (if e
                   (.completeExceptionally fut e)
                   (.complete fut nil)))))
      fut))

  (subscribeFileNotifications [this subscriber]
    (handle-file-notif-subscriber this subscriber))

  AutoCloseable
  (close [_]
    (util/close producer)))

(defrecord KafkaLog [^KafkaTxLog tx-log ^KafkaFileListCache file-list-cache]
  Log
  (latestSubmittedTxId [_] (.latestSubmittedTxId tx-log))

  (appendTx [_ record] (.appendTx tx-log record))
  (readTxs [_ after-tx-id limit] (.readTxs tx-log after-tx-id limit))
  (subscribeTxs [_ after-tx-id subscriber] (.subscribeTxs tx-log after-tx-id subscriber))

  (appendFileNotification [_ n] (.appendFileNotification file-list-cache n))
  (subscribeFileNotifications [_ subscriber] (.subscribeFileNotifications file-list-cache subscriber))

  (close [_]
    (.close tx-log)
    (.close file-list-cache)))

(defn ensure-topic-exists [kafka-config {:keys [topic-name create-topic?]}]
  (with-open [admin-client (AdminClient/create ^Map kafka-config)] 
    (or (when-let [^TopicDescription
                   desc (-> (try
                              (let [^List topics [topic-name]]
                                (-> @(.all (.describeTopics admin-client topics))
                                    (util/rethrowing-cause)))
                              (catch TopicAuthorizationException e
                                (ctl/errorf "Failed to auth when calling 'describeTopics' on %s" topic-name)
                                (throw e))
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
                                           (short 1) ; replication-factor
                                           )
                            (.configs {"message.timestamp.type" "LogAppendTime"}))]
            (try
              (-> @(.all (.createTopics admin-client [new-topic]))
                  (util/rethrowing-cause))
              :created
              (catch TopicExistsException _))))

        (throw (IllegalStateException. (format "Topic '%s' does not exist", topic-name))))))

(defmethod xtn/apply-config! ::log 
  [^Xtdb$Config config _ {:keys [bootstrap-servers
                                 tx-topic files-topic create-topics?
                                 tx-poll-duration file-poll-duration
                                 properties-map properties-file]}]
  (doto config
    (.setTxLog (cond-> (Kafka/kafka bootstrap-servers tx-topic files-topic)
                 create-topics? (.autoCreateTopics create-topics?)
                 tx-poll-duration (.txPollDuration (time/->duration tx-poll-duration))
                 file-poll-duration (.filePollDuration (time/->duration file-poll-duration))
                 properties-map (.propertiesMap properties-map)
                 properties-file (.propertiesFile (util/->path properties-file))))))

(defn- open-tx-log [^Kafka$Factory factory kafka-config]
  (let [topic-name (.getTxTopic factory)
        poll-duration (.getTxPollDuration factory)
        tp (TopicPartition. topic-name 0)]
    (ensure-topic-exists kafka-config
                         {:topic-name topic-name
                          :create-topic? (.getAutoCreateTopics factory)})

    (KafkaTxLog. kafka-config
                 (->tx-producer kafka-config)
                 (doto (->tx-consumer kafka-config)
                   (.assign #{tp}))
                 tp
                 poll-duration)))

(defn- open-file-list-cache [^Kafka$Factory factory kafka-config]
  (let [topic-name (.getFilesTopic factory)
        poll-duration (.getFilePollDuration factory)
        tp (TopicPartition. topic-name 0)]
    (ensure-topic-exists kafka-config
                         {:topic-name topic-name
                          :create-topic? (.getAutoCreateTopics factory)})

    (KafkaFileListCache. kafka-config (->producer kafka-config) tp poll-duration)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-log [^Kafka$Factory factory]
  (let [kafka-config (->kafka-config {:bootstrap-servers (.getBootstrapServers factory)
                                      :properties-file (.getPropertiesFile factory)
                                      :properties-map (.getPropertiesMap factory)})]

    (util/with-close-on-catch [tx-log (open-tx-log factory kafka-config)
                               file-list-cache (open-file-list-cache factory kafka-config)]
      (KafkaLog. tx-log file-list-cache))))
