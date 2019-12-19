(ns crux.kafka
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.node :as n]
            [crux.status :as status]
            [crux.tx :as tx]
            [taoensso.nippy :as nippy]
            [clojure.set :as set])
  (:import [crux.kafka.nippy NippyDeserializer NippySerializer]
           java.io.Closeable
           java.time.Duration
           [java.util Date List Map UUID]
           java.util.concurrent.ExecutionException
           [org.apache.kafka.clients.admin AdminClient NewTopic TopicDescription]
           [org.apache.kafka.clients.consumer ConsumerRebalanceListener ConsumerRecord KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord RecordMetadata]
           [org.apache.kafka.common.errors TopicExistsException InterruptException]
           org.apache.kafka.common.TopicPartition))

(s/def ::bootstrap-servers string?)
(s/def ::topic string?)
(s/def ::partitions pos-int?)
(s/def ::replication-factor pos-int?)

(s/def ::tx-topic ::topic)
(s/def ::doc-topic ::topic)
(s/def ::doc-partitions ::partitions)
(s/def ::create-topics boolean?)

(def default-producer-config
  {"enable.idempotence" "true"
   "acks" "all"
   "compression.type" "snappy"
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

(defn- read-kafka-properties-file [f]
  (when f
    (with-open [in (io/reader (io/file f))]
      (cio/load-properties in))))

(defn- ->kafka-config [{:keys [crux.kafka/bootstrap-servers
                               crux.kafka/kafka-properties-file
                               crux.kafka/kafka-properties-map]}]
  (merge {"bootstrap.servers" bootstrap-servers}
         (read-kafka-properties-file kafka-properties-file)
         kafka-properties-map))

(defn create-producer ^org.apache.kafka.clients.producer.KafkaProducer [config]
  (KafkaProducer. ^Map (merge default-producer-config config)))

(defn create-consumer ^org.apache.kafka.clients.consumer.KafkaConsumer [config]
  (KafkaConsumer. ^Map (merge default-consumer-config config)))

(defn create-admin-client ^AdminClient [config]
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

(defn- ensure-tx-topic-has-single-partition [^AdminClient admin-client tx-topic]
  (let [name->description @(.all (.describeTopics admin-client [tx-topic]))]
    (assert (= 1 (count (.partitions ^TopicDescription (get name->description tx-topic)))))))

(defn ensure-topics [{::keys [tx-topic replication-factor doc-partitions doc-topic create-topics] :as config} ^AdminClient admin-client]
  (when create-topics
    (create-topic admin-client tx-topic 1 replication-factor tx-topic-config)
    (create-topic admin-client doc-topic doc-partitions replication-factor doc-topic-config))

  (ensure-tx-topic-has-single-partition admin-client tx-topic))

(defn tx-record->tx-log-entry [^ConsumerRecord record]
  {:crux.tx.event/tx-events (.value record)
   :crux.tx/tx-id (.offset record)
   :crux.tx/tx-time (Date. (.timestamp record))})

;;; Transacting Producer

(defrecord KafkaTxLog [^KafkaProducer producer tx-topic doc-topic kafka-config]
  Closeable
  (close [_])

  db/TxLog
  (submit-doc [this content-hash doc]
    (->> (ProducerRecord. doc-topic content-hash doc)
         (.send producer)))

  (submit-tx [this tx-ops]
    (try
      (s/assert :crux.api/tx-ops tx-ops)
      (let [tx-events (map tx/tx-op->tx-event tx-ops)
            content-hash->doc (->> (for [doc (mapcat tx/tx-op->docs tx-ops)]
                                     [(c/new-id doc) doc])
                                   (into {}))]
        (doseq [f (->> (for [[content-hash doc] content-hash->doc]
                         (db/submit-doc this (str content-hash) doc))
                       (doall))]
          @f)
        (.flush producer)
        (let [tx-send-future (->> (doto (ProducerRecord. tx-topic nil tx-events)
                                    (-> (.headers) (.add (str :crux.tx/docs)
                                                         (nippy/fast-freeze (set (keys content-hash->doc))))))
                                  (.send producer))]
          (delay
           (let [record-meta ^RecordMetadata @tx-send-future]
             {:crux.tx/tx-id (.offset record-meta)
              :crux.tx/tx-time (Date. (.timestamp record-meta))}))))))

  (new-tx-log-context [this]
    (create-consumer (assoc kafka-config "enable.auto.commit" "false")))

  (tx-log [this tx-topic-consumer from-tx-id]
    (let [tx-topic-consumer ^KafkaConsumer tx-topic-consumer
          tx-topic-partition (TopicPartition. tx-topic 0)]
      (.assign tx-topic-consumer [tx-topic-partition])
      (if from-tx-id
        (.seek tx-topic-consumer tx-topic-partition (long from-tx-id))
        (.seekToBeginning tx-topic-consumer (.assignment tx-topic-consumer)))
      ((fn step []
         (lazy-seq
          (when-let [records (seq (.poll tx-topic-consumer (Duration/ofMillis 1000)))]
            (concat (map tx-record->tx-log-entry records)
                    (step)))))))))

;;; Indexing Consumer

(defn subscribe-topic [^KafkaConsumer consumer topic partition->offset]
  (let [^List topics [topic]]
    (.subscribe consumer topics
                (reify ConsumerRebalanceListener
                  (onPartitionsRevoked [_ partitions]
                    (log/debug "Partitions revoked:" (str partitions)))
                  (onPartitionsAssigned [_ partitions]
                    (log/debug "Partitions assigned:" (str partitions))

                    (doseq [^TopicPartition topic-partition partitions]
                      (if-let [^long offset (partition->offset (.partition topic-partition))]
                        (.seek consumer topic-partition offset)
                        (.seekToBeginning consumer [topic-partition]))))))))

(defn docs-indexed [ag-state {:keys [indexed-hashes doc-partition-offsets]} {:keys [indexer]}]
  (db/swap-index-meta indexer ::doc-partition-offsets merge doc-partition-offsets)

  (when-let [{:keys [awaited-hashes !latch]} ag-state]
    (if-let [awaited-hashes (not-empty (set/difference awaited-hashes indexed-hashes))]
      (assoc ag-state :awaited-hashes awaited-hashes)

      (do
        (deliver !latch nil)
        nil))))

(defn index-docs [doc-records {:keys [indexer !agent]}]
  (let [doc-results (reduce (fn [{:keys [indexed-hashes doc-partition-offsets]} ^ConsumerRecord doc-record]
                              (let [content-hash (.key doc-record)
                                    doc (.value doc-record)]
                                (db/index-doc indexer content-hash doc)

                                {:indexed-hashes (conj indexed-hashes content-hash)
                                 :doc-partition-offsets (assoc doc-partition-offsets (.partition doc-record) (inc (.offset doc-record)))}))
                            {:indexed-hashes #{}
                             :doc-partition-offsets {}}
                            doc-records)]

    (log/debugf "Indexed %d documents" (count (:indexed-hashes doc-results)))
    (send-off !agent docs-indexed doc-results {:indexer indexer})))

(defn doc-consumer-loop ^Runnable [{:keys [consumer-config ::doc-topic indexer ^Duration timeout]
                                    :or {timeout (Duration/ofMillis 5000)}
                                    :as config}]
  (fn []
    (let [thread-name (.getName (Thread/currentThread))]
      (with-open [consumer (create-consumer consumer-config)]
        (subscribe-topic consumer doc-topic (into {} (db/read-index-meta indexer ::doc-partition-offsets)))

        (log/info (str thread-name " subscribed."))

        (while (not (Thread/interrupted))
          (try
            (when-let [doc-records (seq (.poll consumer timeout))]
              (index-docs doc-records config))

            (catch InterruptedException _)
            (catch InterruptException _)))

        (log/info (str thread-name " stopped."))))))

(defn doc-latch [content-hashes {:keys [indexer !agent]}]
  (let [!latch (promise)]
    (if (not-empty (db/missing-hashes indexer content-hashes))
      (send-off !agent (fn [_]
                         (if-let [awaited-hashes (not-empty (db/missing-hashes indexer content-hashes))]
                           {:awaited-hashes awaited-hashes, :!latch !latch}
                           (do
                             (deliver !latch nil)
                             nil))))

      (deliver !latch nil))

    !latch))

(defn index-tx-record [{:keys [content-hashes crux.tx.event/tx-events]
                        :crux.tx/keys [tx-time tx-id]
                        :as tx-log-entry}
                       {:keys [indexer !agent] :as config}]
  @(doc-latch content-hashes config)
  (db/index-tx indexer tx-events tx-time tx-id)

  (db/store-index-meta indexer :crux.tx/latest-completed-tx (-> tx-log-entry
                                                                (select-keys [:crux.tx/tx-id :crux.tx/tx-time]))))

(defn tx-consumer-loop ^Runnable [{:keys [consumer-config ::tx-topic indexer !agent ^Duration timeout]
                                   :or {timeout (Duration/ofMillis 5000)}
                                   :as config}]
  (fn []
    (with-open [consumer (create-consumer consumer-config)]
      (subscribe-topic consumer tx-topic
                       (constantly (-> (db/read-index-meta indexer :crux.tx/latest-completed-tx)
                                       :crux.tx/tx-id)))
      (log/info "tx-consumer subscribed...")

      (while (not (Thread/interrupted))
        (try
          (doseq [^ConsumerRecord tx-record (.poll consumer timeout)]
            (when (Thread/interrupted)
              (throw (InterruptedException.)))

            (index-tx-record (merge (tx-record->tx-log-entry tx-record)
                                    {:content-hashes (-> (.headers tx-record)
                                                         (.lastHeader (str :crux.tx/docs))
                                                         .value
                                                         nippy/fast-thaw)})
                             config))

          (catch InterruptedException _
            (.interrupt (Thread/currentThread)))
          (catch InterruptException _
            (.interrupt (Thread/currentThread)))))

      (log/info "tx-consumer stopped."))))

(defrecord IndexingConsumer [!agent ^Thread tx-consumer-thread doc-consumer-threads consumer-config]
  status/Status
  (status-map [_]
    {:crux.zk/zk-active?
     (try
       (with-open [^KafkaConsumer consumer (create-consumer (merge consumer-config {"default.api.timeout.ms" (int 1000)}))]
         (boolean (.listTopics consumer)))
       (catch Exception e
         (log/warn e "Could not list Kafka topics:")
         false))})

  Closeable
  (close [_]
    (run! #(.interrupt ^Thread %) doc-consumer-threads)
    (.interrupt tx-consumer-thread)

    (run! #(.join ^Thread %) doc-consumer-threads)
    (.join tx-consumer-thread)

    (await !agent)
    ))

(defn topic-partition-counts [topic ^AdminClient admin-client]
  (-> (.describeTopics admin-client [topic]) .all .get ^TopicDescription (get topic) .partitions count))

(defn ->started-thread [^Runnable f ^String name]
  (doto (Thread. f name)
    (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                    (uncaughtException [_ _ t]
                                      (log/error t (or (.getMessage t) (-> t .getClass .getName))))))
    .start))

(defn- start-indexing-consumer ^IndexingConsumer [{:keys [crux.node/indexer]} {::keys [doc-topic max-doc-threads] :as config}]
  (let [consumer-config (->kafka-config config)]
    (with-open [admin-client (create-admin-client consumer-config)]
      (ensure-topics config admin-client)

      (let [indexing-config (merge config
                                   {:consumer-config consumer-config
                                    :indexer indexer
                                    :!agent (agent nil)})
            tx-consumer-thread (->started-thread (tx-consumer-loop (-> indexing-config
                                                                       (assoc-in [:consumer-config "group.id"] (str (UUID/randomUUID)))))
                                                 "tx-consumer")
            doc-consumer-threads (let [indexing-config (-> indexing-config
                                                           (assoc-in [:consumer-config "group.id"] (str (UUID/randomUUID))))]
                                   (set (for [i (range (min (topic-partition-counts doc-topic admin-client)
                                                            max-doc-threads))]
                                          (->started-thread (doc-consumer-loop indexing-config)
                                                            (str "doc-consumer-" i)))))]

        (map->IndexingConsumer (assoc indexing-config
                                      :tx-consumer-thread tx-consumer-thread
                                      :doc-consumer-threads doc-consumer-threads))))))

(def default-config
  {::bootstrap-servers {:doc "URL for connecting to Kafka i.e. \"kafka-cluster-kafka-brokers.crux.svc.cluster.local:9092\""
                        :default "localhost:9092"
                        :crux.config/type :crux.config/string}
   ::tx-topic {:doc "Kafka transaction topic"
               :default "crux-transaction-log"
               :crux.config/type :crux.config/string}
   ::doc-topic {:doc "Kafka document topic"
                :default "crux-docs"
                :crux.config/type :crux.config/string}
   ::doc-partitions {:doc "Partitions for document topic"
                     :default 10
                     :crux.config/type :crux.config/nat-int}
   ::create-topics {:doc "Create topics if they do not exist"
                    :default true
                    :crux.config/type :crux.config/boolean}
   ::replication-factor {:doc "Level of durability for Kafka"
                         :default 1
                         :crux.config/type :crux.config/nat-int}
   ::kafka-properties-file {:doc "Used for supplying Kafka connection properties to the underlying Kafka API."
                            :crux.config/type :crux.config/string}
   ::kafka-properties-map {:doc "Used for supplying Kafka connection properties to the underlying Kafka API."
                           :crux.config/type [map? identity]}})

(def indexing-consumer
  {:start-fn #'start-indexing-consumer

   :deps [:crux.node/indexer]

   :args (merge default-config
                {::max-doc-threads
                 {:doc "Threads for document consumer. Won't exceed number of partitions in the doc topic."
                  :default (+ (.availableProcessors (Runtime/getRuntime)) 2)
                  :crux.config/type :crux.config/nat-int}})})

(def producer
  {:start-fn (fn [_ config]
               (create-producer (->kafka-config config)))
   :args default-config})

(def tx-log
  {:start-fn (fn [{::keys [producer]} {:keys [crux.kafka/tx-topic crux.kafka/doc-topic] :as config}]
               (->KafkaTxLog producer tx-topic doc-topic (->kafka-config config)))
   :deps [::producer]
   :args default-config})

(def topology
  (merge n/base-topology
         {:crux.node/tx-log tx-log
          ::producer producer
          ::indexing-consumer indexing-consumer}))
