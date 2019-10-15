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
            [taoensso.nippy :as nippy])
  (:import [crux.kafka.nippy NippyDeserializer NippySerializer]
           java.io.Closeable
           java.time.Duration
           [java.util Date List Map]
           java.util.concurrent.ExecutionException
           [org.apache.kafka.clients.admin AdminClient NewTopic TopicDescription]
           [org.apache.kafka.clients.consumer ConsumerRebalanceListener ConsumerRecord KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord RecordMetadata]
           org.apache.kafka.common.errors.TopicExistsException
           org.apache.kafka.common.TopicPartition))

(s/def ::bootstrap-servers string?)
(s/def ::group-id string?)
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

(defn- derive-kafka-config [{:keys [crux.kafka/bootstrap-servers
                                    crux.kafka/kafka-properties-file
                                    crux.kafka/kafka-properties-map]}]
  (merge {"bootstrap.servers" bootstrap-servers}
         (read-kafka-properties-file kafka-properties-file)
         kafka-properties-map))

(defn create-producer
  ^org.apache.kafka.clients.producer.KafkaProducer [config]
  (KafkaProducer. ^Map (merge default-producer-config config)))

(defn create-consumer
  ^org.apache.kafka.clients.consumer.KafkaConsumer [config]
  (KafkaConsumer. ^Map (merge default-consumer-config config)))

(defn create-admin-client
  ^org.apache.kafka.clients.admin.AdminClient [config]
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
      (let [tx-events (tx/tx-ops->tx-events tx-ops)
            content-hash->doc (->> (for [doc (tx/tx-ops->docs tx-ops)]
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

(defn consumer-record->value [^ConsumerRecord record]
  (.value record))

(defn- topic-partition-meta-key [^TopicPartition partition]
  (keyword "crux.kafka.topic-partition" (str partition)))

(defn- update-stored-consumer-state [indexer ^KafkaConsumer consumer records]
  (let [partition->records (group-by (fn [^ConsumerRecord r]
                                       (TopicPartition. (.topic r)
                                                        (.partition r))) records)
        partitions (vec (keys partition->records))
        end-offsets (.endOffsets consumer partitions)
        stored-consumer-state (or (db/read-index-meta indexer :crux.tx-log/consumer-state) {})
        consumer-state (->> (for [^TopicPartition partition partitions
                                  :let [^ConsumerRecord last-record-in-batch (->> (get partition->records partition)
                                                                                  (sort-by #(.offset ^ConsumerRecord %))
                                                                                  (last))
                                        next-offset (inc (.offset last-record-in-batch))
                                        end-offset (get end-offsets partition)
                                        lag (- end-offset next-offset)]]
                              (do (when-not (zero? lag)
                                    (log/debug "Falling behind" (str partition) "at:" next-offset "end:" end-offset))
                                  [(topic-partition-meta-key partition)
                                   {:next-offset next-offset
                                    :time (Date. (.timestamp last-record-in-batch))
                                    :lag lag}]))
                            (into stored-consumer-state))]
    (db/store-index-meta indexer :crux.tx-log/consumer-state consumer-state)))

(defn- prune-consumer-state [indexer ^KafkaConsumer consumer partitions]
  (let [consumer-state (db/read-index-meta indexer :crux.tx-log/consumer-state)
        end-offsets (.endOffsets consumer (vec partitions))]
    (->> (for [^TopicPartition partition partitions
               :let [partition-key (topic-partition-meta-key partition)
                     next-offset (or (get-in consumer-state [partition-key :next-offset]) 0)]]
           (do
             [partition-key {:next-offset next-offset
                             :lag (- (dec (get end-offsets partition)) next-offset)
                             :time (get-in consumer-state [partition-key :time])}]))
         (into {})
         (db/store-index-meta indexer :crux.tx-log/consumer-state))))

(defn seek-to-stored-offsets [indexer ^KafkaConsumer consumer partitions]
  (let [consumer-state (db/read-index-meta indexer :crux.tx-log/consumer-state)]
    (doseq [^TopicPartition partition partitions]
      (if-let [offset (get-in consumer-state [(topic-partition-meta-key partition) :next-offset])]
        (.seek consumer partition (long offset))
        (.seekToBeginning consumer [partition])))))

(defn- index-doc-record [indexer ^ConsumerRecord record]
  (let [content-hash (.key record)
        doc (.value record)]
    (db/index-doc indexer content-hash doc)
    doc))

(defn- index-tx-record [indexer ^ConsumerRecord record]
  (let [record (tx-record->tx-log-entry record)
        {:crux.tx/keys [tx-time
                        tx-id]} record
        {:crux.tx.event/keys [tx-events]} record]
    (db/index-tx indexer tx-events tx-time tx-id)
    tx-events))

(defn consume-and-index-entities
  [{:keys [indexer ^KafkaConsumer consumer pending-txs-state
           follower timeout tx-topic doc-topic]
    :or   {timeout 5000}}]
  (let [tx-topic-partition (TopicPartition. tx-topic 0)
        _ (when (and (.contains (.paused consumer) tx-topic-partition)
                     (empty? @pending-txs-state))
            (log/debug "Resuming" tx-topic)
            (.resume consumer [tx-topic-partition]))
        records (.poll consumer (Duration/ofMillis timeout))
        doc-records (vec (.records records (str doc-topic)))
        _ (doseq [record doc-records]
            (index-doc-record indexer record))
        tx-records (vec (.records records (str tx-topic)))
        pending-tx-records (swap! pending-txs-state into tx-records)
        tx-records (->> pending-tx-records
                        (take-while
                         (fn [^ConsumerRecord tx-record]
                           (let [content-hashes (->> (.lastHeader (.headers tx-record)
                                                                  (str :crux.tx/docs))
                                                     (.value)
                                                     (nippy/fast-thaw))
                                 ready? (db/docs-exist? indexer content-hashes)
                                 {:crux.tx/keys [tx-time
                                                 tx-id]} (tx-record->tx-log-entry tx-record)]
                             (if ready?
                               (log/info "Ready for indexing of tx" tx-id (pr-str tx-time))
                               (do (when-not (.contains (.paused consumer) tx-topic-partition)
                                     (log/debug "Pausing" tx-topic)
                                     (.pause consumer [tx-topic-partition]))
                                   (log/info "Delaying indexing of tx" tx-id (pr-str tx-time) "pending:" (count pending-tx-records))))
                             ready?)))
                        (vec))]
    (doseq [record tx-records]
      (index-tx-record indexer record))
    (when-let [records (seq (concat doc-records tx-records))]
      (update-stored-consumer-state indexer consumer records)
      (swap! pending-txs-state (comp vec (partial drop (count tx-records)))))
    {:txs (count tx-records)
     :docs (count doc-records)}))

;; TODO: This works as long as each node has a unique consumer group
;; id, if not the node will only get a subset of the doc-topic. The
;; tx-topic is always only one partition.
(defn subscribe-from-stored-offsets
  [indexer ^KafkaConsumer consumer ^List topics]
  (.subscribe consumer
              topics
              (reify ConsumerRebalanceListener
                (onPartitionsRevoked [_ partitions]
                  (log/info "Partitions revoked:" (str partitions)))
                (onPartitionsAssigned [_ partitions]
                  (log/info "Partitions assigned:" (str partitions))
                  (prune-consumer-state indexer consumer partitions)
                  (seek-to-stored-offsets indexer consumer partitions)))))

(defrecord IndexingConsumer [running? ^Thread worker-thread consumer-config indexer options]
  status/Status
  (status-map [_]
    {:crux.zk/zk-active?
     (try
       (with-open [^KafkaConsumer consumer (create-consumer (merge consumer-config {"default.api.timeout.ms" (int 1000)}))]
         (boolean (.listTopics consumer)))
       (catch Exception e
         (log/debug e "Could not list Kafka topics:")
         false))})

  Closeable
  (close [_]
    (reset! running? false)
    (.join worker-thread)))

(defn- indexing-consumer-thread-main-loop
  [{:keys [running? indexer consumer-config options]}]
  (with-open [consumer (create-consumer consumer-config)]
    (subscribe-from-stored-offsets
     indexer consumer [(::tx-topic options) (::doc-topic options)])
    (let [pending-txs-state (atom [])]
      (while @running?
        (try
          (consume-and-index-entities
           {:indexer indexer
            :consumer consumer
            :timeout 1000
            :pending-txs-state pending-txs-state
            :tx-topic (::tx-topic options)
            :doc-topic (::doc-topic options)})
          (catch Exception e
            (log/error e "Error while consuming and indexing from Kafka:")
            (Thread/sleep 500)))))))

(defn- ensure-tx-topic-has-single-partition [^AdminClient admin-client tx-topic]
  (let [name->description @(.all (.describeTopics admin-client [tx-topic]))]
    (assert (= 1 (count (.partitions ^TopicDescription (get name->description tx-topic)))))))

(defn- start-indexing-consumer
  ^java.io.Closeable
  [admin-client consumer-config indexer
   {:keys [crux.kafka/tx-topic
           crux.kafka/replication-factor
           crux.kafka/doc-partitions
           crux.kafka/doc-topic
           crux.kafka/create-topics] :as options}]
  (when create-topics
    (create-topic admin-client tx-topic 1 replication-factor tx-topic-config)
    (create-topic admin-client doc-topic doc-partitions
                  replication-factor doc-topic-config))
  (ensure-tx-topic-has-single-partition admin-client tx-topic)
  (let [indexing-consumer (map->IndexingConsumer {:running? (atom true)
                                                  :indexer indexer
                                                  :consumer-config consumer-config
                                                  :options options})]
    (assoc
     indexing-consumer
     :worker-thread
     (doto (Thread. ^Runnable (partial indexing-consumer-thread-main-loop indexing-consumer)
                    "crux.kafka.indexing-consumer-thread")
       (.start)))))

(def default-options {::bootstrap-servers
                      {:doc "URL for connecting to Kafka i.e. \"kafka-cluster-kafka-brokers.crux.svc.cluster.local:9092\""
                       :default "localhost:9092"
                       :crux.config/type :crux.config/string}
                      ::tx-topic
                      {:doc "Kafka transaction topic"
                       :default "crux-transaction-log"
                       :crux.config/type :crux.config/string}
                      ::doc-topic
                      {:doc "Kafka document topic"
                       :default "crux-docs"
                       :crux.config/type :crux.config/string}
                      ::doc-partitions
                      {:doc "Partitions for document topic"
                       :default 1
                       :crux.config/type :crux.config/nat-int}
                      ::create-topics
                      {:doc "Create topics if they do not exist"
                       :default true
                       :crux.config/type :crux.config/boolean}
                      ::replication-factor
                      {:doc "Level of durability for Kakfa"
                       :default 1
                       :crux.config/type :crux.config/nat-int}
                      ::group-id
                      {:doc "Kafka client group.id"
                       :default (str/trim (or (System/getenv "HOSTNAME")
                                              (System/getenv "COMPUTERNAME")
                                              (.toString (java.util.UUID/randomUUID))))
                       :crux.config/type :crux.config/string}
                      ::kafka-properties-file
                      {:doc "Used for supplying Kakfa connection properties to the underlying Kafka API."
                       :crux.config/type :crux.config/string}
                      ::kafka-properties-map
                      {:doc "Used for supplying Kakfa connection properties to the underlying Kafka API."
                       :crux.config/type [map? identity]}})

(def indexing-consumer {:start-fn (fn [{:keys [crux.kafka/admin-client crux.node/indexer]} options]
                                    (let [kafka-config (derive-kafka-config options)
                                          consumer-config (merge {"group.id" (::group-id options)} kafka-config)]
                                      (start-indexing-consumer admin-client consumer-config indexer options)))
                        :deps [:crux.node/indexer ::admin-client]
                        :args default-options})

(def admin-client {:start-fn (fn [_ options]
                               (create-admin-client (derive-kafka-config options)))
                   :args default-options})

(def admin-wrapper {:start-fn (fn [{::keys [admin-client]} _]
                                (reify Closeable
                                  (close [_])))
                    :deps [::admin-client]})

(def producer {:start-fn (fn [_ options]
                           (create-producer (derive-kafka-config options)))
               :args default-options})

(def tx-log {:start-fn (fn [{::keys [producer]} {:keys [crux.kafka/tx-topic crux.kafka/doc-topic] :as options}]
                         (->KafkaTxLog producer tx-topic doc-topic (derive-kafka-config options)))
             :deps [::producer]
             :args default-options})

(def topology (merge n/base-topology
                     {:crux.node/tx-log tx-log
                      ::admin-client admin-client
                      ::admin-wrapper admin-wrapper
                      ::producer producer
                      ::indexing-consumer indexing-consumer}))
