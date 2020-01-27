(ns crux.kafka
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.node :as n]
            [crux.tx :as tx]
            [crux.kafka.consumer :as kc]
            [taoensso.nippy :as nippy]
            [crux.kv :as kv])
  (:import crux.kafka.nippy.NippySerializer
           crux.db.DocumentStore
           java.io.Closeable
           java.time.Duration
           [java.util Date Map]
           java.util.concurrent.ExecutionException
           [org.apache.kafka.clients.admin AdminClient NewTopic TopicDescription]
           [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
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

(defn- ensure-topic-exists [admin-client topic topic-config partitions {:keys [crux.kafka/replication-factor
                                                                               crux.kafka/create-topics]}]
  (when create-topics
    (create-topic admin-client topic partitions replication-factor topic-config)))

(defn- ensure-tx-topic-has-single-partition [^AdminClient admin-client tx-topic]
  (let [name->description @(.all (.describeTopics admin-client [tx-topic]))]
    (assert (= 1 (count (.partitions ^TopicDescription (get name->description tx-topic)))))))

(defn tx-record->tx-log-entry [^ConsumerRecord record]
  {:crux.tx.event/tx-events (.value record)
   :crux.tx/tx-id (.offset record)
   :crux.tx/tx-time (Date. (.timestamp record))})

(defrecord KafkaTxLog [^DocumentStore doc-store, ^KafkaProducer producer, ^KafkaConsumer latest-submitted-tx-consumer, tx-topic, kafka-config]
  Closeable
  (close [_])

  db/TxLog
  (submit-tx [this tx-ops]
    (try
      (let [tx-events (map tx/tx-op->tx-event tx-ops)
            content-hashes (->> (set (map c/new-id (mapcat tx/tx-op->docs tx-ops))))
            tx-send-future (->> (doto (ProducerRecord. tx-topic nil tx-events)
                                  (-> (.headers) (.add (str :crux.tx/docs)
                                                       (nippy/fast-freeze content-hashes))))
                                (.send producer))]
        (delay
         (let [record-meta ^RecordMetadata @tx-send-future]
           {:crux.tx/tx-id (.offset record-meta)
            :crux.tx/tx-time (Date. (.timestamp record-meta))})))))

  (open-tx-log [this from-tx-id]
    (let [tx-topic-consumer ^KafkaConsumer (kc/create-consumer (assoc kafka-config "enable.auto.commit" "false"))
          tx-topic-partition (TopicPartition. tx-topic 0)]
      (.assign tx-topic-consumer [tx-topic-partition])
      (if from-tx-id
        (.seek tx-topic-consumer tx-topic-partition (long from-tx-id))
        (.seekToBeginning tx-topic-consumer (.assignment tx-topic-consumer)))
      (db/->closeable-tx-log-iterator
       #(.close tx-topic-consumer)
       ((fn step []
           (when-let [records (seq (.poll tx-topic-consumer (Duration/ofMillis 1000)))]
             (concat (map tx-record->tx-log-entry records)
                     (step))))))))

  (latest-submitted-tx [this]
    (let [tx-tp (TopicPartition. tx-topic 0)
          end-offset (-> (.endOffsets latest-submitted-tx-consumer [tx-tp]) (get tx-tp))]
      (when (pos? end-offset)
        {:crux.tx/tx-id (dec end-offset)}))))

(defn consume-and-index-txes
  [{:keys [offsets indexer pending-txs-state timeout tx-topic]
    :or {timeout 5000}}
   ^KafkaConsumer consumer]
  (let [tx-topic-partition (TopicPartition. tx-topic 0)
        _ (when (and (.contains (.paused consumer) tx-topic-partition)
                     (empty? @pending-txs-state))
            (log/debug "Resuming" tx-topic)
            (.resume consumer [tx-topic-partition]))
        records (.poll consumer (Duration/ofMillis timeout))
        tx-records (vec (.records records (str tx-topic)))
        pending-tx-records (swap! pending-txs-state into tx-records)
        tx-records (->> pending-tx-records
                        (take-while
                         (fn [^ConsumerRecord tx-record]
                           (let [content-hashes (->> (.lastHeader (.headers tx-record)
                                                                  (str :crux.tx/docs))
                                                     (.value)
                                                     (nippy/fast-thaw))
                                 ready? (db/docs-indexed? indexer content-hashes)
                                 {:crux.tx/keys [tx-time
                                                 tx-id]} (tx-record->tx-log-entry tx-record)]
                             (if ready?
                               (log/info "Ready for indexing of tx" tx-id (cio/pr-edn-str tx-time))
                               (do (when-not (.contains (.paused consumer) tx-topic-partition)
                                     (log/debug "Pausing" tx-topic)
                                     (.pause consumer [tx-topic-partition]))
                                   (log/info "Delaying indexing of tx" tx-id (cio/pr-edn-str tx-time) "pending:" (count pending-tx-records))))
                             ready?)))
                        (vec))]

    (doseq [record tx-records]
      (let [{:keys [crux.tx.event/tx-events] :as record} (tx-record->tx-log-entry record)]
        (db/index-tx indexer (select-keys record [:crux.tx/tx-time :crux.tx/tx-id]) tx-events)))

    (kc/update-stored-consumer-state offsets consumer records)
    (swap! pending-txs-state (comp vec (partial drop (count tx-records))))

    (count tx-records)))

(defrecord KafkaDocumentStore [^KafkaProducer producer, doc-topic]
  Closeable
  (close [_])

  db/DocumentStore
  (submit-docs [this id-and-docs]
    (doseq [[content-hash doc] id-and-docs]
      @(->> (ProducerRecord. doc-topic content-hash doc)
            (.send producer)))
    (.flush producer)))

(defn consume-and-index-documents
  [{:keys [offsets indexer timeout doc-topic]
    :or {timeout 5000}}
   ^KafkaConsumer consumer]
  (let [records (.poll consumer (Duration/ofMillis timeout))
        doc-records (vec (.records records (str doc-topic)))]
    (db/index-docs indexer (->> doc-records
                                (into {} (map (fn [^ConsumerRecord record]
                                                [(c/new-id (.key record)) (.value record)])))))
    (when-let [records (seq doc-records)]
      (kc/update-stored-consumer-state offsets consumer records))
    (count doc-records)))

(defn consume-and-index-documents-from-txes
  [{:keys [offsets indexer document-store timeout tx-topic]
    :or {timeout 5000}}
   ^KafkaConsumer consumer]
  (let [records (.poll consumer (Duration/ofMillis timeout))
        tx-records (vec (.records records (str tx-topic)))]
    (doseq [^ConsumerRecord tx-record tx-records]
      (let [content-hashes (->> (.lastHeader (.headers tx-record)
                                             (str :crux.tx/docs))
                                (.value)
                                (nippy/fast-thaw))]
        (let [docs (db/fetch-docs document-store content-hashes)]
          (db/index-docs indexer docs))))
    (when (seq tx-records)
      (kc/update-stored-consumer-state offsets consumer tx-records))
    (count tx-records)))

(defn- group-name []
  (str/trim (or (System/getenv "HOSTNAME")
                (System/getenv "COMPUTERNAME")
                (.toString (java.util.UUID/randomUUID)))))

(def default-options
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
                     :default 1
                     :crux.config/type :crux.config/nat-int}
   ::create-topics {:doc "Create topics if they do not exist"
                    :default true
                    :crux.config/type :crux.config/boolean}
   ::replication-factor {:doc "Level of durability for Kafka"
                         :default 1
                         :crux.config/type :crux.config/nat-int}
   ::group-id {:doc "Kafka client group.id"
               :default (group-name)
               :crux.config/type :crux.config/string}
   ::kafka-properties-file {:doc "Used for supplying Kafka connection properties to the underlying Kafka API."
                            :crux.config/type :crux.config/string}
   ::kafka-properties-map {:doc "Used for supplying Kafka connection properties to the underlying Kafka API."
                           :crux.config/type [map? identity]}})

(def tx-indexing-consumer
  {:start-fn (fn [{:keys [crux.kafka/admin-client crux.node/indexer]}
                  {::keys [tx-topic] :as options}]
               (let [kafka-config (derive-kafka-config options)
                     consumer-config (merge {"group.id" (::group-id options)} kafka-config)
                     offsets (kc/map->IndexedOffsets {:indexer indexer :k :crux.tx-log/consumer-state})
                     index-fn (partial consume-and-index-txes
                                       {:indexer indexer
                                        :offsets offsets
                                        :timeout 1000
                                        :pending-txs-state (atom [])
                                        :tx-topic tx-topic})]
                 (ensure-topic-exists admin-client tx-topic tx-topic-config 1 options)
                 (ensure-tx-topic-has-single-partition admin-client tx-topic)
                 (kc/start-indexing-consumer consumer-config offsets tx-topic index-fn)))
   :deps [:crux.node/indexer ::admin-client]
   :args default-options})

(def doc-indexing-consumer
  {:start-fn (fn [{:keys [crux.kafka/admin-client crux.node/indexer]}
                  {::keys [doc-topic doc-partitions] :as options}]
               (let [kafka-config (derive-kafka-config options)
                     consumer-config (merge {"group.id" (::group-id options)} kafka-config)
                     offsets (kc/map->IndexedOffsets {:indexer indexer :k :crux.doc-log/consumer-state})
                     index-fn (partial consume-and-index-documents {:indexer indexer
                                                                    :offsets offsets
                                                                    :timeout 1000
                                                                    :doc-topic (::doc-topic options)})]
                 (ensure-topic-exists admin-client doc-topic doc-topic-config doc-partitions options)
                 (kc/start-indexing-consumer consumer-config offsets doc-topic index-fn)))
   :deps [:crux.node/indexer ::admin-client]
   :args default-options})

(def doc-indexing-from-tx-topic-consumer
  {:start-fn (fn [{:keys [crux.node/indexer crux.node/document-store]}
                  {::keys [tx-topic] :as options}]
               (let [kafka-config (derive-kafka-config options)
                     consumer-config (merge {"group.id" (::doc-group-id options)} kafka-config)
                     offsets (kc/map->IndexedOffsets {:indexer indexer :k :crux.tx-doc-log/consumer-state})
                     index-fn (partial consume-and-index-documents-from-txes {:indexer indexer
                                                                              :document-store document-store
                                                                              :offsets offsets
                                                                              :timeout 1000
                                                                              :tx-topic (::tx-topic options)})]
                 (kc/start-indexing-consumer consumer-config offsets tx-topic index-fn)))
   :deps [:crux.node/indexer :crux.node/document-store ::tx-indexing-consumer]
   :args (assoc default-options
                ::doc-group-id {:doc "Kafka client group.id for ingesting documents using tx topic"
                                :default (str "documents-" (group-name))
                                :crux.config/type :crux.config/string})})

(def admin-client
  {:start-fn (fn [_ options]
               (create-admin-client (derive-kafka-config options)))
   :args default-options})

(def admin-wrapper
  {:start-fn (fn [{::keys [admin-client]} _]
               (reify Closeable
                 (close [_])))
   :deps [::admin-client]})

(def producer
  {:start-fn (fn [_ options]
               (create-producer (derive-kafka-config options)))
   :args default-options})

(def latest-submitted-tx-consumer
  {:start-fn (fn [_ options]
               (kc/create-consumer (derive-kafka-config options)))
   :args default-options})

(def tx-log
  {:start-fn (fn [{:keys [crux.node/document-store ::producer ::latest-submitted-tx-consumer]}
                  {:keys [crux.kafka/tx-topic] :as options}]
               (->KafkaTxLog document-store producer latest-submitted-tx-consumer tx-topic (derive-kafka-config options)))
   :deps [::producer ::latest-submitted-tx-consumer :crux.node/document-store]
   :args default-options})

(def document-store
  {:start-fn (fn [{::keys [producer]} {:keys [crux.kafka/doc-topic] :as options}]
               (->KafkaDocumentStore producer doc-topic))
   :deps [::producer]
   :args default-options})

(def topology
  (merge n/base-topology
         {:crux.node/tx-log tx-log
          :crux.node/document-store document-store
          ::admin-client admin-client
          ::admin-wrapper admin-wrapper
          ::producer producer
          ::tx-indexing-consumer tx-indexing-consumer
          ::doc-indexing-consumer doc-indexing-consumer
          ::latest-submitted-tx-consumer latest-submitted-tx-consumer}))
