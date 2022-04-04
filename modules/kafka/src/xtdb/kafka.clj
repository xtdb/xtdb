(ns xtdb.kafka
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.io :as xio]
            [xtdb.status :as status]
            [xtdb.system :as sys]
            [xtdb.tx.event :as txe]
            [xtdb.tx.subscribe :as tx-sub])
  (:import clojure.lang.MapEntry
           [xtdb.kafka.nippy NippyDeserializer NippySerializer]
           java.io.Closeable
           java.nio.file.Path
           java.time.Duration
           [java.util Collection Date Map UUID]
           [java.util.concurrent CompletableFuture ExecutionException]
           [org.apache.kafka.clients.admin AdminClient NewTopic TopicDescription]
           [org.apache.kafka.clients.consumer ConsumerRebalanceListener ConsumerRecord KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord RecordMetadata]
           [org.apache.kafka.common PartitionInfo TopicPartition]
           [org.apache.kafka.common.errors InterruptException TopicExistsException]))

(defn ->kafka-config {::sys/args {:bootstrap-servers {:spec ::sys/string
                                                      :doc "URL for connecting to Kafka, eg \"kafka-cluster-kafka-brokers.xtdb.svc.cluster.local:9092\""
                                                      :required? true
                                                      :default "localhost:9092"}
                                  :properties-file {:spec ::sys/path
                                                    :doc "Used for supplying Kafka connection properties to the underlying Kafka API."}
                                  :properties-map {:spec ::sys/string-map
                                                   :doc "Used for supplying Kafka connection properties to the underlying Kafka API."}}}
  [{:keys [bootstrap-servers ^Path properties-file properties-map]}]
  (merge {"bootstrap.servers" bootstrap-servers}
         (when properties-file
           (with-open [in (io/reader (.toFile properties-file))]
             (xio/load-properties in)))
         properties-map))

(defn ->topic-opts {::sys/args {:topic-name {:required? true
                                             :spec ::sys/string}
                                :num-partitions {:required? true
                                                 :default 1
                                                 :spec ::sys/pos-int}
                                :replication-factor {:required? true
                                                     :default 1
                                                     :doc "Level of durability for Kafka"
                                                     :spec ::sys/pos-int}
                                :create-topics? {:required? true
                                                 :default true
                                                 :doc "Create topics if they do not exist"
                                                 :spec ::sys/boolean}
                                :topic-config {:spec ::sys/string-map}}}
  [opts]
  (-> opts
      (update :topic-config (fn [config]
                              (merge {"message.timestamp.type" "LogAppendTime"}
                                     config)))))

(defn ->producer {::sys/deps {:kafka-config `->kafka-config}}
  ^org.apache.kafka.clients.producer.KafkaProducer
  [{:keys [kafka-config]}]
  (KafkaProducer. ^Map (merge {"enable.idempotence" "true"
                               "acks" "all"
                               "compression.type" "snappy"
                               "key.serializer" (.getName NippySerializer)
                               "value.serializer" (.getName NippySerializer)}
                              kafka-config)))

(defn ->consumer {::sys/deps {:kafka-config `->kafka-config}}
  ^org.apache.kafka.clients.consumer.KafkaConsumer
  [{:keys [kafka-config]}]
  (KafkaConsumer. ^Map (merge {"enable.auto.commit" "false"
                               "isolation.level" "read_committed"
                               "auto.offset.reset" "earliest"
                               "key.deserializer" (.getName NippyDeserializer)
                               "value.deserializer" (.getName NippyDeserializer)}
                              kafka-config)))

(defn ->admin-client {::sys/deps {:kafka-config `->kafka-config}}
  ^org.apache.kafka.clients.admin.AdminClient
  [{:keys [kafka-config]}]
  (AdminClient/create ^Map kafka-config))

(defn- create-topic [^AdminClient admin-client {:keys [topic-name num-partitions replication-factor topic-config]}]
  (let [new-topic (doto (NewTopic. ^String topic-name ^int num-partitions ^short (short replication-factor))
                    (.configs topic-config))]
    (try
      @(.all (.createTopics admin-client [new-topic]))
      (catch ExecutionException e
        (let [cause (.getCause e)]
          (when-not (instance? TopicExistsException cause)
            (throw e)))))))

(defn- ensure-topic-exists [admin-client {:keys [create-topics?] :as topic-opts}]
  (when create-topics?
    (create-topic admin-client topic-opts)))

(defn- seek-consumer [^KafkaConsumer consumer tp-offsets]
  ;; tp-offsets :: TP -> offset
  (doseq [^TopicPartition tp (.assignment consumer)]
    (if-let [next-offset (get tp-offsets tp)]
      (.seek consumer tp ^long next-offset)
      (.seekToBeginning consumer [tp]))))

(defn- subscribe-consumer [^KafkaConsumer consumer ^Collection topics tp-offsets]
  (.subscribe consumer topics (reify ConsumerRebalanceListener
                                (onPartitionsRevoked [_ partitions]
                                  (log/debug "Partitions revoked:" (str partitions)))
                                (onPartitionsAssigned [_ partitions]
                                  (log/debug "Partitions assigned:" (str partitions))
                                  (seek-consumer consumer tp-offsets)))))

(defn- poll-consumer [^KafkaConsumer consumer ^Duration poll-duration]
  (xio/with-nippy-thaw-all
    (try
      (.poll consumer poll-duration)
      (catch InterruptException e
        (Thread/interrupted)
        (throw (.getCause e))))))

(defn- consumer-seqs [consumer poll-duration]
  (lazy-seq
   (log/trace "polling")
   (when-let [records (seq (poll-consumer consumer poll-duration))]
     (log/tracef "got %d records" (count records))
     (cons records (consumer-seqs consumer poll-duration)))))

;;;; TxLog

(defn- ensure-tx-topic-has-single-partition [^AdminClient admin-client tx-topic]
  (let [name->description @(.all (.describeTopics admin-client [tx-topic]))]
    (assert (= 1 (count (.partitions ^TopicDescription (get name->description tx-topic)))))))

(defn- tx-record->tx-log-entry [^ConsumerRecord record]
  (xio/conform-tx-log-entry {::xt/tx-id (.offset record)
                             ::xt/tx-time (Date. (.timestamp record))}
                            (.value record)))

(defn- open-consumer ^org.apache.kafka.clients.consumer.KafkaConsumer [{:keys [kafka-config tx-topic]} after-tx-id]
  (let [tp-offsets {(TopicPartition. tx-topic 0) (some-> after-tx-id inc)}]
    (doto (->consumer {:kafka-config kafka-config})
      (.assign (keys tp-offsets))
      (seek-consumer tp-offsets))))

(defn- handle-subscriber [{:keys [^Duration poll-wait-duration] :as tx-log} after-tx-id f]
  (tx-sub/completable-thread
   (fn [^CompletableFuture fut]
     (with-open [consumer (open-consumer tx-log after-tx-id)]
       (loop []
         (->> (poll-consumer consumer poll-wait-duration)
              (map tx-record->tx-log-entry)
              (reduce (fn [_ tx]
                        (when (Thread/interrupted)
                          (throw (InterruptedException.)))

                        (when (.isDone fut)
                          (reduced nil))

                        (f fut tx)

                        true)
                      false))

         (cond
           (.isDone fut) nil
           (Thread/interrupted) (throw (InterruptedException.))
           :else (recur)))))))

(defrecord KafkaTxLog [^KafkaProducer producer,
                       tx-topic, kafka-config,
                       ^Duration poll-wait-duration
                       ^Closeable consumer]
  db/TxLog
  (submit-tx [this tx-events] (db/submit-tx this tx-events {}))

  (submit-tx [_ tx-events opts]
    (let [tx-send-future (.send producer (ProducerRecord. tx-topic nil {::txe/tx-events tx-events
                                                                        ::xt/submit-tx-opts opts}))]
      (delay
        (let [record-meta ^RecordMetadata @tx-send-future]
          {::xt/tx-id (.offset record-meta)
           ::xt/tx-time (or (::xt/tx-time opts)
                            (Date. (.timestamp record-meta)))}))))

  (open-tx-log [this after-tx-id]
    (let [consumer (open-consumer this after-tx-id)]
      (xio/->cursor #(.close consumer)
                    (->> (consumer-seqs consumer poll-wait-duration)
                         (mapcat identity)
                         (map tx-record->tx-log-entry)))))

  (subscribe [this after-tx-id f]
    (handle-subscriber this after-tx-id f))

  (latest-submitted-tx [_]
    (with-open [consumer (->consumer {:kafka-config kafka-config})]
      (let [tx-tp (TopicPartition. tx-topic 0)
            end-offset (-> (.endOffsets consumer [tx-tp]) (get tx-tp))]
        (when (pos? end-offset)
          {::xt/tx-id (dec end-offset)}))))

  status/Status
  (status-map [_]
    {:xtdb.zk/zk-active?
     (try
       (with-open [consumer (->consumer {:kafka-config kafka-config})]
         (boolean (.listTopics consumer)))
       (catch Exception e
         (log/debug e "Could not list Kafka topics:")
         false))})

  Closeable
  (close [_]
    (xio/try-close producer)))

(defn ->tx-log {::sys/deps {:kafka-config `->kafka-config
                            :tx-topic-opts {:xtdb/module `->topic-opts, :topic-name "crux-transaction-log"}}
                ::sys/args {:poll-wait-duration {:spec ::sys/duration
                                                 :required? true
                                                 :doc "How long to wait when polling Kafka"
                                                 :default (Duration/ofSeconds 1)}}}

  [{:keys [tx-topic-opts kafka-config poll-wait-duration]}]
  (let [producer (->producer {:kafka-config kafka-config})
        tx-topic-opts (-> tx-topic-opts
                          (assoc :num-partitions 1)
                          (update :topic-config
                                  (fn [topic-config]
                                    (merge {"retention.ms" (str Long/MAX_VALUE)}
                                           topic-config))))
        tx-topic (:topic-name tx-topic-opts)]
    (with-open [admin-client (->admin-client {:kafka-config kafka-config})]
      (ensure-topic-exists admin-client tx-topic-opts)
      (ensure-tx-topic-has-single-partition admin-client tx-topic))
    (map->KafkaTxLog {:producer producer
                      :tx-topic tx-topic
                      :kafka-config kafka-config
                      :poll-wait-duration poll-wait-duration})))

;;;; DocumentStore
(defn- submit-docs [id-and-docs {:keys [^KafkaProducer producer, doc-topic]}]
  ;; TODO this no longer preserves submit-tx-async semantics, see #1266
  (let [fs (doall (for [[content-hash doc] id-and-docs]
                    (->> (ProducerRecord. doc-topic content-hash doc)
                         (.send producer))))]
    (.flush producer)
    (doseq [f fs]
      @f)))

(defn- read-doc-offsets [index-store]
  (->> (db/read-index-meta index-store :xtdb.tx-log/consumer-state)
       (into {} (map (fn [[k {:keys [next-offset]}]]
                       [(let [[_ t p] (re-matches #"(.+)-(\d+)" k)]
                          (TopicPartition. t (Long/parseLong p)))
                        next-offset])))))

(defn- store-doc-offsets [index-store tp-offsets]
  (db/store-index-meta index-store :xtdb.tx-log/consumer-state
                       (->> tp-offsets
                            (into {} (map (fn [[k v]]
                                            [(str k) {:next-offset v}]))))))

(defn- update-doc-offsets [tp-offsets doc-records]
  (reduce (fn [tp-offsets ^ConsumerRecord record]
            (assoc tp-offsets
                   (TopicPartition. (.topic record) (.partition record)) (inc (.offset record))))
          tp-offsets
          doc-records))

(defrecord KafkaDocumentStore [^KafkaProducer producer doc-topic
                               ^KafkaConsumer end-offset-consumer
                               local-document-store index-store
                               ^Thread indexing-thread !indexing-error]
  Closeable
  (close [_]
    (xio/try-close end-offset-consumer)
    (xio/try-close producer)
    (.interrupt indexing-thread)
    (.join indexing-thread))

  db/DocumentStore
  (submit-docs [this id-and-docs]
    (submit-docs id-and-docs this)

    ;; for #1256 - let's fast-track tx-fn doc replacements straight into the local doc-store to prevent the race condition.
    ;; we don't do this for all docs because of put/evict ordering - would prefer the topic to be the authority on this.
    (some->> (seq (->> id-and-docs
                       (filter (comp (some-fn :crux.db.fn/tx-events :crux.db.fn/failed?) val))))
             (db/submit-docs local-document-store)))

  (fetch-docs [_ ids]
    (let [ids (set ids)

          ;; ideally we'd use AdminClient.listOffsets for this, but it was only introduced in 2.5.0
          ;; which may be a bit recent (April 2020) for XTDB folks
          !end-offsets (delay
                         (.endOffsets end-offset-consumer
                                      (for [^PartitionInfo partition-info (.partitionsFor end-offset-consumer doc-topic)]
                                        (TopicPartition. doc-topic (.partition partition-info)))))]

      (loop [doc-offsets (read-doc-offsets index-store)
             docs (db/fetch-docs local-document-store ids)]
        (if (or (= (count docs) (count ids))
                (every? (fn [[tp end-offset]]
                          (or (zero? end-offset)
                              (when-let [consumed-offset (get doc-offsets tp)]
                                (>= consumed-offset end-offset))))
                        @!end-offsets))
          docs

          (do
            (Thread/sleep 100)

            (when-let [indexing-error @!indexing-error]
              (throw (IllegalStateException. "document indexing error" indexing-error)))

            (recur (read-doc-offsets index-store)
                   (into docs
                         (db/fetch-docs local-document-store (set/difference ids (set (keys docs))))))))))))

(defn doc-record->id+doc [^ConsumerRecord doc-record]
  (MapEntry/create (c/new-id (.key doc-record))
                   (.value doc-record)))

(defn- index-doc-log [{:keys [local-document-store index-store !indexing-error doc-topic-opts kafka-config group-id poll-wait-duration]}]
  (let [doc-topic (:topic-name doc-topic-opts)
        tp-offsets (read-doc-offsets index-store)]
    (try
      (with-open [consumer (doto (->consumer {:kafka-config (assoc kafka-config
                                                                   "group.id" (or group-id (str (UUID/randomUUID))))})
                             (subscribe-consumer #{doc-topic} tp-offsets))]
        (loop [tp-offsets tp-offsets]
          (let [tp-offsets (->> (consumer-seqs consumer poll-wait-duration)
                                (reduce (fn [tp-offsets doc-records]
                                          (db/submit-docs local-document-store (->> doc-records (into {} (map doc-record->id+doc))))
                                          (doto (update-doc-offsets tp-offsets doc-records)
                                            (->> (store-doc-offsets index-store))))
                                        tp-offsets))]
            (when (Thread/interrupted)
              (throw (InterruptedException.)))
            (recur tp-offsets))))
      (catch InterruptException _
        (Thread/interrupted))
      (catch InterruptedException _)
      (catch Exception e
        (reset! !indexing-error e)
        (log/error e "Error while consuming documents")))))

(defn- ensure-doc-topic-exists [{:keys [kafka-config doc-topic-opts]}]
  (with-open [admin-client (->admin-client {:kafka-config kafka-config})]
    (ensure-topic-exists admin-client (-> doc-topic-opts
                                          (update :topic-config
                                                  (fn [topic-config]
                                                    (merge {"retention.ms" (str Long/MAX_VALUE)
                                                            "cleanup.policy" "compact"}
                                                           topic-config)))))))

(defn ->document-store {::sys/deps {:kafka-config `->kafka-config
                                    :doc-topic-opts {:xtdb/module `->topic-opts,
                                                     :topic-name "crux-docs",
                                                     :num-partitions 1}
                                    :local-document-store 'xtdb.kv.document-store/->document-store
                                    :index-store :xtdb/index-store}
                        ::sys/args {:group-id {:doc "Kafka client group.id"
                                               :required? false
                                               :spec ::sys/string}
                                    :poll-wait-duration {:spec ::sys/duration
                                                         :required? true
                                                         :doc "How long to wait when polling Kafka"
                                                         :default (Duration/ofSeconds 1)}} }
  [{:keys [index-store local-document-store kafka-config doc-topic-opts] :as opts}]
  (ensure-doc-topic-exists opts)

  (let [!indexing-error (atom nil)]
    (map->KafkaDocumentStore {:producer (->producer {:kafka-config kafka-config})
                              :end-offset-consumer (->consumer {:kafka-config kafka-config})
                              :doc-topic (:topic-name doc-topic-opts)
                              :index-store index-store
                              :local-document-store local-document-store
                              :!indexing-error !indexing-error
                              :indexing-thread (doto (Thread. #(index-doc-log (assoc opts :!indexing-error !indexing-error)))
                                                 (.setName "xtdb-doc-consumer")
                                                 (.start))})))

(defrecord SubmitOnlyDocumentStore [^KafkaProducer producer doc-topic]
  db/DocumentStore
  (submit-docs [this id-and-docs]
    (submit-docs id-and-docs this))

  (fetch-docs [this ids]
    (throw (UnsupportedOperationException. "Can't fetch docs from submit-only Kafka document store"))))

(defn ->submit-only-document-store {::sys/deps {:kafka-config `->kafka-config
                                                :doc-topic-opts {:xtdb/module `->topic-opts
                                                                 :topic-name "crux-docs"
                                                                 :num-partitions 1}}}
  [{:keys [kafka-config doc-topic-opts] :as opts}]
  (ensure-doc-topic-exists opts)
  (->SubmitOnlyDocumentStore (->producer {:kafka-config kafka-config})
                             (:topic-name doc-topic-opts)))
