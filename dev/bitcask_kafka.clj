(ns bitcask-kafka
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.kafka :as k]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [taoensso.nippy :as nippy])
  (:import [crux.kafka.nippy NippyDeserializer NippySerializer]
           java.util.function.Supplier
           java.io.Closeable
           java.time.Duration
           java.util.List
           [org.apache.kafka.clients.consumer ConsumerRebalanceListener ConsumerRecord KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord RecordMetadata]
           [org.apache.kafka.common.serialization BytesSerializer StringSerializer]
           org.apache.kafka.common.TopicPartition))

;; Spike implementation of crux.db.ObjectStore on top of two Kafka
;; partitions, based on https://github.com/basho/bitcask

;; Should really spill the local cached values out to disk,
;; potentially using something like
;; https://github.com/JakeWharton/DiskLruCache

;; The hint map could also be cached locally to disk. The offset to
;; seek to would be the max + 1.

(def ^:private topic-config {"cleanup.policy" "compact"})
(def ^:private default-consumer-config {"enable.auto.commit" "false"})
(def ^:private data-consumer-config {"max.poll.records" "1"})

(defn- hint-consumer-main-loop [keydir running? consumer-config {:keys [timeout] :or {timeout 1000} :as options}]
  (with-open [consumer (k/create-consumer consumer-config)]
    (let [topic-partitions [(TopicPartition. (get options :hint-topic) 0)]]
      (.assign consumer topic-partitions)
      (.seekToBeginning consumer topic-partitions)
      (while @running?
        (when-let [records (seq (.poll consumer (Duration/ofMillis timeout)))]
          (->> (for [^ConsumerRecord record records]
                 [(.key record) (.value record)])
               (into {})
               (swap! keydir merge)))))))

(defrecord BitcaskKafkaObjectStore [keydir running? consumer-config
                                    ^KafkaProducer producer consumers ^ThreadLocal consumer-tl
                                    object-cache
                                    options ^Thread worker-thread]
  db/ObjectStore
  (init [this
         {:keys [admin-client]
          :as partial-node}
         {:keys [data-topic hint-topic data-partitions replication-factor object-cache-size]
          :or {data-topic "data-topic"
               hint-topic "hint-topic"
               data-partitions 3
               replication-factor 1
               object-cache-size 10000}
          :as options}]
    (k/create-topic admin-client hint-topic 1 replication-factor topic-config)
    (k/create-topic admin-client data-topic data-partitions replication-factor topic-config)
    (let [consumer-config (merge (@#'k/derive-kafka-config options) default-consumer-config)
          keydir (atom (sorted-map))
          running? (atom true)
          options (assoc options :hint-topic hint-topic :data-topic data-topic)
          consumers (atom #{})
          consumer-tl (ThreadLocal/withInitial
                       (reify Supplier
                         (get [_]
                           (doto (k/create-consumer (merge consumer-config data-consumer-config))
                             (->> (swap! consumers conj))))))]
      (assoc this
             :keydir keydir
             :running? running?
             :consumer-config consumer-config
             :producer (k/create-producer consumer-config)
             :options options
             :consumer-tl consumer-tl
             :consumers consumers
             :object-cache (lru/new-cache object-cache-size)
             :worker-thread
             (doto (Thread. ^Runnable #(hint-consumer-main-loop keydir running? consumer-config options)
                            "bitcask-kafka.indexing-consumer-thread")
               (.start)))))

  (get-single-object [this snapshot k]
    (let [doc-key (str (c/new-id k))]
      (when-let [{:bitcask-kafka/keys [partition offset deleted?]} (get @keydir doc-key)]
        (when-not deleted?
          (lru/compute-if-absent
           object-cache
           [doc-key partition offset]
           identity
           (fn [_]
             (let [^KafkaConsumer consumer (.get consumer-tl)
                   topic-partition (TopicPartition. (get options :data-topic) partition)]
               (.assign consumer [topic-partition])
               (.seek consumer topic-partition (long offset))
               (when-let [^ConsumerRecord record (first (.poll consumer (Duration/ofMillis 1000)))]
                 (.value record)))))))))

  (get-objects [this snapshot ks]
    (->> (for [k ks
               :let [v (db/get-single-object this nil k)]
               :when (not (nil? v))]
           [k v])
         (into {})))

  (known-keys? [this snapshot ks]
    (let [keydir @keydir]
      (->> (for [k ks]
             (str (c/new-id k)))
           (every? #(and (contains? keydir %)
                         (not (get-in keydir [% :bitcask-kafka/deleted?])))))))

  (put-objects [this kvs]
    (let [{:keys [data-topic hint-topic]} options
          k+deleted?+mds (->> (for [[k v] kvs
                                    :let [doc-key (str (c/new-id k))]]
                                [doc-key
                                 (nil? v)
                                 (.send producer (ProducerRecord. data-topic doc-key v))])
                              (doall))]
      (.flush producer)
      (doseq [[doc-key deleted? md] k+deleted?+mds
              :let [^RecordMetadata metadata @md
                    hint {:bitcask-kafka/partition (.partition metadata)
                          :bitcask-kafka/offset (.offset metadata)
                          :bitcask-kafka/deleted? deleted?}]]
        @(.send producer (ProducerRecord. hint-topic doc-key hint))
        (swap! keydir assoc doc-key hint))))

  (delete-objects [this ks]
    (db/put-objects this (zipmap ks (repeat nil))))

  Closeable
  (close [this]
    (reset! running? false)
    (.join worker-thread)
    (some-> producer (.close))
    (doseq [^KafkaConsumer consumer @consumers]
      (.close consumer))
    (dissoc this :keydir)))

(comment
  (dev)
  (start)

  (def os (db/init (bitcask-kafka/map->BitcaskKafkaObjectStore {})
                   node
                   (:options node)))


  (db/put-objects os [[:bar "foo"]])
  ;;=> nil
  @(:keydir os)
  ;;=> {key-hash hint}

  (db/known-keys? os nil [:bar])
  ;;=>  true
  (db/get-single-object os nil :bar)
  ;;=>  "foo"

  (db/delete-objects os [:bar])
  ;;=> nil
  (db/known-keys? os nil [:bar])
  ;;=>  false

  (db/get-single-object os nil :bar)
  ;;=> nil
  )
