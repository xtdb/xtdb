(ns xtdb.bench.kafka-source
  "Benchmarks the !KafkaConnect external source: pre-loads a Kafka topic with
   small patient-shaped JSON records, attaches a source database, and measures
   how fast the source drains the topic into its `patient` table.

   Needs a local Kafka broker (`docker-compose up kafka`) and a node config that
   registers it as a remote - see modules/bench/config/kafka-source.yaml (the
   Gradle task defaults to it).

   Run: ./gradlew kafka-source -PmessageCount=10000 [-Pyourkit]"
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.util :as util])
  (:import [io.micrometer.core.instrument Counter DistributionSummary Gauge Meter MeterRegistry]
           [java.sql Connection Statement]
           [java.time Instant]
           [java.util Properties]
           [org.apache.kafka.clients.admin AdminClient NewTopic]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization ByteArraySerializer StringSerializer]))

(defn- ->props ^Properties [m]
  (doto (Properties.) (.putAll m)))

(defn- create-topics! [{:keys [bootstrap-servers topics]}]
  (with-open [admin (AdminClient/create (->props {"bootstrap.servers" bootstrap-servers}))]
    (-> (.createTopics admin (for [^String topic topics]
                               (NewTopic. topic (int 1) (short 1))))
        (.all)
        (.get))))

(defn- produce! [{:keys [bootstrap-servers ^String source-topic message-count]}]
  (let [start-ms (System/currentTimeMillis)]
    (with-open [producer (KafkaProducer. (->props {"bootstrap.servers" bootstrap-servers
                                                   "key.serializer" (.getName StringSerializer)
                                                   "value.serializer" (.getName ByteArraySerializer)}))]
      (dotimes [_ message-count]
        (when (Thread/interrupted) (throw (InterruptedException.)))
        (let [id (str (random-uuid))
              ^String json (json/write-str {:_id id
                                            :resource_type "patient"
                                            :status "bench"
                                            :last_updated (str (Instant/now))})]
          (.send producer (ProducerRecord. source-topic id (.getBytes json)))))
      (.flush producer))
    (let [secs (/ (- (System/currentTimeMillis) start-ms) 1000.0)]
      (log/infof "produced %,d messages in %.3fs (%.1f msgs/sec)"
                 (long message-count) secs (/ message-count secs)))))

(defn- attach-source-db! [node {:keys [db-name ^String source-topic]}]
  (with-open [^Connection conn (jdbc/get-connection node)
              ^Statement stmt (.createStatement conn)]
    (.execute stmt
              (format "ATTACH DATABASE %s WITH $$
log: !Kafka
  cluster: kafka
  topic: %s-replica
externalSource: !KafkaConnect
  remote: kafka
  topic: %s
  connectConfig:
    key.converter: org.apache.kafka.connect.storage.StringConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable: \"false\"
  indexer: !Docs
    table: patient
$$"
                      db-name source-topic source-topic))))

(defn- drain! [node {:keys [db-name ^long message-count]}]
  (let [query (format "SELECT COUNT(*) n FROM %s.public.patient" db-name)
        ;; table only exists once the source has indexed its first record
        row-count (fn [] (or (try
                               (:n (first (xt/q node [query])))
                               (catch Exception _ nil))
                             0))
        start-ms (System/currentTimeMillis)]
    (loop [logged-ms start-ms, logged-n 0]
      (let [n (long (row-count))
            now-ms (System/currentTimeMillis)]
        (if (>= n message-count)
          (let [secs (/ (- now-ms start-ms) 1000.0)]
            (log/infof "drain complete: %,d rows in %.3fs (%.1f rows/sec)" n secs (/ n secs)))
          (if (>= (- now-ms logged-ms) 10000)
            (do (log/infof "drained %,d/%,d rows (%.1f rows/sec)"
                           n message-count (/ (- n logged-n) (/ (- now-ms logged-ms) 1000.0)))
                (Thread/sleep 1000)
                (recur now-ms n))
            (do (Thread/sleep 1000)
                (recur logged-ms logged-n))))))))

(defn- meter-value [m]
  (condp instance? m
    Counter {:count (.count ^Counter m)}
    Gauge {:value (.value ^Gauge m)}
    DistributionSummary (let [^DistributionSummary m m]
                          {:count (.count m), :total (.totalAmount m), :max (.max m)})
    {:type (str (class m))}))

(defn- log-source-metrics []
  (when-let [^MeterRegistry reg b/*registry*]
    (doseq [^Meter m (->> (.getMeters reg)
                          (filter #(= "kafka-connect" (-> ^Meter % (.getId) (.getTag "source_type"))))
                          (sort-by #(-> ^Meter % (.getId) (.getName))))]
      (log/info "source meter:" (-> m (.getId) (.getName)) (meter-value m)))))

(defmethod b/cli-flags :kafka-source [_]
  [[nil "--message-count COUNT" "Number of messages to produce"
    :id :message-count
    :parse-fn parse-long
    :default 10000]

   [nil "--bootstrap-servers SERVERS" "Kafka bootstrap servers - must match the `kafka` remote in the node config"
    :id :bootstrap-servers
    :default "localhost:9092"]

   ["-h" "--help"]])

(defn benchmark [{:keys [message-count bootstrap-servers]
                  :or {message-count 10000, bootstrap-servers "localhost:9092"}}]
  (let [source-topic (str "bench-kafka-source-" (subs (str (random-uuid)) 0 8))
        opts {:bootstrap-servers bootstrap-servers
              :source-topic source-topic
              :topics [source-topic (str source-topic "-replica")]
              :message-count message-count
              :db-name "bench_kafka_src"}]
    {:title "Kafka Connect source ingestion"
     :benchmark-type :kafka-source
     :parameters {:message-count message-count}
     :tasks [{:t :call, :stage :create-topics
              :f (fn [_] (create-topics! opts))}

             {:t :call, :stage :produce
              :f (fn [_] (produce! opts))}

             ;; produce happens before attach, so the drain stage is pure
             ;; source-consumption + indexing - a clean profiling window
             {:t :call, :stage :attach
              :f (fn [{:keys [node]}] (attach-source-db! node opts))}

             {:t :call, :stage :drain
              :f (fn [{:keys [node]}]
                   (drain! node opts)
                   (log-source-metrics))}]}))

(defmethod b/->benchmark :kafka-source [_ opts]
  (benchmark opts))

(comment
  ;; REPL workflow (e.g. under `./gradlew :clojureRepl -Pyourkit` for live-attach
  ;; profiling) - needs `docker-compose up kafka`:
  (require '[xtdb.node :as xtn])
  (import '[xtdb.api Xtdb]
          '[xtdb.api.log KafkaCluster$ClusterFactory])

  (def node
    ;; the map config has no hook for remotes, so register the cluster on the Config
    (-> (xtn/->config {})
        (.logCluster "kafka" (KafkaCluster$ClusterFactory. "localhost:9092"))
        (Xtdb/openNode)))

  (let [f (b/compile-benchmark (benchmark {:message-count 1000}))]
    (f node))

  (util/close node))
