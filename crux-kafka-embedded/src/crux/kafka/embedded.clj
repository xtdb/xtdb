(ns crux.kafka.embedded
  "Embedded Kafka for self-contained Crux deployments."
  (:require [clojure.java.io :as io]
            [crux.io :as cio]
            [clojure.spec.alpha :as s])
  (:import [kafka.server
            KafkaConfig KafkaServerStartable]
           [org.apache.zookeeper.server
            ServerCnxnFactory ZooKeeperServer]
           java.io.Closeable))

;; Based on:
;; https://github.com/pingles/clj-kafka/blob/master/test/clj_kafka/test/utils.clj
;; https://github.com/chbatey/kafka-unit/blob/master/src/main/java/info/batey/kafka/unit/KafkaUnit.java
;; https://github.com/apache/curator/blob/master/curator-test/src/main/java/org/apache/curator/test/TestingZooKeeperMain.java

(def ^:dynamic ^String *host* "localhost")
(def ^:dynamic ^String *broker-id* "0")

(def default-zookeeper-port 2182)
(def default-kafka-port 9092)

(def default-kafka-broker-config
  {"host" *host*
   "port" (str default-kafka-port)
   "broker.id" *broker-id*
   "num.io.threads" "2"
   "num.network.threads" "2"
   "log.cleaner.dedupe.buffer.size" "1048577"
   "offsets.topic.num.partitions" "1"
   "offsets.topic.replication.factor" "1"
   "transaction.state.log.num.partitions" "1"
   "transaction.state.log.replication.factor" "1"
   "transaction.state.log.min.isr" "1"
   "auto.create.topics.enable" "false"})

(defn start-kafka-broker ^KafkaServerStartable [config]
  (doto (KafkaServerStartable. (KafkaConfig. (merge default-kafka-broker-config config)))
    (.startup)))

(defn stop-kafka-broker [^KafkaServerStartable broker]
  (some-> broker .shutdown)
  (some-> broker .awaitShutdown))

(defn start-zookeeper
  (^org.apache.zookeeper.server.ServerCnxnFactory [data-dir]
   (start-zookeeper data-dir default-zookeeper-port))
  (^org.apache.zookeeper.server.ServerCnxnFactory [data-dir ^long port]
   (cio/delete-dir data-dir)
   (let [tick-time 2000
         max-connections 16
         server (ZooKeeperServer. (io/file data-dir) (io/file data-dir) tick-time)]
     (doto (ServerCnxnFactory/createFactory port max-connections)
       (.startup server)))))

(defn stop-zookeeper [^ServerCnxnFactory server-cnxn-factory]
  (when server-cnxn-factory
    (.shutdown server-cnxn-factory)
    (when-let [server ^ZooKeeperServer (.invoke (doto (.getDeclaredMethod ServerCnxnFactory
                                                                          "getZooKeeperServer"
                                                                          (make-array Class 0))
                                                  (.setAccessible true))
                                                server-cnxn-factory (object-array 0))]
      (.shutdown server)
      (some-> (.getZKDatabase server) (.close)))))

(defrecord EmbeddedKafka [zookeeper kafka options]
  Closeable
  (close [_]
    (stop-kafka-broker kafka)
    (stop-zookeeper zookeeper)))

(s/def ::zookeeper-data-dir string?)
(s/def ::zookeeper-port :crux.io/port)
(s/def ::kafka-log-dir string?)
(s/def ::kafka-port :crux.io/port)
(s/def ::broker-config (s/map-of string? string?))

(s/def ::options (s/keys :req [::zookeeper-data-dir
                               ::kafka-log-dir]
                         :opt [::zookeeper-port
                               ::kafka-port
                               ::broker-config]))

(defn start-embedded-kafka
  "Starts ZooKeeper and Kafka locally. This can be used to run Crux in a
  self-contained single node mode. The options zookeeper-data-dir and
  kafka-log-dir are required.

  Returns a crux.kafka.embedded_kafka.EmbeddedKafka component that
  implements java.io.Closeable, which allows ZooKeeper and Kafka to be
  stopped by calling close.

  NOTE: requires org.apache.kafka/kafka_2.11 and
  org.apache.zookeeper/zookeeper on the classpath."
  ^java.io.Closeable
  [{:crux.kafka.embedded/keys [zookeeper-data-dir zookeeper-port kafka-log-dir kafka-port broker-config]
    :or {zookeeper-port default-zookeeper-port
         kafka-port default-kafka-port}
    :as options}]
  (s/assert ::options options)
  (let [zookeeper (start-zookeeper (io/file zookeeper-data-dir) zookeeper-port)
        kafka (try
                (start-kafka-broker (merge broker-config
                                           {"log.dir" (str (io/file kafka-log-dir))
                                            "port" (str kafka-port)
                                            "zookeeper.connect" (str *host* ":" zookeeper-port)}))
                (catch Throwable t
                  (stop-zookeeper zookeeper)
                  (throw t)))]
    (->EmbeddedKafka zookeeper kafka (assoc options :bootstrap-servers (str *host* ":" kafka-port)))))
