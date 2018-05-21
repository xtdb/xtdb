(ns crux.embedded-kafka
  (:require [clojure.java.io :as io]
            [crux.io :as cio]
            [crux.kafka :as k])
  (:import [kafka.server
            KafkaConfig KafkaServerStartable]
           [org.apache.kafka.clients.admin
            AdminClient]
           [org.apache.kafka.clients.producer
            KafkaProducer]
           [org.apache.kafka.clients.consumer
            KafkaConsumer]
           [org.apache.zookeeper.server
             ServerCnxnFactory NIOServerCnxnFactory ZooKeeperServer]
           [java.net InetSocketAddress]
           [java.util Properties]))

;; Based on:
;; https://github.com/pingles/clj-kafka/blob/master/test/clj_kafka/test/utils.clj
;; https://github.com/chbatey/kafka-unit/blob/master/src/main/java/info/batey/kafka/unit/KafkaUnit.java

(def ^:dynamic ^String *host* "localhost")
(def ^:dynamic ^String *broker-id* "0")

(def default-zookeeper-port 2182)
(def default-kafka-port 9092)

(def ^:dynamic *zookeeper-connect* (str *host* ":" default-zookeeper-port))
(def ^:dynamic *kafka-bootstrap-servers* (str *host* ":" default-kafka-port))

(def default-kafka-broker-config
  {"host" *host*
   "port" (str default-kafka-port)
   "broker.id" *broker-id*
   "zookeeper.connect" *zookeeper-connect*
   "offsets.topic.replication.factor" "1"
   "transaction.state.log.replication.factor" "1"
   "transaction.state.log.min.isr" "1"
   "auto.create.topics.enable" "false"})

(defn write-meta-properties [log-dir broker-id]
  (let [meta-properties (io/file log-dir "meta.properties")]
    (when-not (.exists meta-properties)
      (io/make-parents meta-properties)
      (with-open [out (io/output-stream meta-properties)]
        (doto (Properties.)
          (.setProperty "version" "0")
          (.setProperty "broker.id" (str broker-id))
          (.store out ""))))))

(defn start-kafka-broker ^KafkaServerStartable [config]
  (doto (KafkaServerStartable. (KafkaConfig. (merge default-kafka-broker-config config)))
    (.startup)))

(defn with-embedded-kafka [f]
  (let [log-dir (doto (cio/create-tmpdir "kafka-log")
                  (write-meta-properties *broker-id*))
        port (cio/free-port)
        broker (start-kafka-broker {"host.name" *host*
                                    "port" (str port)
                                    "broker.id" *broker-id*
                                    "zookeeper.connect" *zookeeper-connect*
                                    "log.dir" (.getAbsolutePath log-dir)})]
    (try
      (binding [*kafka-bootstrap-servers* (str *host* ":" port)]
        (f))
      (finally
        (some-> broker .shutdown)
        (some-> broker .awaitShutdown)
        (cio/delete-dir log-dir)))))

(defn start-zookeeper ^ServerCnxnFactory
  ([data-dir]
   (start-zookeeper data-dir *host* default-zookeeper-port))
  ([data-dir ^String host ^long port]
   (let [tick-time 500
         max-connections 16
         server (ZooKeeperServer. (io/file data-dir) (io/file data-dir) tick-time)]
     (doto (NIOServerCnxnFactory/createFactory)
       (.configure (InetSocketAddress. host port)
                   max-connections)
       (.startup server)))))

(defn with-embedded-zookeeper [f]
  (let [data-dir (cio/create-tmpdir "zookeeper")
        port (cio/free-port)
        server-cnxn-factory (start-zookeeper data-dir *host* port)]
    (try
      (binding [*zookeeper-connect* (str  *host* ":" port)]
        (f))
      (finally
        (some-> ^ServerCnxnFactory server-cnxn-factory .shutdown)
        (cio/delete-dir data-dir)))))

(defn with-embedded-kafka-cluster [f]
  (with-embedded-zookeeper
    (partial with-embedded-kafka f)))

(def ^:dynamic ^AdminClient *admin-client*)
(def ^:dynamic ^KafkaProducer *producer*)
(def ^:dynamic ^KafkaConsumer *consumer*)

(defn with-kafka-client [f]
  (with-open [admin-client (k/create-admin-client {"bootstrap.servers"*kafka-bootstrap-servers*})
              producer (k/create-producer {"bootstrap.servers" *kafka-bootstrap-servers*})
              consumer (k/create-consumer {"bootstrap.servers" *kafka-bootstrap-servers*
                                           "group.id" "0"})]
    (binding [*admin-client* admin-client
              *producer* producer
              *consumer* consumer]
      (f))))
