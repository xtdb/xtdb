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
            NIOServerCnxnFactory ZooKeeperServer]
           [java.net InetSocketAddress]
           [java.util Properties]))

;; Based on:
;; https://github.com/pingles/clj-kafka/blob/master/test/clj_kafka/test/utils.clj
;; https://github.com/chbatey/kafka-unit/blob/master/src/main/java/info/batey/kafka/unit/KafkaUnit.java

(def ^:dynamic ^String *host* "localhost")
(def ^:dynamic ^String *broker-id* "0")

(def ^:dynamic *zookeeper-connect* (str *host* ":" 2181))
(def ^:dynamic *kafka-bootstrap-servers* (str *host* ":" 9092))

(def ^:dynamic *kafka-broker-config*
  {"offsets.topic.replication.factor" "1"
   "transaction.state.log.replication.factor" "1"
   "transaction.state.log.min.isr" "1"})

(defn write-meta-properties [log-dir broker-id]
  (with-open [out (io/output-stream (io/file log-dir "meta.properties"))]
    (doto (Properties.)
      (.setProperty "version" "0")
      (.setProperty "broker.id" (str broker-id))
      (.store out ""))))

(defn with-embedded-kafka [f]
  (let [log-dir (cio/create-tmpdir "kafka-log")
        port (cio/free-port)
        config (KafkaConfig.
                (merge *kafka-broker-config*
                       {"host.name" *host*
                        "port" (str port)
                        "broker.id" *broker-id*
                        "zookeeper.connect" *zookeeper-connect*
                        "log.dir" (.getAbsolutePath log-dir)}))
        broker (KafkaServerStartable. config)]
    (write-meta-properties log-dir *broker-id*)
    (.startup broker)
    (try
      (binding [*kafka-bootstrap-servers* (str *host* ":" port)]
        (f))
      (finally
        (some-> broker .shutdown)
        (some-> broker .awaitShutdown)
        (cio/delete-dir log-dir)))))

(defn with-embedded-zookeeper [f]
  (let [tick-time 500
        max-connections 16
        snapshot-dir (cio/create-tmpdir "zk-snapshot")
        log-dir (cio/create-tmpdir "zk-log")
        port (cio/free-port)
        server (ZooKeeperServer. snapshot-dir log-dir tick-time)
        server-cnxn-factory (doto (NIOServerCnxnFactory/createFactory)
                              (.configure (InetSocketAddress. *host* port)
                                          max-connections)
                              (.startup server))]
    (try
      (binding [*zookeeper-connect* (str  *host* ":" port)]
        (f))
      (finally
        (some-> server-cnxn-factory .shutdown)
        (doseq [dir [snapshot-dir log-dir]]
          (cio/delete-dir dir))))))

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
