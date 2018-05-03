(ns crux.embedded-kafka
  (:require [crux.test-utils :as tu])
  (:import [kafka.server
            KafkaConfig KafkaServerStartable]
           [org.apache.zookeeper.server
            NIOServerCnxnFactory ZooKeeperServer]
           [java.net InetSocketAddress]))

;; Based on:
;; https://github.com/pingles/clj-kafka/blob/master/test/clj_kafka/test/utils.clj
;; https://github.com/chbatey/kafka-unit/blob/master/src/main/java/info/batey/kafka/unit/KafkaUnit.java

(def ^:dynamic ^String *host* "localhost")

(def ^:dynamic *zookeeper-connect* (str *host* ":" 2181))
(def ^:dynamic *kafka-bootstrap-servers* (str *host* ":" 9092))

(def ^:dynamic *kafka-broker-config*
  {"broker.id" "0"
   "offsets.topic.replication.factor" "1"
   "auto.create.topics.enable" "true"})

(defn with-embedded-kafka [f]
  (let [log-dir (tu/create-tmpdir "kafka-log")
        port (tu/free-port)
        config (KafkaConfig.
                (merge *kafka-broker-config*
                       {"host.name" *host*
                        "port" (str port)
                        "zookeeper.connect" *zookeeper-connect*
                        "log.dir" (.getAbsolutePath log-dir)}))
        broker (doto (KafkaServerStartable. config)
                 (.startup))]
    (try
      (binding [*kafka-bootstrap-servers* (str *host* ":" port)]
        (f))
      (finally
        (some-> broker .shutdown)
        (some-> broker .awaitShutdown)
        (tu/delete-dir log-dir)))))

(defn with-embedded-zookeeper [f]
  (let [tick-time 500
        max-connections 16
        snapshot-dir (tu/create-tmpdir "zk-snapshot")
        log-dir (tu/create-tmpdir "zk-log")
        port (tu/free-port)
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
          (tu/delete-dir dir))))))

(defn with-embedded-kafka-cluster [f]
  (with-embedded-zookeeper
    (partial with-embedded-kafka f)))
