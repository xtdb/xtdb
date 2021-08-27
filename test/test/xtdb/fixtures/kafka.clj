(ns xtdb.fixtures.kafka
  (:require [clojure.java.io :as io]
            [xtdb.fixtures :as fix]
            [xtdb.io :as cio]
            [xtdb.kafka :as k]
            [xtdb.kafka.embedded :as ek])
  (:import [java.util Properties UUID]
           org.apache.kafka.clients.consumer.KafkaConsumer))

(def ^:dynamic *kafka-bootstrap-servers*)
(def ^:dynamic ^String *tx-topic*)
(def ^:dynamic ^String *doc-topic*)

(defn- write-kafka-meta-properties [log-dir broker-id]
  (let [meta-properties (io/file log-dir "meta.properties")]
    (when-not (.exists meta-properties)
      (io/make-parents meta-properties)
      (with-open [out (io/output-stream meta-properties)]
        (doto (Properties.)
          (.setProperty "version" "0")
          (.setProperty "broker.id" (str broker-id))
          (.store out ""))))))

(defn with-embedded-kafka-cluster [f]
  (fix/with-tmp-dir "zk" [zk-data-dir]
    (fix/with-tmp-dir "kafka-log" [kafka-log-dir]
      (write-kafka-meta-properties kafka-log-dir ek/*broker-id*)

      (let [zookeeper-port (cio/free-port)
            kafka-port (cio/free-port)]
        (with-open [embedded-kafka (ek/start-embedded-kafka
                                    #::ek{:zookeeper-data-dir (str zk-data-dir)
                                          :zookeeper-port zookeeper-port
                                          :kafka-log-dir (str kafka-log-dir)
                                          :kafka-port kafka-port})]
          (binding [*kafka-bootstrap-servers* (get-in embedded-kafka [:options :bootstrap-servers])]
            (f)))))))

(def ^:dynamic *consumer-options* {})

(defn ^KafkaConsumer open-consumer []
  (k/->consumer {:kafka-config (k/->kafka-config {:bootstrap-servers *kafka-bootstrap-servers*
                                                  :properties-map (merge {"group.id" (str (UUID/randomUUID))}
                                                                         *consumer-options*)})}))

(def ^:dynamic *kafka-config* {})

(defn with-kafka-config
  ([opts] (fn [f] (with-kafka-config opts f)))
  ([opts f]
   (binding [*kafka-config* (merge *kafka-config* opts)]
     (f))))

(defn with-cluster-tx-log-opts [f]
  (let [test-id (UUID/randomUUID)]
    (binding [*tx-topic* (str "tx-topic-" test-id)]
      (fix/with-opts {::k/kafka-config (merge
                                        {:bootstrap-servers *kafka-bootstrap-servers*}
                                        *kafka-config*)
                      ::tx-topic-opts {:xt/module `k/->topic-opts, :topic-name *tx-topic*}
                      :xt/tx-log {:xt/module `k/->tx-log, :kafka-config ::k/kafka-config, :tx-topic-opts ::tx-topic-opts}}
        f))))

(defn with-cluster-doc-store-opts [f]
  (assert (bound? #'*kafka-bootstrap-servers*))
  (let [test-id (UUID/randomUUID)]
    (binding [*doc-topic* (str "doc-topic-" test-id)]
      (fix/with-opts {::k/kafka-config (merge
                                        {:bootstrap-servers *kafka-bootstrap-servers*}
                                        *kafka-config*)
                      ::doc-topic-opts {:xt/module `k/->topic-opts, :topic-name *doc-topic*}
                      :xt/document-store {:xt/module `k/->document-store, :kafka-config ::k/kafka-config, :doc-topic-opts ::doc-topic-opts}}
        f))))
