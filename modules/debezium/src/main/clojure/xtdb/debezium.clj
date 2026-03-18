(ns xtdb.debezium
  (:require [clojure.tools.logging :as log]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (org.apache.arrow.memory RootAllocator)
           (xtdb.api.log KafkaCluster$ClusterFactory KafkaCluster$LogFactory)
           (xtdb.debezium DebeziumLog DebeziumProcessor)))

(defn start!
  "Starts a CDC ingestion node, returns an AutoCloseable."
  [node-opts {:keys [kafka-cluster source-topic debezium-topic]}]
  (let [cfg (xtn/->config node-opts)
        cluster (.open ^KafkaCluster$ClusterFactory (get (.getLogClusters cfg) kafka-cluster))
        kafka-config (.getKafkaConfigMap cluster)
        source-log (.openSourceLog (doto (KafkaCluster$LogFactory. kafka-cluster source-topic)
                                     (.groupId (str "xtdb-" source-topic "-debezium")))
                                   {kafka-cluster cluster})
        producer (.openAtomicProducer source-log (str "debezium-" source-topic))
        debezium-log (DebeziumLog. kafka-config debezium-topic)
        allocator (RootAllocator.)
        processor (DebeziumProcessor. producer allocator (.getDefaultTz cfg))
        subscription (.tailAll debezium-log -1 processor)]
    (log/info "Debezium CDC process started"
              {:kafka-cluster kafka-cluster
               :source-topic source-topic
               :debezium-topic debezium-topic})
    (reify java.lang.AutoCloseable
      (close [_]
        (log/info "Shutting down Debezium CDC process...")
        (util/close subscription)
        (util/close processor)
        (util/close allocator)
        (util/close debezium-log)
        (util/close producer)
        (util/close source-log)
        (util/close cluster)
        (log/info "Debezium CDC process shut down.")))))
