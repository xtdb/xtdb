(ns xtdb.debezium
  (:require [clojure.tools.logging :as log]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (xtdb.api.log KafkaCluster Log)
           (xtdb.debezium DebeziumLog DebeziumProcessor)))

(defn- resolve-kafka-config
  "Extracts the Kafka config map from a named cluster in the node's Integrant system."
  [node cluster-alias]
  (let [clusters (util/component node :xtdb.log/clusters)
        ^KafkaCluster cluster (or (get clusters cluster-alias)
                                  (throw (err/incorrect ::unknown-kafka-cluster
                                                        (format "Unknown Kafka cluster alias: '%s'" cluster-alias)
                                                        {:cluster-alias cluster-alias
                                                         :available (vec (keys clusters))})))]
    (into {} (.getKafkaConfigMap cluster))))

(defn start!
  "Starts a CDC ingestion node. Returns an AutoCloseable that tears down
   the subscription, processor, log and node on close."
  [node-opts {:keys [kafka-cluster topic db-name]}]
  (let [node (let [config (doto (xtn/->config node-opts)
                            (-> (.getCompactor) (.threads 0))
                            (.setServer nil)
                            (.setFlightSql nil))]
               (.open config))
        kafka-config (resolve-kafka-config node kafka-cluster)
        log (DebeziumLog. kafka-config topic)
        processor (DebeziumProcessor. node db-name (.allocator node))
        subscription (Log/tailAll log -1 processor)]
    (log/info "Debezium CDC node started"
              {:kafka-cluster kafka-cluster
               :topic topic :db-name db-name})
    (reify java.lang.AutoCloseable
      (close [_]
        (log/info "Shutting down Debezium CDC node...")
        (util/close subscription)
        (util/close processor)
        (util/close log)
        (util/close node)
        (log/info "Debezium CDC node shut down.")))))
