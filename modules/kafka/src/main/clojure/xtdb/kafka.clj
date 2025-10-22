(ns xtdb.kafka
  (:require [xtdb.time :as time]
            [xtdb.util :as util]
            [xtdb.log :as log])
  (:import [xtdb.api.log KafkaCluster$ClusterFactory KafkaCluster$LogFactory]))

(defmethod log/->log-cluster-factory ::cluster
  [_ {:keys [bootstrap-servers poll-duration properties-map properties-file]}]
  (cond-> (KafkaCluster$ClusterFactory. bootstrap-servers)
    poll-duration (.pollDuration (time/->duration poll-duration))
    properties-map (.propertiesMap properties-map)
    properties-file (.propertiesFile (util/->path properties-file))))

(defmethod log/->log-factory ::kafka [_ {:keys [cluster topic epoch] :as opts}]
  (cond-> (KafkaCluster$LogFactory. (str (symbol cluster)) topic (boolean (:create-topic? opts true)))
    epoch (.epoch epoch)))
