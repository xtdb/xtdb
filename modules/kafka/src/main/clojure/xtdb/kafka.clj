(ns xtdb.kafka
  (:require [xtdb.util :as util]
            [xtdb.log :as log])
  (:import [xtdb.api.log KafkaCluster$ClusterFactory KafkaCluster$LogFactory]))

(defmethod log/->log-cluster-factory ::cluster
  [_ {:keys [bootstrap-servers properties-map properties-file group-id]}]
  (cond-> (KafkaCluster$ClusterFactory. bootstrap-servers)
    properties-map (.propertiesMap properties-map)
    properties-file (.propertiesFile (util/->path properties-file))
    group-id (.groupId group-id)))

(defmethod log/->log-factory ::kafka [_ {:keys [cluster topic replica-cluster replica-topic epoch] :as opts}]
  (let [cluster-str (str (symbol cluster))]
    (cond-> (KafkaCluster$LogFactory. cluster-str topic
                                      (or replica-cluster cluster-str)
                                      (or replica-topic (str topic "-replica"))
                                      (boolean (:create-topic? opts true)))
      epoch (.epoch epoch))))
