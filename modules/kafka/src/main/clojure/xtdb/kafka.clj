(ns xtdb.kafka
  (:require [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.node :as xtn]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import [xtdb.api Xtdb$Config]
           [xtdb.api.log KafkaCluster$ClusterFactory KafkaCluster$LogFactory]))

(defmethod xtn/apply-config! ::cluster
  [^Xtdb$Config config _ [cluster-alias {:keys [bootstrap-servers poll-duration properties-map properties-file]}]]
  (doto config
    (.logCluster (str (symbol cluster-alias))
                 (cond-> (KafkaCluster$ClusterFactory. bootstrap-servers)
                   poll-duration (.pollDuration (time/->duration poll-duration))
                   properties-map (.propertiesMap properties-map)
                   properties-file (.propertiesFile (util/->path properties-file))))))

(defmethod db/->log-factory :xtdb/kafka
  [_ {:keys [cluster topic epoch] :as opts}]
  (cond-> (KafkaCluster$LogFactory. (str (symbol cluster)) topic (boolean (:create-topic? opts true)))
    epoch (.epoch epoch)))
