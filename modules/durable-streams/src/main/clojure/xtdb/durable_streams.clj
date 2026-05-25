(ns xtdb.durable-streams
  (:require [xtdb.log :as log]
            [xtdb.time :as time])
  (:import [xtdb.api.log DurableStreamsLog$ClusterFactory DurableStreamsLog$LogFactory]))

(defmethod log/->log-cluster-factory ::cluster
  [_ {:keys [base-url connect-timeout long-poll-timeout]}]
  (cond-> (DurableStreamsLog$ClusterFactory. base-url)
    connect-timeout    (.connectTimeout (time/->duration connect-timeout))
    long-poll-timeout  (.longPollTimeout (time/->duration long-poll-timeout))))

(defmethod log/->log-factory ::durable-streams
  [_ {:keys [cluster topic replica-topic epoch tenant create-stream?] :as opts}]
  (let [cluster-str (str (symbol cluster))]
    (cond-> (DurableStreamsLog$LogFactory. cluster-str topic)
      replica-topic (.replicaTopic replica-topic)
      epoch         (.epoch epoch)
      tenant        (.tenant tenant)
      (contains? opts :create-stream?) (.createStream (boolean create-stream?)))))
