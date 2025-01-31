(ns xtdb.kafka
  (:require [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import [xtdb.api Xtdb$Config]
           [xtdb.api.log KafkaLog]))

(defmethod xtn/apply-config! ::log
  [^Xtdb$Config config _ {:keys [bootstrap-servers
                                 topic create-topic?
                                 poll-duration
                                 properties-map properties-file]}]
  (doto config
    (.setLog (cond-> (KafkaLog/kafka bootstrap-servers topic)
               create-topic? (.autoCreateTopic create-topic?)
               poll-duration (.pollDuration (time/->duration poll-duration))
               properties-map (.propertiesMap properties-map)
               properties-file (.propertiesFile (util/->path properties-file))))))
