(ns xtdb.kafka
  (:require [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import [xtdb.api Xtdb$Config]
           [xtdb.api.log KafkaLog]))

(defmethod xtn/apply-config! ::log
  [^Xtdb$Config config _ {:keys [bootstrap-servers
                                 tx-topic files-topic create-topics?
                                 poll-duration
                                 properties-map properties-file]}]
  (doto config
    (.setLog (cond-> (KafkaLog/kafka bootstrap-servers tx-topic files-topic)
               create-topics? (.autoCreateTopics create-topics?)
               poll-duration (.pollDuration (time/->duration poll-duration))
               properties-map (.propertiesMap properties-map)
               properties-file (.propertiesFile (util/->path properties-file))))))
