(ns xtdb.aws.cloudwatch
  (:require [xtdb.node :as xtn])
  (:import (software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient)
           (xtdb.api Xtdb$Config)
           (xtdb.aws CloudWatchMetrics)))

(defmethod xtn/apply-config! ::metrics [^Xtdb$Config config _ {:keys [namespace client]
                                                               :or {namespace "xtdb.metrics"
                                                                    client (CloudWatchAsyncClient/create)}}]
  (.module config (CloudWatchMetrics. namespace client)))
