(ns xtdb.tracer
  (:require [xtdb.node :as xtn])
  (:import (xtdb.api Xtdb$Config)
           (xtdb.api.metrics TracerConfig)))

(defmethod xtn/apply-config! :xtdb/tracer [^Xtdb$Config config _ {:keys [enabled? endpoint service-name query-tracing? transaction-tracing? span-processor]}]
  (.tracer config
           (cond-> (TracerConfig.)
             (some? enabled?) (.enabled enabled?)
             endpoint (.endpoint endpoint)
             service-name (.serviceName service-name)
             (some? query-tracing?) (.queryTracing query-tracing?)
             (some? transaction-tracing?) (.transactionTracing transaction-tracing?)
             span-processor (.spanProcessor span-processor))))
