(ns xtdb.azure.monitor
  (:require [xtdb.node :as xtn])
  (:import (xtdb.api Xtdb$Config)
           (xtdb.azure AzureMonitorMetrics)))

(defmethod xtn/apply-config! ::metrics [^Xtdb$Config config _ {:keys [instrumentation-key]
                                                               :or {instrumentation-key "xtdb.metrics"}}]
  (.module config (AzureMonitorMetrics. instrumentation-key)))
