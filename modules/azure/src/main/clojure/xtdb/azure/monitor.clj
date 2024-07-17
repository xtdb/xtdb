(ns xtdb.azure.monitor
  (:require [xtdb.node :as xtn])
  (:import (xtdb.api Xtdb$Config)
           (xtdb.azure AzureMonitorMetrics$Factory)))

(defmethod xtn/apply-config! ::metrics [^Xtdb$Config config _ {:keys [instrumentation-key]
                                                               :or {instrumentation-key "xtdb.metrics"}}]
  (.setMetrics config (AzureMonitorMetrics$Factory. instrumentation-key)))
