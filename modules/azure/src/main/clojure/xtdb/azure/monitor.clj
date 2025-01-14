(ns xtdb.azure.monitor
  (:require [xtdb.node :as xtn])
  (:import (xtdb.api Xtdb$Config)
           (xtdb.azure AzureMonitorMetrics)))

(defmethod xtn/apply-config! ::metrics [^Xtdb$Config config _ {:keys [connection-string]}]
  (.module config (AzureMonitorMetrics. connection-string)))
