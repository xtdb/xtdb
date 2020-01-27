(ns crux.metrics.kv-store
  (:require [crux.status :as status]
            [metrics.gauges :as gauges]))

(defn estimate-num-keys
  [registry {:crux.node/keys [kv-store]}]
  (gauges/gauge-fn registry ["crux" "kv" "estimate-num-keys"]
                   #(:crux.kv/estimate-num-keys (status/status-map kv-store))))

(defn kv-size-mb
  [registry {:crux.node/keys [kv-store]}]
  (gauges/gauge-fn registry ["crux" "kv" "kv-size-mb"]
                   #(:crux.kv/size (status/status-map kv-store))))
  

(defn assign-listeners
  [registry deps]

  {:estimate-num-keys (estimate-num-keys registry deps)
   :kv-size-mb (kv-size-mb registry deps)})
