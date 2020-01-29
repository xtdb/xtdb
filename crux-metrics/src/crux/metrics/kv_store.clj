(ns crux.metrics.kv-store
  (:require [crux.status :as status]
            [crux.dropwizard :as dropwizard]))

(defn assign-estimate-num-keys-gauge
  [registry {:crux.node/keys [kv-store]}]
  (dropwizard/gauge-fn registry ["crux" "kv" "assign-estimate-num-keys-gauge"]
                   #(:crux.kv/estimate-num-keys (status/status-map kv-store))))

(defn assign-kv-size-mb-gauge
  [registry {:crux.node/keys [kv-store]}]
  (dropwizard/gauge-fn registry ["crux" "kv" "kv-size-mb"]
                   #(:crux.kv/size (status/status-map kv-store))))
  

(defn assign-listeners
  [registry deps]

  {:estimate-num-keys (assign-estimate-num-keys-gauge registry deps)
   :kv-size-mb (assign-kv-size-mb-gauge registry deps)})
