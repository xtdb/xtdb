(ns crux.metrics.indexer
  (:require [crux.bus :as bus]
            [crux.api :as api]
            [crux.tx :as tx]
            [metrics.gauges :as gauges]
            [metrics.meters :as meters]
            [metrics.timers :as timers]))

(defn assign-tx-id-lag [registry {:crux.node/keys [node]}]
  (assert node)
  (gauges/gauge-fn registry
                   ["crux" "indexer" "tx-id-lag"]
                   #(let [completed (api/latest-completed-tx node)]
                      (if completed
                        (- (::tx/tx-id (api/latest-submitted-tx node))
                           (::tx/tx-id (api/latest-completed-tx node)))
                        0))))

(defn assign-doc-meter [registry {:crux.node/keys [bus]}]
  (let [meter (meters/meter registry ["crux" "indexer" "indexed-docs"])]
    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [doc-ids]}]
                  (meters/mark! meter (count doc-ids))))

    meter))

(defn assign-tx-timer [registry {:crux.node/keys [bus]}]
  (let [timer (timers/timer registry ["crux" "indexer" "indexed-txs"])
        !timer (atom nil)]
    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexing-tx}}
                (fn [_]
                  (reset! !timer (timers/start timer))))

    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexed-tx}}
                (fn [_]
                  (let [[ctx _] (reset-vals! !timer nil)]
                    (timers/stop ctx))))

    timer))

(defn assign-listeners
  "Assigns listeners to an event bus for a given node.
  Returns an atom containing uptading metrics"
  [registry deps]
  {:tx-id-lag (assign-tx-id-lag registry deps)
   :docs-ingest-meter (assign-doc-meter registry deps)
   :tx-ingest-timer (assign-tx-timer registry deps)})
