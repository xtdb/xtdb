(ns crux.metrics.indexer
  (:require [crux.bus :as bus]
            [crux.api :as api]
            [crux.tx :as tx]
            [crux.metrics.dropwizard :as dropwizard]))

(defn assign-tx-id-lag [registry {:crux.node/keys [node]}]
  (dropwizard/gauge registry
                    ["indexer" "tx-id-lag"]
                    #(when-let [completed (api/latest-completed-tx node)]
                       (- (::tx/tx-id (api/latest-submitted-tx node))
                          (::tx/tx-id completed)))))

(defn assign-doc-meter [registry {:crux.node/keys [bus]}]
  (let [meter (dropwizard/meter registry ["indexer" "indexed-docs"])]
    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [doc-ids]}]
                  (dropwizard/mark! meter (count doc-ids))))

    meter))

(defn assign-av-meter [registry {:crux.node/keys [bus]}]
  (let [meter (dropwizard/meter registry ["indexer" "indexed-avs"])]
    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [av-count]}]
                  (dropwizard/mark! meter av-count)))
    meter))

(defn assign-bytes-meter [registry {:crux.node/keys [bus]}]
  (let [meter (dropwizard/meter registry ["indexer" "indexed-bytes"])]
    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [bytes-indexed]}]
                  (dropwizard/mark! meter bytes-indexed)))

    meter))

(defn assign-tx-timer [registry {:crux.node/keys [bus]}]
  (let [timer (dropwizard/timer registry ["indexer" "indexed-txs"])
        !timer (atom nil)]
    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexing-tx}}
                (fn [_]
                  (reset! !timer (dropwizard/start timer))))

    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexed-tx}}
                (fn [_]
                  (let [[ctx _] (reset-vals! !timer nil)]
                    (dropwizard/stop ctx))))

    timer))

(defn assign-listeners
  "Assigns listeners to an event bus for a given node.
  Returns an atom containing updating metrics"
  [registry deps]
  {:tx-id-lag (assign-tx-id-lag registry deps)
   :docs-ingested-meter (assign-doc-meter registry deps)
   :av-ingested-meter (assign-av-meter registry deps)
   :bytes-ingested-meter (assign-bytes-meter registry deps)
   :tx-ingested-timer (assign-tx-timer registry deps)})
