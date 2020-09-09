(ns ^:no-doc crux.metrics.indexer
  (:require [crux.bus :as bus]
            [crux.api :as api]
            [crux.tx :as tx]
            [crux.metrics.dropwizard :as dropwizard])
  (:import (java.util Date)))

(defn assign-tx-id-lag [registry {:crux/keys [node]}]
  (dropwizard/gauge registry
                    ["indexer" "tx-id-lag"]
                    #(when-let [completed (api/latest-completed-tx node)]
                       (- (::tx/tx-id (api/latest-submitted-tx node))
                          (::tx/tx-id completed)))))

(defn assign-tx-latency-gauge [registry {:crux/keys [bus]}]
  (let [!last-tx-lag (atom 0)]
    (bus/listen bus
                {:crux/event-types #{:crux.tx/indexed-tx}}
                (fn [{::tx/keys [submitted-tx]}]
                  (reset! !last-tx-lag (- (System/currentTimeMillis)
                                          (.getTime ^Date (::tx/tx-time submitted-tx))))))
    (dropwizard/gauge registry
                      ["indexer" "tx-latency"]
                      (fn []
                        (first (reset-vals! !last-tx-lag 0))))))

(defn assign-doc-meter [registry {:crux/keys [bus]}]
  (let [meter (dropwizard/meter registry ["indexer" "indexed-docs"])]
    (bus/listen bus
                {:crux/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [doc-ids]}]
                  (dropwizard/mark! meter (count doc-ids))))

    meter))

(defn assign-av-meter [registry {:crux/keys [bus]}]
  (let [meter (dropwizard/meter registry ["indexer" "indexed-avs"])]
    (bus/listen bus
                {:crux/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [av-count]}]
                  (dropwizard/mark! meter av-count)))
    meter))

(defn assign-bytes-meter [registry {:crux/keys [bus]}]
  (let [meter (dropwizard/meter registry ["indexer" "indexed-bytes"])]
    (bus/listen bus
                {:crux/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [bytes-indexed]}]
                  (dropwizard/mark! meter bytes-indexed)))

    meter))

(defn assign-tx-timer [registry {:crux/keys [bus]}]
  (let [timer (dropwizard/timer registry ["indexer" "indexed-txs"])
        !timer (atom nil)]
    (bus/listen bus
                {:crux/event-types #{:crux.tx/indexing-tx}}
                (fn [_]
                  (reset! !timer (dropwizard/start timer))))

    (bus/listen bus
                {:crux/event-types #{:crux.tx/indexed-tx}}
                (fn [_]
                  (let [[ctx _] (reset-vals! !timer nil)]
                    (dropwizard/stop ctx))))

    timer))

(defn assign-listeners
  "Assigns listeners to an event bus for a given node.
  Returns an atom containing updating metrics"
  [registry deps]
  {:tx-id-lag (assign-tx-id-lag registry deps)
   :tx-latency-gauge (assign-tx-latency-gauge registry deps)
   :docs-ingested-meter (assign-doc-meter registry deps)
   :av-ingested-meter (assign-av-meter registry deps)
   :bytes-ingested-meter (assign-bytes-meter registry deps)
   :tx-ingest-timer (assign-tx-timer registry deps)})
