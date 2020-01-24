(ns crux.metrics.ingest
  (:require [crux.bus :as bus]
            [crux.db :as db]
            [metrics.gauges :as gauges]
            [metrics.meters :as meters]
            [crux.tx :as tx]))

(defn assign-tx-id-lag [registry {:crux.node/keys [bus indexer tx-log]}]
  (gauges/gauge-fn registry
                   ["crux" "ingest" "tx-id-lag"]
                   #(when-let [latest-tx-id (db/read-index-meta indexer :crux.tx/latest-completed-tx)]
                      (- (::tx/tx-id (db/latest-submitted-tx tx-log))
                         (::tx/tx-id latest-tx-id)))))

(defn assign-doc-meter [registry {:crux.node/keys [bus indexer tx-log]}]
  (let [meter (meters/meter registry ["crux" "ingest" "indexed-docs"])]
    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [doc-ids]}]
                  (meters/mark! meter (count doc-ids))))

    meter))

(defn assign-tx-meter [registry {:crux.node/keys [bus indexer tx-log]}]
  (let [meter (meters/meter registry ["crux" "ingest" "indexed-txs"])]
    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexed-tx}}
                (fn [_]
                  (meters/mark! meter)))

    meter))

(defn assign-ingest
  "Assigns listeners to an event bus for a given node.
  Returns an atom containing uptading metrics"
  [registry {:crux.node/keys [bus indexer tx-log] :as deps}]

  {:tx-id-lag (assign-tx-id-lag registry deps)
   :docs-ingest-meter (assign-doc-meter registry deps)
   :tx-ingest-meter (assign-tx-meter registry deps)})
