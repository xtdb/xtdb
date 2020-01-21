(ns crux.metrics.bus
  (:require [crux.api :as api]
            [crux.bus :as bus]) )

;; I might be storing too much metadata. Maybe timings don't need to be stored
(defn assign-ingest
  "Assigns listeners to an event bus for a given node.
  Returns an atom containing uptading metrics"
  [node]

  (let [!metrics (atom {:crux.metrics/indexing-tx 0
                        :crux.metrics/indexed-tx 0
                        :crux.metrics/indexing-docs 0
                        :crux.metrics/indexed-docs 0
                        :crux.metrics/latest-tx-id []})]
    (bus/listen (:bus node)
                {:crux.bus/event-types #{:crux.tx/indexing-docs}}
                (fn [{:keys [doc-ids]}]
                  (swap! !metrics update :crux.metrics/indexing-docs inc)))
    (bus/listen (:bus node)
                {:crux.bus/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [doc-ids]}]
                  (swap! !metrics update :crux.metrics/indexing-docs dec)
                  (swap! !metrics update :crux.metrics/indexed-docs inc)))

    (bus/listen (:bus node)
                {:crux.bus/event-types #{:crux.tx/indexing-tx}}
                (fn [event]
                  (swap! !metrics assoc :crux.metrics/latest-tx-id
                         [(:crux.tx/tx-id (:crux.tx/submitted-tx event))
                          (:crux.tx/tx-id (:crux.tx/latest-completed-tx (api/status node)))])
                  (swap! !metrics update :crux.metrics/indexing-tx inc)))

    (bus/listen (:bus node)
                {:crux.bus/event-types #{:crux.tx/indexed-tx}}
                (fn [event]
                  (let [meta-doc (get (:crux.metrics/indexing-tx @!metrics) (:crux.tx/submitted-tx event))
                        start-time-ms (:start-time-ms meta-doc)
                        end-time-ms (System/currentTimeMillis)]
                    (swap! !metrics update :crux.metrics/indexing-tx dec)
                    (swap! !metrics update :crux.metrics/indexed-tx inc))))
    !metrics))
