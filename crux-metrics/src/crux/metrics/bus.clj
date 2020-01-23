(ns crux.metrics.bus
  (:require [crux.api :as api]
            [crux.bus :as bus]) )

;; I might be storing too much metadata. Maybe timings don't need to be stored
(defn assign-ingest
  "Assigns listeners to an event bus for a given node.
  Returns an atom containing uptading metrics"
  [node]

  (let [!metrics (atom {:crux.metrics/indexing-tx {}
                        :crux.metrics/indexed-tx {}
                        :crux.metrics/indexing-docs {}
                        :crux.metrics/indexed-docs {}
                        :crux.metrics/latest-latency-docs -1
                        :crux.metrics/latest-latency-tx -1
                        :crux.metrics/latest-tx-id []})]
    (bus/listen (:bus node)
                {:crux.bus/event-types #{:crux.tx/indexing-docs}}
                (fn [{:keys [doc-ids]}]
                  (swap! !metrics assoc-in [:crux.metrics/indexing-docs doc-ids]
                         {:start-time-ms (System/currentTimeMillis)})))
    (bus/listen (:bus node)
                {:crux.bus/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [doc-ids]}]
                  (let [meta-doc (get (:crux.metrics/indexing-docs @!metrics) doc-ids)
                        start-time-ms (:start-time-ms meta-doc)
                        end-time-ms (System/currentTimeMillis)]
                    (swap! !metrics update :crux.metrics/indexing-docs dissoc doc-ids)
                    (swap! !metrics update :crux.metrics/indexed-docs assoc doc-ids
                           {:start-time-ms start-time-ms
                            :time-elapsed-ms (- end-time-ms start-time-ms)})
                    (swap! !metrics assoc :crux.metrics/latest-latency-docs (- end-time-ms start-time-ms)))))

    (bus/listen (:bus node)
                {:crux.bus/event-types #{:crux.tx/indexing-tx}}
                (fn [event]
                  (swap! !metrics assoc :crux.metrics/latest-tx-id
                         [(:crux.tx/tx-id (:crux.tx/submitted-tx event))
                          (:crux.tx/tx-id (:crux.tx/latest-completed-tx (api/status node)))])
                  (swap! !metrics assoc-in [:crux.metrics/indexing-tx (:crux.tx/submitted-tx event)]
                         {:start-time-ms (System/currentTimeMillis)})))

    (bus/listen (:bus node)
                {:crux.bus/event-types #{:crux.tx/indexed-tx}}
                (fn [event]
                  (let [meta-doc (get (:crux.metrics/indexing-tx @!metrics) (:crux.tx/submitted-tx event))
                        start-time-ms (:start-time-ms meta-doc)
                        end-time-ms (System/currentTimeMillis)]
                    (swap! !metrics update :crux.metrics/indexing-tx dissoc (:crux.tx/submitted-tx event))
                    (swap! !metrics update :crux.metrics/indexed-tx assoc (:crux.tx/submitted-tx event)
                           {:start-time-ms start-time-ms
                            :time-elapsed-ms (- end-time-ms start-time-ms)})
                    (swap! !metrics update :crux.metrics/latest-latency-tx (- end-time-ms start-time-ms)))))
    !metrics))
