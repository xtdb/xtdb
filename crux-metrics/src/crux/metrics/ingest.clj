(ns crux.metrics.ingest
  (:require [crux.bus :as bus]
            [crux.db :as db]
            [metrics.timers :as timers]
            [metrics.gauges :as gauges]
            [crux.tx :as tx]))

(defn assign-ingest
  "Assigns listeners to an event bus for a given node.
  Returns an atom containing uptading metrics"
  [registry {:crux.node/keys [bus indexer tx-log]}]

  ;; Create registry and add timer metrics + lag gauge
  ;; NOTE: might be a way to see currently running timers, so indexing* gauges
  ;; can be discarded
  (let [docs-ingest-timer (timers/timer registry ["metrics" "ingest" "docs_timer"])
        tx-ingest-timer (timers/timer registry ["metrics" "ingest" "tx_timer"])
        !timer-contexts (atom {:tx {}
                               :docs {}})
        ingesting-docs (gauges/gauge-fn registry
                                        ["metrics" "ingest" "docs_ingesting"]
                                        #(count (:docs @!timer-contexts)))
        ingesting-tx (gauges/gauge-fn registry
                                      ["metrics" "ingest" "tx_ingesting"]
                                      #(count (:tx @!timer-contexts)))
        tx-id-lag (gauges/gauge-fn registry
                                   ["metrics" "ingest" "tx_id_lag"]
                                   #(when-let [latest-tx-id (db/read-index-meta indexer :crux.tx/latest-completed-tx)]
                                      (- (::tx/tx-id (db/latest-submitted-tx tx-log))
                                         (::tx/tx-id latest-tx-id))))]
    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexing-docs}}
                (fn [{:keys [doc-ids]}]
                  (swap! !timer-contexts assoc-in
                         [:docs doc-ids]
                         (timers/start docs-ingest-timer))))
    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexed-docs}}
                (fn [{:keys [doc-ids]}]
                  (timers/stop (get-in @!timer-contexts [:docs doc-ids]))
                  (swap! !timer-contexts update :docs dissoc doc-ids)))

    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexing-tx}}
                (fn [{:keys [crux.tx/submitted-tx]}]
                  (swap! !timer-contexts assoc-in
                         [:tx submitted-tx]
                         (timers/start tx-ingest-timer))))

    (bus/listen bus
                {:crux.bus/event-types #{:crux.tx/indexed-tx}}
                (fn [{:keys [crux.tx/submitted-tx]}]
                  (timers/stop (get-in @!timer-contexts [:tx submitted-tx]))
                  (swap! !timer-contexts update :tx dissoc submitted-tx)))
    {:ingesting-docs ingesting-docs
     :ingesting-tx ingesting-tx
     :tx-id-lag tx-id-lag
     :docs-ingest-timer docs-ingest-timer
     :tx-ingest-timer tx-ingest-timer}))
