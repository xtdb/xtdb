(ns ^:no-doc crux.metrics.index-store
  (:require [crux.bus :as bus]
            [crux.api :as api]
            [crux.tx :as tx]
            [crux.metrics.dropwizard :as dropwizard])
  (:import (java.util Date)))

(defn assign-tx-id-lag [registry {:crux/keys [node]}]
  (dropwizard/gauge registry
                    ["index-store" "tx-id-lag"]
                    #(when-let [completed (api/latest-completed-tx node)]
                       (when-let [submitted (api/latest-submitted-tx node)]
                         (- (::tx/tx-id submitted)
                            (::tx/tx-id completed))))))

(defn assign-tx-latency-gauge [registry {:crux/keys [bus]}]
  (let [!last-tx-lag (atom 0)]
    (bus/listen bus
                {:crux/event-types #{::tx/indexed-tx}}
                (fn [{:keys [submitted-tx]}]
                  (reset! !last-tx-lag (- (System/currentTimeMillis)
                                          (.getTime ^Date (::tx/tx-time submitted-tx))))))
    (dropwizard/gauge registry
                      ["index-store" "tx-latency"]
                      (fn []
                        (first (reset-vals! !last-tx-lag 0))))))

(defn assign-doc-meters [registry {:crux/keys [bus]}]
  (let [docs-ingested-meter (dropwizard/meter registry ["index-store" "indexed-docs"])
        av-ingested-meter (dropwizard/meter registry ["index-store" "indexed-avs"])
        bytes-ingested-meter (dropwizard/meter registry ["index-store" "indexed-bytes"])]
    (bus/listen bus
                {:crux/event-types #{::tx/indexed-tx}}
                (fn [{:keys [doc-ids av-count bytes-indexed]}]
                  (dropwizard/mark! docs-ingested-meter (count doc-ids))
                  (dropwizard/mark! av-ingested-meter av-count)
                  (dropwizard/mark! bytes-ingested-meter bytes-indexed)))
    {:docs-ingested-meter docs-ingested-meter
     :av-ingested-meter av-ingested-meter
     :bytes-ingested-meter bytes-ingested-meter}))

(defn assign-tx-timer [registry {:crux/keys [bus]}]
  (let [timer (dropwizard/timer registry ["index-store" "indexed-txs"])
        !timer-store (atom {})]
    (bus/listen bus
                {:crux/event-types #{::tx/indexing-tx ::tx/indexed-tx}}
                (fn [{:keys [crux/event-type submitted-tx]}]
                  (case event-type
                    ::tx/indexing-tx
                    (swap! !timer-store assoc submitted-tx (dropwizard/start timer))

                    ::tx/indexed-tx
                    (when-let [timer-context (get @!timer-store submitted-tx)]
                      (dropwizard/stop timer-context)
                      (swap! !timer-store dissoc submitted-tx)))))
    timer))

(defn assign-listeners
  "Assigns listeners to an event bus for a given node.
  Returns an atom containing updating metrics"
  [registry deps]
  (merge (assign-doc-meters registry deps)
         {:tx-id-lag (assign-tx-id-lag registry deps)
          :tx-latency-gauge (assign-tx-latency-gauge registry deps)
          :tx-ingest-timer (assign-tx-timer registry deps)}))
