(ns crux.metrics.query
  (:require [crux.bus :as bus]
            [metrics.timers :as timers]))

(defn assign-query-timer
  [registry {:crux.node/keys [bus]}]
  (let [!timer-store (atom {})
        query-timer (timers/timer registry ["crux" "query" "timer"])]
    (bus/listen bus #{:crux.query/submitted-query}
              (fn [event]
                (swap! !timer-store assoc event (timers/start query-timer))))

    (bus/listen bus #{:crux.query/completed-query}
              (fn [event]
                (get @!timer-store event)
                (swap! !timer-store dissoc event)))
    query-timer))

(defn assign-listeners
  [registry deps]
  {:query-timer (assign-query-timer registry deps)})
