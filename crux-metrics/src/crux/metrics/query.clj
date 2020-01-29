(ns crux.metrics.query
  (:require [crux.bus :as bus]
            [crux.dropwizard :as dropwizard]))

(defn assign-query-timer
  [registry {:crux.node/keys [bus]}]
  (let [!timer-store (atom {})
        query-timer (dropwizard/timer registry ["crux" "query" "timer"])]
    (bus/listen bus #{:crux.query/submitted-query}
              (fn [event]
                (swap! !timer-store assoc event (dropwizard/start query-timer))))

    (bus/listen bus #{:crux.query/completed-query}
              (fn [event]
                (dropwizard/stop (get @!timer-store event))
                (swap! !timer-store dissoc event)))
    query-timer))

(defn assign-listeners
  [registry deps]
  {:query-timer (assign-query-timer registry deps)})
