(ns crux.metrics.query
  (:require [crux.bus :as bus]
            [crux.metrics.dropwizard :as dropwizard]))

(defn assign-query-timer
  [registry {:crux.node/keys [bus]}]
  (let [!timer-store (atom {})
        query-timer (dropwizard/timer registry ["crux" "query" "timer"])]
    (bus/listen bus {:crux.bus/event-types #{:crux.query/submitted-query}}
              (fn [event]
                (swap! !timer-store assoc (:crux.query/uuid event) (dropwizard/start query-timer))))

    (bus/listen bus {:crux.bus/event-types #{:crux.query/completed-query}}
              (fn [event]
                (dropwizard/stop (get @!timer-store (:crux.query/uuid event)))
                (swap! !timer-store dissoc (:crux.query/uuid event))))
    query-timer))

(defn assign-listeners
  [registry deps]
  {:query-timer (assign-query-timer registry deps)})
