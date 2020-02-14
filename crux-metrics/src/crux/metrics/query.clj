(ns crux.metrics.query
  (:require [crux.bus :as bus]
            [crux.metrics.dropwizard :as dropwizard]))

(defn assign-listeners
  [registry {:crux.node/keys [bus]}]
  (let [!timer-store (atom {})
        query-timer (dropwizard/timer registry ["query" "timer"])]
    (bus/listen bus {:crux.bus/event-types #{:crux.query/submitted-query}}
              (fn [event]
                (swap! !timer-store assoc (:crux.query/query-id event) (dropwizard/start query-timer))))

    (bus/listen bus {:crux.bus/event-types #{:crux.query/completed-query}}
              (fn [event]
                (dropwizard/stop (get @!timer-store (:crux.query/query-id event)))
                (swap! !timer-store dissoc (:crux.query/query-id event))))
    {:query-timer query-timer
     :current-query-count (dropwizard/gauge registry
                                            ["query" "currently-running"]
                                            (fn [] (count @!timer-store)))}))
