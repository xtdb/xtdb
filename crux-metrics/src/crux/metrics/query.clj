(ns ^:no-doc crux.metrics.query
  (:require [crux.bus :as bus]
            [crux.node :as node]
            [crux.query :as q]
            [crux.metrics.dropwizard :as dropwizard])
  (:import java.util.Date))

(defn assign-listeners
  [registry {:crux/keys [bus]}]
  (let [!timer-store (atom {})
        query-timer (dropwizard/timer registry ["query" "timer"])]
    (bus/listen bus {:crux/event-types #{:crux.query/submitted-query}}
                (fn [event]
                  (swap! !timer-store assoc (:crux.query/query-id event) (dropwizard/start query-timer))))

    (bus/listen bus {:crux/event-types #{:crux.query/completed-query}}
                (fn [event]
                  (dropwizard/stop (get @!timer-store (:crux.query/query-id event)))
                  (swap! !timer-store dissoc (:crux.query/query-id event))))
    {:query-timer query-timer
     :current-query-count (dropwizard/gauge registry
                                            ["query" "currently-running"]
                                            (fn [] (count @!timer-store)))}))
