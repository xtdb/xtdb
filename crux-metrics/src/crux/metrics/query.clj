(ns ^:no-doc crux.metrics.query
  (:require [crux.bus :as bus]
            [crux.node :as node]
            [crux.query :as q]
            [crux.metrics.dropwizard :as dropwizard])
  (:import java.util.Date))

(defn assign-listeners
  [registry {:crux.node/keys [bus]}]
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

(defn add-slow-query-listeners
  [{:crux.node/keys [bus]} {:crux.metrics/keys [slow-query-callback-fn] :as node-opts}]
  (let [!in-progress-queries (atom {})]
    (bus/listen bus {:crux/event-types #{::q/submitted-query}}
                (fn [{::q/keys [query-id] :as query}]
                  (swap! !in-progress-queries assoc query-id {:started-at (Date.)})))
    (bus/listen bus {:crux/event-types #{::q/completed-query ::q/failed-query}}
                (fn [{::q/keys [query-id] :as query}]
                  (let [started-at (get-in @!in-progress-queries [query-id :started-at])
                        query (assoc query :started-at started-at :finished-at (Date.))]
                    (swap! !in-progress-queries dissoc query-id)
                    (when (node/slow-query? query node-opts)
                      (slow-query-callback-fn query)))))))
