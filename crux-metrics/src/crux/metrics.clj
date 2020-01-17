(ns crux.metrics
  (:require [crux.api :as api]
            [crux.bus :as bus]))

(defn ingesting-tx [!metrics]
  (count (::indexing-tx @!metrics)))

(defn ingested-docs [!metrics]
  (count (::indexing-docs @!metrics)))

(defn ingested-tx [!metrics]
  (count (::indexing-tx @!metrics)))

(defn current-lag [!metrics]
  (::current-lag @!metrics))

;; I might be storing too much metadata. Maybe timings don't need to be stored
(defn assign-ingest
  [node]
  "Assigns listeners to an event bus for a given node.
  Returns an atom containing uptading metrics"

  (let [!metrics (atom {::indexing-tx {}
                        ::indexed-tx {}
                        ::indexing-docs {}
                        ::indexed-docs {}
                        ::current-lag 0})]
    (bus/listen (:bus node)
                {:crux.bus/event-types #{:crux.tx/indexing-docs}}
                (fn [{:keys [doc-ids]}]
                  (swap! !metrics update ::indexing-docs assoc
                         doc-ids {:start-time-ms (System/currentTimeMillis)})))
  (bus/listen (:bus node)
              {:crux.bus/event-types #{:crux.tx/indexed-docs}}
              (fn [{:keys [doc-ids]}]
                (let [{:keys start-time-ms} (get doc-ids @!metrics)
                      start-time-ms (:start-time-ms doc-ids)
                      end-time-ms (System/currentTimeMillis)]
                  (swap! !metrics update ::indexing-docs dissoc doc-ids)
                  (swap! !metrics update ::indexed-docs assoc doc-ids
                         {:start-time-ms start-time-ms
                          :end-time-ms end-time-ms
                          :time-elapsed-ms (- end-time-ms start-time-ms)})
                  (swap! !metrics update ::current-lag (- end-time-ms start-time-ms)))))

  (bus/listen (:bus node)
              {:crux.bus/event-types #{:crux.tx/indexing-tx}}
              (fn [event]
                (swap! !metrics update ::indexing-tx assoc
                       (:crux.tx/submitted-tx event) {:start-time-ms (System/currentTimeMillis)})))

  (bus/listen (:bus node)
              {:crux.bus/event-types #{:crux.tx/indexed-tx}}
              (fn [event]
                (let [{:keys start-time-ms} (get doc-ids @!metrics)
                      start-time-ms (:start-time-ms doc-ids)
                      end-time-ms (System/currentTimeMillis)]
                  (swap! !metrics update ::indexing-tx dissoc (:crux.tx/submitted-tx event))
                  (swap! !metrics update ::indexed-tx assoc (:crux.tx/submitted-tx event)
                         {:start-time-ms start-time-ms
                          :end-time-ms end-time-ms
                          :time-elapsed-ms (- end-time-ms start-time-ms)})
                  (swap! !metrics update ::current-lag (- end-time-ms start-time-ms)))))
  !metrics))

#_(with-open [node (api/start-node {:crux.node/topology :crux.standalone/topology
                                    :crux.node/kv-store "crux.kv.memdb/kv"
                                    :crux.kv/db-dir "data/db-dir-1"
                                    :crux.standalone/event-log-dir "data/eventlog-1"
                                    :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"})]
    (reset! !metrics {::indexing-tx #{}
                      ::indexed-tx #{}
                      ::indexing-docs #{}
                      ::indexed-docs #{}})
    (Thread/sleep 1000)
    (assign-ingest node)
    (Thread/sleep 1000)
    (api/submit-tx node [[:crux.tx/put {:crux.db/id (keyword (str (rand-int 1000)))}]])
    (Thread/sleep 1000)
    @!metrics)
