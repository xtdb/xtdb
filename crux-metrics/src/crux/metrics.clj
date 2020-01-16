(ns crux.metrics
  (:require [crux.api :as api]
            [crux.bus :as bus]
            [clojure.set :refer (union difference)]))

(def !metrics (atom {::indexing-tx #{}
                     ::indexed-tx #{}
                     ::indexing-docs #{}
                     ::indexed-docs #{}}))

(defn ingesting-docs []
  (count (::indexing-docs @!metrics)))

(defn ingesting-tx []
  (count (::indexing-tx @!metrics)))

(defn ingested-docs []
  (count (::indexing-docs @!metrics)))

(defn ingested-tx []
  (count (::indexing-tx @!metrics)))

(defn assign-ingest
  [node]

  (bus/listen (:bus node)
              {:crux.bus/event-types #{:crux.tx/indexing-docs}}
              (fn [{:keys [doc-ids]}]
                (swap! !metrics update ::indexing-docs union doc-ids)))
  (bus/listen (:bus node)
              {:crux.bus/event-types #{:crux.tx/indexed-docs}}
              (fn [{:keys [doc-ids]}]
                (swap! !metrics update ::indexing-docs difference doc-ids)
                (swap! !metrics update ::indexed-docs union doc-ids)))

  (bus/listen (:bus node)
              {:crux.bus/event-types #{:crux.tx/indexing-tx}}
              (fn [event]
                (swap! !metrics update ::indexing-tx union #{(:crux.tx/submitted-tx event)})))

  (bus/listen (:bus node)
              {:crux.bus/event-types #{:crux.tx/indexed-tx}}
              (fn [event]
                (swap! !metrics update ::indexing-tx difference #{(:crux.tx/submitted-tx event)})
                (swap! !metrics update ::indexed-tx union #{(:crux.tx/submitted-tx event)}))))

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
