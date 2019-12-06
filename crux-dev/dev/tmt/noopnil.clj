(ns tmt.noopnil
  (:require [crux.api]))

(def node (crux.api/start-node {:crux.node/topology :crux.standalone/topology
                                :crux.node/kv-store "crux.kv.memdb/kv"
                                :crux.kv/db-dir "data/db-dir-1"
                                :crux.standalone/event-log-dir "data/eventlog-1"
                                :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"}))

;; atm, I have to write
(def merge-fn-hack  {:crux.db/id :spoc/merge
                     :crux.db.fn/body '(fn [db entity-id valid-time new-doc]
                                         (let [current-doc (crux.api/entity db entity-id)
                                               merged-doc (merge current-doc new-doc)]
                                           (if (not= current-doc merged-doc)
                                             [(cond-> [:crux.tx/put merged-doc]
                                                valid-time (conj valid-time))]
                                             [])))})

;; I'd like to write:
(def merge-fn  {:crux.db/id :spoc/merge
                :crux.db.fn/body '(fn [db entity-id valid-time new-doc]
                                    (let [current-doc (crux.api/entity db entity-id)
                                          merged-doc (merge current-doc new-doc)]
                                      (when (not= current-doc merged-doc)
                                        [(cond-> [:crux.tx/put merged-doc]
                                           valid-time (conj valid-time))])))})

(defn merge-hack [db entity-id valid-time new-doc]
  (let [current-doc (crux.api/entity db entity-id)
        merged-doc (merge current-doc new-doc)]
    (if (not= current-doc merged-doc)
      [(cond-> [:crux.tx/put merged-doc]
         valid-time (conj valid-time))]
      [])))

(defn merge-desired [db entity-id valid-time new-doc]
  (let [current-doc (crux.api/entity db entity-id)
        merged-doc (merge current-doc new-doc)]
    (when (not= current-doc merged-doc)
      [(cond-> [:crux.tx/put merged-doc]
         valid-time (conj valid-time))])))

(crux.api/submit-tx node [[:crux.tx/put {:crux.db/id :id :this :that}]])

(crux.api/submit-tx node (merge-hack (crux.api/db node) :id (java.util.Date.) {}))
(crux.api/submit-tx node (merge-desired (crux.api/db node) :id (java.util.Date.) {}))
