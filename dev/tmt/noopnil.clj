(ns tmt.noopnil
  (:require [crux.api :as api]))

(def node (api/start-node {:crux.node/topology 'crux.standalone/topology
                           :crux.kv/db-dir "data/db-dir-1"
                           :crux.standalone/event-log-dir "data/eventlog-1" }))

;; atm, I have to write
(def merge-fn-hack  {:crux.db/id :spoc/merge-hack
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

(api/submit-tx node [[:crux.tx/put {:crux.db/id :id :this :that}]])

(api/q (api/db node) {:find ['e] :where [['e :crux.db/id :id]] :full-results? true})

(api/submit-tx node [[:crux.tx/put merge-fn]])
(api/submit-tx node [[:crux.tx/put merge-fn-hack]])
;; id valid-time doc
(api/submit-tx node [[:crux.tx/fn
                      :spoc/merge
                      {:crux.db/id
                       (java.util.UUID/randomUUID)
                       :crux.db.fn/args [:id (java.util.Date.) {:crux.db/id :id :this :that}]}]])

(api/q (api/db node) {:find ['e] :where [['e :crux.db/id :id]] :full-results? true})
