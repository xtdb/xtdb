(ns tmt.noopnil
  (:require [crux.api]))

;; atm, I have to write(def merge-fn
(def merge-fn-hack  {:crux.db/id :spoc/merge
                     :crux.db.fn/body '(fn [db entity-id valid-time new-doc]
                                         (let [current-doc (crux.api/entity db entity-id)
                                               merged-doc (merge current-doc new-doc)]
                                           (if (not= current-doc merged-doc)
                                             [(cond-> [:crux.tx/put merged-doc]
                                                valid-time (conj valid-time))]
                                             [])))})

;; I'd like to write:(def merge-fn
(def merge-fn  {:crux.db/id :spoc/merge
                :crux.db.fn/body '(fn [db entity-id valid-time new-doc]
                                    (let [current-doc (crux.api/entity db entity-id)
                                          merged-doc (merge current-doc new-doc)]
                                      (when (not= current-doc merged-doc)
                                        [(cond-> [:crux.tx/put merged-doc]
                                           valid-time (conj valid-time))])))})
