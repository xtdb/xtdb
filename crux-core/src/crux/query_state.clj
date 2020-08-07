(ns ^:no-doc crux.query-state
  (:import (crux.api QueryState QueryState$QueryStatus QueryState$QueryError)))

(defn <-QueryState [^QueryState query-state]
  {:status (case (str (.status query-state))
             "FAILED" :failed
             "COMPLETED" :completed
             "IN_PROGRESS" :in-progress)
   :query-id (.queryId query-state)
   :query (.query query-state)
   :started-at (.startedAt query-state)
   :finished-at (.finishedAt query-state)
   :error (when-let [error (.error query-state)]
            {:type (.errorClass error)
             :message (.errorMessage error)})})

(defn ->QueryState [{:keys [query-id started-at finished-at status error query] :as query-state}]
  (QueryState. query-id
               started-at
               finished-at
               (case status
                 :failed QueryState$QueryStatus/FAILED
                 :completed QueryState$QueryStatus/COMPLETED
                 :in-progress QueryState$QueryStatus/IN_PROGRESS)
               query
               (when error
                 (let [{:keys [type message]} error]
                   (QueryState$QueryError. type message)))))
