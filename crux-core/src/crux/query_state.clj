(ns ^:no-doc crux.query-state
  (:import (crux.api IQueryState IQueryState$IQueryError IQueryState$QueryStatus)
           (java.io Writer)))

(defn ->query-status [status]
  (case status
    :failed IQueryState$QueryStatus/FAILED
    :completed IQueryState$QueryStatus/COMPLETED
    :in-progress IQueryState$QueryStatus/IN_PROGRESS))

(defrecord QueryError [type message]
  IQueryState$IQueryError
  (getErrorClass [this] type)
  (getErrorMessage [this] message))

(defrecord QueryState [query-id started-at finished-at status query error]
  IQueryState
  (getQueryId [this] query-id)
  (getStartedAt [this] started-at)
  (getFinishedAt [this] finished-at)
  (getStatus [this] (->query-status status))
  (getQuery [this] query)
  (getError [this] error))

(defmethod print-method QueryState [qs ^Writer w]
  (.write w "#xt/query-state ")
  (print-method (into {} qs) w))

(defmethod print-method QueryError [qs ^Writer w]
  (.write w "#xt/query-error ")
  (print-method (into {} qs) w))

(defn ->QueryError [error]
  (let [{:keys [type message]} error]
    (QueryError. type message)))

(defn ->QueryState [{:keys [query-id started-at finished-at status error query] :as query-state}]
  (QueryState. query-id
               started-at
               finished-at
               status
               query
               (when error
                 (->QueryError error))))
