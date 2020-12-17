(ns ^:no-doc crux.query-state
    (:import (crux.api IQueryState IQueryState$IQueryError IQueryState$QueryStatus)))

(defrecord QueryState [query-id started-at finished-at status query error]
           IQueryState
           (getQueryId [this] query-id)
           (getStartedAt [this] started-at)
           (getFinishedAt [this] finished-at)
           (getStatus ^IQueryState$QueryStatus [this] status)
           (getQuery [this] query)
           (getError ^IQueryState$IQueryError [this] error))

(defrecord QueryState$QueryError [type message]
           IQueryState$IQueryError
           (getErrorClass [this] type)
           (getErrorMessage [this] type))

(defn <-QueryState [^QueryState query-state]
      {:status      (case (str (.getStatus query-state))
                          "FAILED" :failed
                          "COMPLETED" :completed
                          "IN_PROGRESS" :in-progress)
       :query-id    (.getQueryId query-state)
       :query       (.getQuery query-state)
       :started-at  (.getStartedAt query-state)
       :finished-at (.getFinishedAt query-state)
       :error       (when-let [error (.getError query-state)]
                              {:type    (.getErrorClass error)
                               :message (.getErrorMessage error)})})

(defn ->QueryState [{:keys [query-id started-at finished-at status error query] :as query-state}]
      (QueryState. query-id
                   started-at
                   finished-at
                   (case status
                         :failed IQueryState$QueryStatus/FAILED
                         :completed IQueryState$QueryStatus/COMPLETED
                         :in-progress IQueryState$QueryStatus/IN_PROGRESS)
                   query
                   (when error
                         (let [{:keys [type message]} error]
                              (QueryState$QueryError. type message)))))