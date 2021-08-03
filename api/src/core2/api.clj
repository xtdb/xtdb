(ns core2.api
  (:import java.util.Date
           java.util.concurrent.ExecutionException))

(defrecord TransactionInstant [^long tx-id, ^Date tx-time]
  Comparable
  (compareTo [_ other]
    (- tx-id (.tx-id ^TransactionInstant other))))

(defprotocol PClient
  (latest-completed-tx ^core2.api.TransactionInstant [node])

  ;; we may want to go to `open-query-async`/`Stream` instead, when we have a Java API
  (-plan-query-async ^java.util.concurrent.CompletableFuture [node query basis params]))

(defprotocol PSubmitNode
  (submit-tx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [tx-producer tx-ops]))

(defn ^java.util.concurrent.CompletableFuture plan-query-async [node query & params]
  (let [[basis params] (let [[maybe-basis & more-params] params]
                         (if (map? maybe-basis)
                           [maybe-basis more-params]
                           [{} params]))]
    (-plan-query-async node query basis params)))

(defn plan-query [node query & params]
  (try
    @(apply plan-query-async node query params)
    (catch ExecutionException e
      (throw (.getCause e)))))

(let [arglists '([node query & params]
                 [node query basis-opts & params])]
  (alter-meta! #'plan-query-async assoc :arglists arglists)
  (alter-meta! #'plan-query assoc :arglists arglists))
