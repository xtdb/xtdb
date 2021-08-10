(ns core2.api
  (:import clojure.lang.IReduceInit
           core2.IResultSet
           java.io.Writer
           java.util.concurrent.ExecutionException
           java.util.Date
           java.util.function.Function))

(defrecord TransactionInstant [^long tx-id, ^Date tx-time]
  Comparable
  (compareTo [_ other]
    (- tx-id (.tx-id ^TransactionInstant other))))

(defmethod print-method TransactionInstant [tx-instant ^Writer w]
  (.write w (str "#core2/tx-instant " (select-keys tx-instant [:tx-id :tx-time]))))

(defprotocol PClient
  ;; we may want to go to `Stream` instead, when we have a Java API
  (-open-query-async ^java.util.concurrent.CompletableFuture [node query params]))

(defprotocol PSubmitNode
  (submit-tx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [node tx-ops]))

(defprotocol PStatus
  (status [node]))

(defmacro ^:private rethrowing-cause [form]
  `(try
     ~form
     (catch ExecutionException e#
       (throw (.getCause e#)))))

(defn open-query-async ^java.util.concurrent.CompletableFuture [node query & params]
  (-open-query-async node query params))

(defn open-query ^core2.IResultSet [node query & params]
  (-> @(-open-query-async node query params)
      rethrowing-cause))

(defn plan-query-async
  "Calling `reduce` on the result from `plan-query-async` yields a `CompletableFuture`."
  [node query & params]

  (reify IReduceInit
    (reduce [_ f init]
      (-> (-open-query-async node query params)
          (.thenApply (reify Function
                        (apply [_ res]
                          (with-open [^IResultSet res res]
                            (reduce f init (iterator-seq res))))))))))

(defn plan-query [node query & params]
  (reify IReduceInit
    (reduce [_ f init]
      (-> @(reduce f init (apply plan-query-async node query params))
          rethrowing-cause))))

(defn query-async ^java.util.concurrent.CompletableFuture [node query & params]
  (into [] (apply plan-query-async node query params)))

(defn query [node query & params]
  (into [] (apply plan-query node query params)))

(def http-routes
  [["/status" {:name :status
               :summary "Status"
               :description "Get status information from the node"}]

   ["/tx" {:name :tx
           :summary "Transaction"
           :description "Submits a transaction to the cluster"}]

   ["/query" {:name :query
              :summary "Query"}]])
