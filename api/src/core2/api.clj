(ns core2.api
  (:require core2.edn)
  (:import clojure.lang.IReduceInit
           core2.IResultSet
           java.io.Writer
           java.time.Instant
           java.util.concurrent.ExecutionException
           java.util.function.Function))

(defrecord TransactionInstant [^long tx-id, ^Instant tx-time]
  Comparable
  (compareTo [_ tx-key]
    (Long/compare tx-id (.tx-id ^TransactionInstant tx-key))))

(defmethod print-dup TransactionInstant [tx-key ^Writer w]
  (.write w "#c2/tx-key ")
  (print-method (into {} tx-key) w))

(defmethod print-method TransactionInstant [tx-key w]
  (print-dup tx-key w))

(defprotocol PClient
  ;; we may want to go to `Stream` instead, when we have a Java API
  (-open-datalog-async ^java.util.concurrent.CompletableFuture [node query params])
  (-open-sql-async ^java.util.concurrent.CompletableFuture [node query params]))

(defprotocol PSubmitNode
  (^java.util.concurrent.CompletableFuture #_<TransactionInstant>
   submit-tx
   [node tx-ops]
   [node tx-ops opts]))

(defprotocol PStatus
  (status [node]))

(defmacro ^:private rethrowing-cause [form]
  `(try
     ~form
     (catch ExecutionException e#
       (throw (.getCause e#)))))

(defn open-datalog-async ^java.util.concurrent.CompletableFuture [node query & params]
  (-open-datalog-async node query params))

(defn open-datalog ^core2.IResultSet [node query & params]
  (-> @(-open-datalog-async node query params)
      rethrowing-cause))

(defn plan-datalog-async
  "Calling `reduce` on the result from `plan-datalog-async` yields a `CompletableFuture`."
  [node query & params]

  (reify IReduceInit
    (reduce [_ f init]
      (-> (-open-datalog-async node query params)
          (.thenApply (reify Function
                        (apply [_ res]
                          (with-open [^IResultSet res res]
                            (reduce f init (iterator-seq res))))))))))

(defn plan-datalog [node query & params]
  (reify IReduceInit
    (reduce [_ f init]
      (-> @(reduce f init (apply plan-datalog-async node query params))
          rethrowing-cause))))

(defn datalog-query-async ^java.util.concurrent.CompletableFuture [node query & params]
  (into [] (apply plan-datalog-async node query params)))

(defn datalog-query [node query & params]
  (into [] (apply plan-datalog node query params)))

;;;; SQL

(defn open-sql-async ^java.util.concurrent.CompletableFuture [node query & params]
  (-open-sql-async node query params))

(defn open-sql ^core2.IResultSet [node query & params]
  (-> @(-open-sql-async node query params)
      rethrowing-cause))

(defn plan-sql-async
  "Calling `reduce` on the result from `plan-sql-async` yields a `CompletableFuture`."
  [node query & params]

  (reify IReduceInit
    (reduce [_ f init]
      (-> (-open-sql-async node query params)
          (.thenApply (reify Function
                        (apply [_ res]
                          (with-open [^IResultSet res res]
                            (reduce f init (iterator-seq res))))))))))

(defn plan-sql [node query & params]
  (reify IReduceInit
    (reduce [_ f init]
      (-> @(reduce f init (apply plan-sql-async node query params))
          rethrowing-cause))))

(defn sql-query-async ^java.util.concurrent.CompletableFuture [node query & params]
  (into [] (apply plan-sql-async node query params)))

(defn sql-query [node query & params]
  (into [] (apply plan-sql node query params)))

(def http-routes
  [["/status" {:name :status
               :summary "Status"
               :description "Get status information from the node"}]

   ["/tx" {:name :tx
           :summary "Transaction"
           :description "Submits a transaction to the cluster"}]

   ["/datalog" {:name :datalog-query
                :summary "Datalog Query"}]

   ["/sql" {:name :sql-query
            :summary "SQL"}]])
