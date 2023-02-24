(ns core2.api
  (:import clojure.lang.IReduceInit
           core2.IResultSet
           java.io.Writer
           java.time.Instant
           java.util.concurrent.ExecutionException
           java.util.function.Function))

(defrecord TransactionInstant [^long tx-id, ^Instant sys-time]
  Comparable
  (compareTo [_ tx-key]
    (Long/compare tx-id (.tx-id ^TransactionInstant tx-key))))

(defmethod print-dup TransactionInstant [tx-key ^Writer w]
  (.write w "#c2/tx-key ")
  (print-method (into {} tx-key) w))

(defmethod print-method TransactionInstant [tx-key w]
  (print-dup tx-key w))

(defrecord ClojureForm [form])

(defmethod print-dup ClojureForm [{:keys [form]} ^Writer w]
  (.write w "#c2/clj-form ")
  (print-method form w))

(defmethod print-method ClojureForm [clj-form w]
  (print-dup clj-form w))

(defprotocol PClient
  ;; we may want to go to `Stream` instead, when we have a Java API
  (-open-datalog-async ^java.util.concurrent.CompletableFuture [node query args])

  ;; TODO will want to accept params in here eventually.
  (-open-sql-async ^java.util.concurrent.CompletableFuture [node query query-opts]
    "query-opts :: :basis, :basis-timeout, :?"))

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

(defn open-datalog-async ^java.util.concurrent.CompletableFuture [node query & args]
  (-open-datalog-async node query args))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-datalog ^core2.IResultSet [node query & args]
  (-> @(-open-datalog-async node query args)
      rethrowing-cause))

(defn plan-datalog-async
  "Calling `reduce` on the result from `plan-datalog-async` yields a `CompletableFuture`."
  [node query & args]

  (reify IReduceInit
    (reduce [_ f init]
      (-> (-open-datalog-async node query args)
          (.thenApply (reify Function
                        (apply [_ res]
                          (with-open [^IResultSet res res]
                            (reduce f init (iterator-seq res))))))))))

(defn plan-datalog [node query & args]
  (reify IReduceInit
    (reduce [_ f init]
      (-> @(reduce f init (apply plan-datalog-async node query args))
          rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn datalog-query-async ^java.util.concurrent.CompletableFuture [node query & args]
  (into [] (apply plan-datalog-async node query args)))

(defn datalog-query [node query & args]
  (into [] (apply plan-datalog node query args)))

;;;; SQL

(defn open-sql-async ^java.util.concurrent.CompletableFuture [node query query-opts]
  (-open-sql-async node query query-opts))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-sql ^core2.IResultSet [node query query-opts]
  (-> @(-open-sql-async node query query-opts)
      rethrowing-cause))

(defn plan-sql-async
  "Calling `reduce` on the result from `plan-sql-async` yields a `CompletableFuture`."
  [node query query-opts]

  (reify IReduceInit
    (reduce [_ f init]
      (-> (-open-sql-async node query query-opts)
          (.thenApply (reify Function
                        (apply [_ res]
                          (with-open [^IResultSet res res]
                            (reduce f init (iterator-seq res))))))))))

(defn plan-sql [node query query-opts]
  (reify IReduceInit
    (reduce [_ f init]
      (-> @(reduce f init (plan-sql-async node query query-opts))
          rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn sql-query-async ^java.util.concurrent.CompletableFuture [node query query-opts]
  (into [] (plan-sql-async node query query-opts)))

(defn sql-query [node query query-opts]
  (into [] (plan-sql node query query-opts)))

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
