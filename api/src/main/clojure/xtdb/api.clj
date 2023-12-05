(ns xtdb.api
  (:require [clojure.spec.alpha :as s]
            [xtdb.backtick :as backtick]
            [xtdb.error :as err]
            [xtdb.protocols :as xtp]
            [xtdb.time :as time])
  (:import [java.io Writer]
           [java.time Instant]
           java.util.concurrent.ExecutionException
           java.util.function.Function
           java.util.List
           xtdb.IResultSet
           [xtdb.tx Ops Ops$HasArgs Ops$HasValidTimeBounds]
           xtdb.types.ClojureForm))

(defmacro ^:private rethrowing-cause [form]
  `(try
     ~form
     (catch ExecutionException e#
       (throw (.getCause e#)))))

(defrecord TransactionKey [^long tx-id, ^Instant system-time]
  Comparable
  (compareTo [_ tx-key]
    (Long/compare tx-id (.tx-id ^TransactionKey tx-key))))

(defmethod print-dup TransactionKey [tx-key ^Writer w]
  (.write w "#xt/tx-key ")
  (print-method (into {} tx-key) w))

(defmethod print-method TransactionKey [tx-key w]
  (print-dup tx-key w))

(defn ->ClojureForm [form]
  (ClojureForm. form))

(defmethod print-dup ClojureForm [^ClojureForm clj-form ^Writer w]
  (.write w "#xt/clj-form ")
  (print-method (.form clj-form) w))

(defmethod print-method ClojureForm [clj-form w]
  (print-dup clj-form w))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q&
  "asynchronously query an XTDB node.

  - query: either an XTQL or SQL query.
  - opts:
    - `:basis`: see 'Transaction Basis'
    - `:args`: arguments to pass to the query.

  For example:

  (q& node '(from ...))

  (q& node '(from :foo [{:a $a, :b $b}])
      {:a a-value, :b b-value})

  (q& node \"SELECT foo.id, foo.v FROM foo WHERE foo.id = 'my-foo'\")
  (q& node \"SELECT foo.id, foo.v FROM foo WHERE foo.id = ?\" {:args [foo-id]})

  Please see XTQL/SQL query language docs for more details.

  This function returns a CompleteableFuture containing the results of its query as a vector of maps

  Transaction Basis:

  In XTDB there are a number of ways to control at what point in time a query is run -
  this is done via a basis map optionally supplied as part of the query map.

  In the case a basis is not provided the query is guaranteed to run sometime after
  the latest transaction submitted by this connection/node.

  Alternatively a basis map containing reference to a specific transaction can be supplied,
  in this case the query will be run exactly at that transaction, ensuring the repeatability of queries.

  This tx key is the same map returned by submit-tx

  (q& node '(from ...)
      {:basis {:tx tx}})

  Additionally a basis timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q& node '(from ...)
      {:basis-timeout (Duration/ofSeconds 1)})"

  (^java.util.concurrent.CompletableFuture [node q+args] (q& node q+args {}))

  (^java.util.concurrent.CompletableFuture
   [node query opts]
   (let [opts (-> (into {:default-all-valid-time? false} opts)
                  (update :basis xtp/after-latest-submitted-tx node))]

     (-> (xtp/open-query& node query opts)
         (.thenApply
          (reify Function
            (apply [_ res]
              (with-open [^IResultSet res res]
                (vec (iterator-seq res))))))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q
  "query an XTDB node.

  - query: either an XTQL or SQL query.
  - opts:
    - `:basis`: see 'Transaction Basis'
    - `:args`: arguments to pass to the query.

  For example:

  (q& node '(from ...))

  (q& node '(from :foo [{:a $a, :b $b}])
      {:a a-value, :b b-value})

  (q& node \"SELECT foo.id, foo.v FROM foo WHERE foo.id = 'my-foo'\")
  (q& node \"SELECT foo.id, foo.v FROM foo WHERE foo.id = ?\" {:args [foo-id]})

  Please see XTQL/SQL query language docs for more details.

  This function returns the results of its query as a vector of maps

  Transaction Basis:

  In XTDB there are a number of ways to control at what point in time a query is run -
  this is done via a basis map optionally supplied as part of the query map.

  In the case a basis is not provided the query is guaranteed to run sometime after
  the latest transaction submitted by this connection/node.

  Alternatively a basis map containing reference to a specific transaction can be supplied,
  in this case the query will be run exactly at that transaction, ensuring the repeatability of queries.

  This tx reference (known as a TransactionKey) is the same map returned by submit-tx

  (q node '(from ...)
     {:basis {:tx tx}})

  Additionally a Basis Timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q node '(from ...)
     {:basis-timeout (Duration/ofSeconds 1)})"
  ([node q+args]
   (-> @(q& node q+args)
       (rethrowing-cause)))

  ([node q+args opts]
   (-> @(q& node q+args opts)
       (rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx&
  "Writes transactions to the log for processing. Non-blocking.

  tx-ops: XTQL/SQL transaction operations.

  Returns a CompleteableFuture containing a map with details about
  the submitted transaction, including system-time and tx-id.

  opts (map):
   - :system-time
     overrides system-time for the transaction,
     mustn't be earlier than any previous system-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId"
  (^java.util.concurrent.CompletableFuture [node tx-ops] (submit-tx& node tx-ops {}))
  (^java.util.concurrent.CompletableFuture [node tx-ops tx-opts]
   (xtp/submit-tx& node tx-ops tx-opts)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx
  "Writes transactions to the log for processing

  tx-ops: XTQL/SQL style transactions.
    [(xt/put :table {:xt/id \"my-id\", ...})
     (xt/delete :table \"my-id\")]

    [(-> (xt/sql-op \"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\")
         (xt/with-op-args [0 1]))

     (-> (xt/sql-op \"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\")
         (xt/with-op-args [2 3] [4 5] [6 7]))

     (xt/sql-op \"UPDATE foo SET b = 1\")]

  Returns a map with details about the submitted transaction, including system-time and tx-id.

  opts (map):
   - :system-time
     overrides system-time for the transaction,
     mustn't be earlier than any previous system-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId"

  ([node tx-ops] (submit-tx node tx-ops {}))
  ([node tx-ops tx-opts]
   (-> @(submit-tx& node tx-ops tx-opts)
       (rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn status
  "Returns the status of this node as a map,
  including details of both the latest submitted and completed tx"
  [node]
  (xtp/status node))

(def ^:private eid? (some-fn uuid? integer? string? keyword?))

(def ^:private table? keyword?)

(defn- expect-table-name [table-name]
  (when-not (table? table-name)
    (throw (err/illegal-arg :xtdb.tx/invalid-table
                            {::err/message "expected table name" :table table-name})))

  table-name)

(defn- expect-eid [eid]
  (if-not (eid? eid)
    (throw (err/illegal-arg :xtdb.tx/invalid-eid
                            {::err/message "expected xt/id", :xt/id eid}))
    eid))

(defn- expect-doc [doc]
  (when-not (map? doc)
    (throw (err/illegal-arg :xtdb.tx/expected-doc
                            {::err/message "expected doc map", :doc doc})))
  (expect-eid (:xt/id doc))

  doc)

(defn- expect-instant [instant]
  (when-not (s/valid? ::time/datetime-value instant)
    (throw (err/illegal-arg :xtdb.tx/invalid-date-time
                            {::err/message "expected date-time"
                             :timestamp instant})))

  (time/->instant instant))

(defn put [table doc]
  (Ops/put (expect-table-name table) (expect-doc doc)))

(defn- expect-fn-id [fn-id]
  (if-not (eid? fn-id)
    (throw (err/illegal-arg :xtdb.tx/invalid-fn-id {::err/message "expected fn-id", :fn-id fn-id}))
    fn-id))

(defn- expect-tx-fn [tx-fn]
  (or tx-fn
      (throw (err/illegal-arg :xtdb.tx/invalid-tx-fn {::err/message "expected tx-fn", :tx-fn tx-fn}))))

(defn put-fn [fn-id tx-fn]
  (Ops/putFn (expect-fn-id fn-id) (expect-tx-fn tx-fn)))

(defn delete [table id]
  (Ops/delete table (expect-eid id)))

(defn during [^Ops$HasValidTimeBounds tx-op from until]
  (.during tx-op (expect-instant from) (expect-instant until)))

(defn starting-from [^Ops$HasValidTimeBounds tx-op from]
  (.startingFrom tx-op (expect-instant from)))

(defn until [^Ops$HasValidTimeBounds tx-op until]
  (.until tx-op (expect-instant until)))

(defn erase [table id]
  (Ops/erase (expect-table-name table) (expect-eid id)))

(defn call [f & args]
  (Ops/call (expect-fn-id f) args))

(defn sql-op [sql]
  (if-not (string? sql)
    (throw (err/illegal-arg :xtdb.tx/expected-sql
                            {::err/message "Expected SQL query",
                             :sql sql}))

    (Ops/sql sql)))

(defn with-op-args [^Ops$HasArgs op & args]
  (.withArgs op ^List args))

(defn with-op-arg-rows [^Ops$HasArgs op arg-rows]
  (.withArgs op ^List arg-rows))

(defn insert-into [table-name xtql-query]
  (Ops/xtql (list 'insert table-name xtql-query)))

(defn update-table
  #_{:clj-kondo/ignore [:unused-binding]}
  [table-name {:keys [bind set] :as opts} & unify-clauses]

  (Ops/xtql (list* 'update table-name opts unify-clauses)))

(defn delete-from [table-name bind-or-opts & unify-clauses]
  (Ops/xtql (list* 'delete table-name bind-or-opts unify-clauses)))

(defn erase-from [table-name bind-or-opts & unify-clauses]
  (Ops/xtql (list* 'erase table-name bind-or-opts unify-clauses)))

(defn assert-exists [xtql-query]
  (Ops/xtql (list 'assert-exists xtql-query)))

(defn assert-not-exists [xtql-query]
  (Ops/xtql (list 'assert-not-exists xtql-query)))

(defmacro template
  "This macro quotes the given query, but additionally allows you to use Clojure's unquote (`~`) and unquote-splicing (`~@`) forms within the quoted form.

  Usage:

  (defn build-posts-query [{:keys [with-author?]}]
    (xt/template (from :posts [{:xt/id id} text
                               ~@(when with-author?
                                   '[author])])))"

  {:clj-kondo/ignore [:unresolved-symbol :unresolved-namespace]}
  [query]

  (backtick/quote-fn query))
