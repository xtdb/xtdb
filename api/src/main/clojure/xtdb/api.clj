(ns xtdb.api
  (:require [xtdb.protocols :as xtp])
  (:import [java.io Writer]
           [java.time Instant]
           java.util.concurrent.ExecutionException
           java.util.function.Function
           xtdb.IResultSet
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

  q+args param is either a Datalog/SQL query, or a vector containing a Datalog/SQL query
  and args that line up with those specified in the query.

  (q& node
      '{:find ...
        :where ...})

  (q& node
      ['{:find ...
         :in [a b]
         :where ...}
       a-value b-value])

  (q& node \"SELECT foo.id, foo.v FROM foo WHERE foo.id = 'my-foo'\")
  (q& node [\"SELECT foo.id, foo.v FROM foo WHERE foo.id = ?\" foo-id])

  Please see Datalog/SQL query language docs for more details.

  This function returns a CompleteableFuture containing the results of its query as a vector of maps

  Transaction Basis:

  In XTDB there are a number of ways to control at what point in time a query is run -
  this is done via a basis map optionally supplied as part of the query map.

  In the case a basis is not provided the query is guaranteed to run sometime after
  the latest transaction submitted by this connection/node.

  Alternatively a basis map containing reference to a specific transaction can be supplied,
  in this case the query will be run exactly at that transaction, ensuring the repeatability of queries.

  This tx key is the same map returned by submit-tx

  (q& node
      '{:find ...
        :where ...}
      {:basis {:tx tx}})

  Additionally a basis timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q& node
      '{:find ...
        :where ...}
      {:basis-timeout (Duration/ofSeconds 1)})"
  (^java.util.concurrent.CompletableFuture [node q+args] (q& node q+args {}))

  (^java.util.concurrent.CompletableFuture
   [node q+args opts]
   (let [[q args] (if (vector? q+args)
                    [(first q+args) (subvec q+args 1)]
                    [q+args nil])

         args (or (:args opts) args) ;;TODO XTQL has no support for q+args vec,
         ;; these approaches for should be exclusive.

         opts (-> (into {:default-all-valid-time? false} opts)
                  (assoc :args args)
                  (update :basis xtp/after-latest-submitted-tx node))]

     (-> (xtp/open-query& node q opts)
         (.thenApply
          (reify Function
            (apply [_ res]
              (with-open [^IResultSet res res]
                (vec (iterator-seq res))))))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q
  "query an XTDB node.

  q+args param is either a Datalog/SQL query, or a vector containing a Datalog/SQL query
  and args that line up with those specified in the query.

  (q node
     '{:find ...
       :where ...})

  (q node
     ['{:find ...
        :in [a b]
        :where ...}
      a-value b-value])

  (q node \"SELECT foo.id, foo.v FROM foo WHERE foo.id = 'my-foo'\")
  (q node [\"SELECT foo.id, foo.v FROM foo WHERE foo.id = ?\" foo-id])

  Please see Datalog/SQL query language docs for more details.

  This function returns the results of its query as a vector of maps

  Transaction Basis:

  In XTDB there are a number of ways to control at what point in time a query is run -
  this is done via a basis map optionally supplied as part of the query map.

  In the case a basis is not provided the query is guaranteed to run sometime after
  the latest transaction submitted by this connection/node.

  Alternatively a basis map containing reference to a specific transaction can be supplied,
  in this case the query will be run exactly at that transaction, ensuring the repeatability of queries.

  This tx reference (known as a TransactionKey) is the same map returned by submit-tx

  (q node
     '{:find ...
       :where ...}
     {:basis {:tx tx}})

  Additionally a Basis Timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q node
     '{:find ...
       :where ...}
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

  tx-ops: Datalog/SQL transaction operations.

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

  tx-ops: Datalog/SQL style transactions.
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

(defn put [table doc]
  [:put table doc])

(defn put-fn [fn-id f]
  [:put-fn fn-id f])

(defn delete [table id]
  [:delete table id])

(defn during [tx-op from until]
  (conj tx-op {:for-valid-time [:in from until]}))

(defn starting-from [tx-op from]
  (during tx-op from nil))

(defn until [tx-op until]
  (during tx-op nil until))

(defn erase [table id]
  [:evict table id])

(defn call [f & args]
  (into [:call f] args))

(defn sql-op [sql]
  [:sql sql])

(defn xtql-op [xtql]
  [:xtql xtql])

(defn with-op-args [op & args]
  (into op args))

(defn with-op-arg-rows [op arg-rows]
  (into op arg-rows))
