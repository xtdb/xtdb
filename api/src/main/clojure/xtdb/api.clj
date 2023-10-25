(ns xtdb.api
  (:require [xtdb.api.protocols :as xtp])
  (:import java.util.concurrent.ExecutionException
           java.util.function.Function
           xtdb.IResultSet))

(defmacro ^:private rethrowing-cause [form]
  `(try
     ~form
     (catch ExecutionException e#
       (throw (.getCause e#)))))

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

  This tx reference (known as a TransactionInstant) is the same map returned by submit-tx

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
    [[:put :table {:xt/id \"my-id\", ...}]
     [:delete :table \"my-id\"]]

    [[:sql [\"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\" 0 1]]
     [:sql-batch [\"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\" [2 3] [4 5] [6 7]]]
     [:sql \"UPDATE foo SET b = 1\"]]

  Returns a map with details about the submitted transaction,
  including system-time and tx-id.

  opts (map):
   - :system-time
     overrides system-time for the transaction,
     mustn't be earlier than any previous system-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId"
  (^xtdb.api.protocols.TransactionInstant [node tx-ops] (submit-tx node tx-ops {}))
  (^xtdb.api.protocols.TransactionInstant [node tx-ops tx-opts]
   (-> @(submit-tx& node tx-ops tx-opts)
       (rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn status
  "Returns the status of this node as a map,
  including details of both the latest submitted and completed tx"
  [node]
  (xtp/status node))
