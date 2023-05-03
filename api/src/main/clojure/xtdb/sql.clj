(ns xtdb.sql
  (:require xtdb.api
            [xtdb.api.impl :as impl])
  (:import xtdb.IResultSet
           java.util.function.Function))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q&
  "asynchronously query an XTDB node.
  q param is a SQL query in string form

  (q node
    \"SELECT foo.id, foo.v FROM foo\")

  Please see SQL query language docs for more details.

  This function returns the results of its query as a vector of maps

  Transaction Basis:

  In XTDB there are a number of ways to control at what point in time a query is ran,
  this is done via a basis map optionally supplied as part of the opts map.

  In the case a basis is not provided the query is guaranteed to run sometime after
  the latest transaction submitted by this connection/node.

  Alternatively a basis map containing reference to a specific transaction can be supplied,
  in this case the query will be run exactly at that transaction, ensuring the repeatability of queries.

  This tx reference (known as a TransactionInstant) is the same map returned by submit-tx

  (q node [query params*] {:basis {:tx tx}})

  Additionally a Basis Timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q node [query params*] {:basis-timeout (Duration/ofSeconds 1)})"
  ^java.util.concurrent.CompletableFuture
  ([node sql+params] (q& node sql+params {}))

  ([node sql+params opts]
   (-> (impl/open-sql& node sql+params (into {:default-all-valid-time? true}
                                             (update opts :basis impl/after-latest-submitted-tx node)))
       (.thenApply
        (reify Function
          (apply [_ res]
            (with-open [^IResultSet res res]
              (vec (iterator-seq res)))))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q
  "query an XTDB node.
  sql+params argument is a vector of the SQL query in string form and any required params.

  (q node [\"SELECT foo.id, foo.v FROM foo WHERE foo.id = ?\" foo-id])

  Please see SQL query language docs for more details.

  This function returns the results of its query as a vector of maps

  Transaction Basis:

  In XTDB there are a number of ways to control at what point in time a query is ran,
  this is done via a basis map optionally supplied as part of the opts map.

  In the case a basis is not provided the query is guaranteed to run sometime after
  the latest transaction submitted by this connection/node.

  Alternatively a basis map containing reference to a specific transaction can be supplied,
  in this case the query will be run exactly at that transaction, ensuring the repeatability of queries.

  This tx reference (known as a TransactionInstant) is the same map returned by submit-tx

  (q node
     query
     {:basis {:tx tx}})

  Additionally a Basis Timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q node sql+params {:basis-timeout (Duration/ofSeconds 1)})"
  ([node sql+params] (q node sql+params {}))
  ([node sql+params opts]
   (-> @(q& node sql+params opts)
       (impl/rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx&
  "Writes transactions to the log for processing. Non Blocking.
  tx-ops is a sequence of vectors, each containing a SQL statement string and any required params.

  [[:sql [\"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\" 0 1]]
   [:sql-batch [\"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\" [2 3] [4 5] [6 7]]]
   [:sql \"UPDATE foo SET b = 1\"]]

  Returns a CompleteableFuture containing a map with details about the submitted transaction,
  including system-time and tx-id."
  (^java.util.concurrent.CompletableFuture [node tx-ops] (submit-tx& node tx-ops {}))
  (^java.util.concurrent.CompletableFuture [node tx-ops tx-opts]
   (impl/submit-tx& node tx-ops (into {:default-all-valid-time? true} tx-opts))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx
  "Writes transactions to the log for processing
  tx-ops is a sequence of vectors, each containing a SQL statement string and any required params.

  [[:sql [\"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\" 0 1]]
   [:sql-batch [\"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\" [2 3] [4 5] [6 7]]]
   [:sql \"UPDATE foo SET b = 1\"]]

  Returns a map with details about the submitted transaction,
  including system-time and tx-id."
  (^xtdb.api.TransactionInstant [node tx-ops] (submit-tx node tx-ops {}))
  (^xtdb.api.TransactionInstant [node tx-ops tx-opts]
   (-> @(submit-tx& node tx-ops tx-opts)
       (impl/rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn status
  "Returns the status of this node as a map,
  including details of both the latest submitted and completed tx"
  [node]
  (impl/status node))
