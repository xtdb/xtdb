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

  (q node
     query
     {:basis {:tx tx}})

  Additionally a Basis Timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q node
     query
     {:basis-timeout (Duration/ofSeconds 1)})"
  ^java.util.concurrent.CompletableFuture
  ([node q] (q& node q {}))

  ([node q opts]
   (-> (impl/open-sql& node q (into {:default-all-app-time? true}
                                    (update opts :basis impl/after-latest-submitted-tx node)))
       (.thenApply
        (reify Function
          (apply [_ res]
            (with-open [^IResultSet res res]
              (vec (iterator-seq res)))))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q
  "query an XTDB node.
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

  (q node
     query
     {:basis {:tx tx}})

  Additionally a Basis Timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q node
     query
     {:basis-timeout (Duration/ofSeconds 1)})"
  ([node sql] (q node sql {}))
  ([node sql opts]
   (-> @(q& node sql opts)
       (impl/rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx&
  "Writes transactions to the log for processing. Non Blocking.
  tx-ops is a sequence of SQL statement strings submitted as :sql operations

  [[:sql \"INSERT INTO foo (xt$id, v) VALUES ('foo', 0)\"]
   [:sql \"UPDATE foo SET v = 1\"]]

  Returns a CompleteableFuture containing a map with details about the submitted transaction,
  including sys-time and tx-id."
  (^java.util.concurrent.CompletableFuture [node tx-ops] (submit-tx& node tx-ops {}))
  (^java.util.concurrent.CompletableFuture [node tx-ops tx-opts]
   (impl/submit-tx& node tx-ops (into {:default-all-app-time? true} tx-opts))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx
  "Writes transactions to the log for processing
  tx-ops is a sequence of SQL statement strings submitted as :sql operations

  [[:sql \"INSERT INTO foo (xt$id, v) VALUES ('foo', 0)\"]
   [:sql \"UPDATE foo SET v = 1\"]]

  Returns a map with details about the submitted transaction,
  including sys-time and tx-id."
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
