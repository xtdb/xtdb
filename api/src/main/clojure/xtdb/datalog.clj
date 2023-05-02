(ns xtdb.datalog
  (:require xtdb.api
            [xtdb.api.impl :as impl])
  (:import xtdb.IResultSet
           java.util.function.Function))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q&
  "asynchronously query an XTDB node.
  q param is a Datalog query in map form, args should line
  up with those specified in the query.

  Please see Datalog query language docs for more details.

  This function returns a CompleteableFuture containing the results of its query as a vector of maps

  Transaction Basis:

  In XTDB there are a number of ways to control at what point in time a query is ran,
  this is done via a basis map optionally supplied as part of the query map.

  In the case a basis is not provided the query is guaranteed to run sometime after
  the latest transaction submitted by this connection/node.

  Alternatively a basis map containing reference to a specific transaction can be supplied,
  in this case the query will be run exactly at that transaction, ensuring the repeatability of queries.

  This tx reference (known as a TransactionInstant) is the same map returned by submit-tx

  (q node
    (-> '{:find ...
          :where ...}
        (assoc :basis {:tx tx})))

  Additionally a Basis Timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q node
    (-> '{:find ...
          :where ...}
        (assoc :basis-timeout (Duration/ofSeconds 1))))"
  ^java.util.concurrent.CompletableFuture
  [node q & args]
  (-> (impl/open-datalog& node (into {:default-all-valid-time? false}
                                     (-> q (update :basis impl/after-latest-submitted-tx node)))
                          args)
      (.thenApply
       (reify Function
         (apply [_ res]
           (with-open [^IResultSet res res]
             (vec (iterator-seq res))))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q
  "query an XTDB node.
  q param is a Datalog query in map form, args should line
  up with those specified in the query.

  Please see Datalog query language docs for more details.

  This function returns the results of its query as a vector of maps

  Transaction Basis:

  In XTDB there are a number of ways to control at what point in time a query is ran,
  this is done via a basis map optionally supplied as part of the query map.

  In the case a basis is not provided the query is guaranteed to run sometime after
  the latest transaction submitted by this connection/node.

  Alternatively a basis map containing reference to a specific transaction can be supplied,
  in this case the query will be run exactly at that transaction, ensuring the repeatability of queries.

  This tx reference (known as a TransactionInstant) is the same map returned by submit-tx

  (q node
    (-> '{:find ...
          :where ...}
        (assoc :basis {:tx tx})))

  Additionally a Basis Timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q node
    (-> '{:find ...
          :where ...}
        (assoc :basis-timeout (Duration/ofSeconds 1))))"
  [node q & args]
  (-> @(apply q& node q args)
      (impl/rethrowing-cause)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx&
  "Writes transactions to the log for processing. Non-blocking.
  tx-ops Datalog style transactions.
  Returns a CompleteableFuture containing a map with details about
  the submitted transaction, including sys-time and tx-id.

  opts (map):
   - :sys-time
     overrides sys-time for the transaction,
     mustn't be earlier than any previous sys-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId"
  (^java.util.concurrent.CompletableFuture [node tx-ops] (submit-tx& node tx-ops {}))
  (^java.util.concurrent.CompletableFuture [node tx-ops tx-opts]
   (impl/submit-tx& node tx-ops (into {:default-all-valid-time? false} tx-opts))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx
  "Writes transactions to the log for processing
  tx-ops Datalog style transactions.
  Returns a map with details about the submitted transaction,
  including sys-time and tx-id.

  opts (map):
   - :sys-time
     overrides sys-time for the transaction,
     mustn't be earlier than any previous sys-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId"
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
