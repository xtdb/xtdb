(ns xtdb.api
  "This namespace is the main public Clojure API to XTDB.

  It lives in the `com.xtdb/xtdb-api` artifact - include this in your dependency manager of choice.

  To start a node, you will additionally need:

  * `xtdb.node`, for an in-process node.
  * `xtdb.client`, for a remote client."

  (:require [clojure.spec.alpha :as s]
            [xtdb.backtick :as backtick]
            [xtdb.error :as err]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.xtql.edn :as xtql.edn])
  (:import (java.io Writer)
           java.util.concurrent.ExecutionException
           java.util.function.Function
           java.util.List
           java.util.Map
           [java.util.stream Stream]
           (xtdb.api IXtdb IXtdbSubmitClient TransactionKey)
           (xtdb.api.query Basis QueryOptions XtqlQuery)
           (xtdb.api.tx TxOp TxOptions)
           xtdb.types.ClojureForm))

(defmacro ^:private rethrowing-cause [form]
  `(try
     ~form
     (catch ExecutionException e#
       (throw (.getCause e#)))))

(defn- expect-instant [instant]
  (when-not (s/valid? ::time/datetime-value instant)
    (throw (err/illegal-arg :xtdb/invalid-date-time
                            {::err/message "expected date-time"
                             :timestamp instant})))

  (time/->instant instant))

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

  This function returns a CompletableFuture containing the results of its query as a vector of maps

  Transaction Basis:

  In XTDB there are a number of ways to control at what point in time a query is run -
  this is done via a basis map optionally supplied as part of the query map.

  In the case a basis is not provided the query is guaranteed to run sometime after
  the latest transaction submitted by this connection/node.

  Alternatively a basis map containing reference to a specific transaction can be supplied,
  in this case the query will be run exactly at that transaction, ensuring the repeatability of queries.

  This tx key is the same map returned by `submit-tx`.

  (q& node '(from ...)
      {:basis {:at-tx tx}})

  Additionally a tx timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q& node '(from ...)
      {:basis-timeout (Duration/ofSeconds 1)})"

  (^java.util.concurrent.CompletableFuture [node q+args] (q& node q+args {}))

  (^java.util.concurrent.CompletableFuture
   [node query opts]
   (let [opts (-> (into {:default-all-valid-time? false} opts)
                  (time/after-latest-submitted-tx node))]

     (-> (xtp/open-query& node query opts)
         (.thenApply
          (reify Function
            (apply [_ res]
              (with-open [^Stream res res]
                (vec (.toList res))))))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q
  "query an XTDB node.

  - query: either an XTQL or SQL query.
  - opts:
    - `:basis`: see 'Transaction Basis'
    - `:args`: arguments to pass to the query.

  For example:

  (q node '(from ...))

  (q node '(from :foo [{:a $a, :b $b}])
      {:a a-value, :b b-value})

  (q node \"SELECT foo.id, foo.v FROM foo WHERE foo.id = 'my-foo'\")
  (q node \"SELECT foo.id, foo.v FROM foo WHERE foo.id = ?\" {:args [foo-id]})

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
     {:basis {:at-tx tx}})

  Additionally a Basis Timeout can be supplied to the query map, which if after the specified duration
  the query's requested basis is not complete the query will be cancelled.

  (q node '(from ...)
     {:tx-timeout (Duration/ofSeconds 1)})"
  ([node q+args]
   (-> @(q& node q+args)
       (rethrowing-cause)))

  ([node q+args opts]
   (-> @(q& node q+args opts)
       (rethrowing-cause))))

(extend-protocol xtp/PNode
  IXtdb
  (open-query& [this query {:keys [args after-tx basis tx-timeout default-tz default-all-valid-time? explain? key-fn], :or {key-fn :kebab-case-keyword}}]
    (let [query-opts (-> (QueryOptions/queryOpts)
                         (cond-> (map? args) (.args ^Map args)
                                 (sequential? args) (.args ^List args)
                                 after-tx (.afterTx after-tx)
                                 basis (.basis (Basis. (:at-tx basis) (:current-time basis)))
                                 default-tz (.defaultTz default-tz)
                                 tx-timeout (.txTimeout tx-timeout)
                                 (some? default-all-valid-time?) (.defaultAllValidTime default-all-valid-time?)
                                 (some? explain?) (.explain explain?))

                         (.keyFn (serde/read-key-fn key-fn))

                         (.build))]
      (if (string? query)
        (.openQueryAsync this ^String query query-opts)

        (let [^XtqlQuery query (cond
                                 (instance? XtqlQuery query) query
                                 (seq? query) (xtql.edn/parse-query query)
                                 :else (throw (err/illegal-arg :unknown-query-type {:query query, :type (type query)})))]
          (.openQueryAsync this query query-opts))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx&
  "Writes transactions to the log for processing. Non-blocking.

  tx-ops: XTQL/SQL transaction operations.
    [[:put :table {:xt/id \"my-id\", ...}]
     [:delete-docs :table \"my-id\"]

     [:sql \"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\"
      [0 1]]

     [:sql \"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\"
      [2 3] [4 5] [6 7]]

     [:sql \"UPDATE foo SET b = 1\"]]

  Returns a CompletableFuture containing a map with details about
  the submitted transaction, including system-time and tx-id.

  opts (map):
   - :system-time
     overrides system-time for the transaction,
     mustn't be earlier than any previous system-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId"
  (^java.util.concurrent.CompletableFuture [node tx-ops] (submit-tx& node tx-ops {}))
  (^java.util.concurrent.CompletableFuture [^IXtdbSubmitClient node tx-ops tx-opts]
   (.submitTxAsync node
                   (cond
                     (instance? TxOptions tx-opts) tx-opts
                     (nil? tx-opts) (TxOptions.)
                     (map? tx-opts) (let [{:keys [system-time default-tz default-all-valid-time?]} tx-opts]
                                      (TxOptions. (some-> system-time expect-instant)
                                                  default-tz
                                                  (boolean default-all-valid-time?))))
                   (->> (for [tx-op tx-ops]
                          (cond-> tx-op
                            (not (instance? TxOp tx-op)) tx-ops/parse-tx-op))
                        (into-array TxOp)))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx
  "Writes transactions to the log for processing

  tx-ops: XTQL/SQL style transactions.
    [[:put :table {:xt/id \"my-id\", ...}]
     [:delete-docs :table \"my-id\"]

     [:sql \"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\"
      [0 1]]

     [:sql \"INSERT INTO foo (xt$id, a, b) VALUES ('foo', ?, ?)\"
      [2 3] [4 5] [6 7]]

     [:sql \"UPDATE foo SET b = 1\"]]

  Returns a map with details about the submitted transaction, including system-time and tx-id.

  opts (map):
   - :system-time
     overrides system-time for the transaction,
     mustn't be earlier than any previous system-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId"

  (^TransactionKey [node tx-ops] (submit-tx node tx-ops {}))
  (^TransactionKey [node tx-ops tx-opts]
   (-> @(submit-tx& node tx-ops tx-opts)
       (rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn status
  "Returns the status of this node as a map,
  including details of both the latest submitted and completed tx"
  [node]
  (xtp/status node))

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
