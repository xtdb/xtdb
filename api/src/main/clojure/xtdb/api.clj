(ns xtdb.api
  "This namespace is the main public Clojure API to XTDB.

  It lives in the `com.xtdb/xtdb-api` artifact - include this in your dependency manager of choice.

  For an in-process node, you will additionally need `xtdb.node`, in the `com.xtdb/xtdb-core` artifact."

  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [xtdb.backtick :as backtick]
            [xtdb.error :as err]
            [xtdb.next.jdbc :as xt-jdbc]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde])
  (:import (clojure.lang IReduceInit)
           (java.io Writer)
           [java.sql Connection]
           org.postgresql.util.PSQLException
           (xtdb.api TransactionKey)
           xtdb.types.ClojureForm))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->ClojureForm [form]
  (ClojureForm. form))

(defmethod print-dup ClojureForm [^ClojureForm clj-form ^Writer w]
  (.write w "#xt/clj-form ")
  (print-method (.form clj-form) w))

(defmethod print-method ClojureForm [clj-form w]
  (print-dup clj-form w))

(defn- begin-ro-sql [{:keys [default-tz after-tx-id snapshot-time current-time]}]
  (let [kvs (->> [["WATERMARK = ?" after-tx-id]
                  ["TIMEZONE = ?" (some-> default-tz str)]
                  ["SNAPSHOT_TIME = ?" snapshot-time]
                  ["CLOCK_TIME = ?" current-time]]
                 (into [] (filter (comp some? second))))]
    (into [(format "BEGIN READ ONLY WITH (%s)"
                   (str/join ", " (map first kvs)))]
          (map second)
          kvs)))

(defn- xtql->sql [xtql]
  (format "XTQL ($$ %s $$ %s)"
          (pr-str xtql)
          (->> (repeat (or (when (seq? xtql)
                             (let [[op params & _] xtql]
                               (when (and (or (= 'fn op) (= 'fn* op))
                                          (vector? params))
                                 (count params))))
                           0)
                       ", ?")
               (str/join ""))))

(defn plan-q
  "General query execution function for controlling the realized result set.

  Returns a reducible that, when reduced (with an initial value), runs the query and yields the result.
  `plan-q` returns an IReduceInit object so you must provide an initial value when calling reduce on it.

  The main use case for `plan-q` is to stream large results sets without having the entire result set in memory.
  A common way to do this is to call run! together with a side-effecting function process-row!
  (which could for example write the row to a file):

  (run! process-row! (xt/plan-q node ...))

  The arguments are the same as for `q`."

  (^clojure.lang.IReduceInit [connectable query+args] (plan-q connectable query+args {}))
  (^clojure.lang.IReduceInit [connectable query+args opts]
   (let [[query args] (if (vector? query+args)
                        [(first query+args) (rest query+args)]
                        [query+args []])
         query (cond
                 (string? query) query
                 (seq? query) (xtql->sql query)
                 :else (throw (err/illegal-arg :unknown-query-type {:query query, :type (type query)})))
         opts (-> opts
                  (update :after-tx-id (fnil identity (xtp/latest-submitted-tx-id connectable))))]
     (reify IReduceInit
       (reduce [_ f start]
         (let [was-conn? (instance? Connection connectable)
               conn (jdbc/get-connection connectable)]
           (try
             (jdbc/execute! conn (begin-ro-sql opts))
             (try
               (->> (jdbc/plan conn (into [query] args)
                               {:builder-fn xt-jdbc/builder-fn
                                ::xt-jdbc/key-fn (:key-fn opts :kebab-case-keyword)})
                    (transduce (map #(into {} %)) (completing f) start))
               (catch PSQLException e
                 (when-let [detail (some-> (.getServerErrorMessage e) .getDetail)]
                   (when-not (str/blank? detail)
                     (throw (serde/read-transit detail :json))))
                 (throw e))
               (finally
                 (jdbc/execute! conn ["ROLLBACK"])))
             (finally
               (when-not was-conn?
                 (.close conn))))))))))

(defn q
  "query an XTDB node/connection.

  - query: either an XTQL or SQL query.
  - opts:
    - `:snapshot-time`: see 'Transaction Basis'
    - `:current-time`: override wall-clock time to use in functions that require it
    - `:default-tz`: overrides the default time zone for the query

  For example:

  (q conn '(from ...))

  (q conn ['(fn [a b] (from :foo [a b])) a-value b-value])
  (q conn ['#(from :foo [{:a %1, :b %2}]) a-value b-value])
  (q conn ['#(from :foo [{:a %} b]) a-value])

  (q conn \"SELECT foo.id, foo.v FROM foo WHERE foo.id = 'my-foo'\")
  (q conn [\"SELECT foo.id, foo.v FROM foo WHERE foo.id = ?\" foo-id])

  Please see XTQL/SQL query language docs for more details.

  This function returns the results of its query as a vector of maps

  Transaction Basis:

  In XTDB there are a number of ways to control at what point in time a query is run -
  this is done via a snapshot-time basis optionally supplied as part of the query map.

  In the case a basis is not provided the query is guaranteed to run sometime after
  the latest transaction submitted by this connection/node.

  Alternatively a specific snapshot-time can be supplied,
  in this case the query will be run exactly at that system-time, ensuring the repeatability of queries.

  (q conn '(from ...)
     {:snapshot-time #inst \"2020-01-02\"}))"
  ([node query] (q node query {}))

  ([node query opts]
   (into [] (plan-q node query opts))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx
  "Writes transactions to the log for processing

  tx-ops: XTQL/SQL style transactions.
    [[:put-docs :table {:xt/id \"my-id\", ...}]
     [:delete-docs :table \"my-id\"]

     [\"INSERT INTO foo (_id, a, b) VALUES ('foo', ?, ?)\" 0 1]

     ;; batches
     [:sql \"INSERT INTO foo (_id, a, b) VALUES ('foo', ?, ?)\"
      [2 3] [4 5] [6 7]]

     \"UPDATE foo SET b = 1\"]

  Returns the tx-id.

  opts (map):
   - :system-time
     overrides system-time for the transaction,
     mustn't be earlier than any previous system-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId

   - :authn
     a map of user and password if the node requires authentication"


  (^TransactionKey [node, tx-ops] (submit-tx node tx-ops {}))
  (^TransactionKey [node, tx-ops tx-opts]
   (xtp/submit-tx node (vec tx-ops) tx-opts)))

(defn execute-tx
  "Executes a transaction; blocks waiting for the receiving node to index it.

  tx-ops: XTQL/SQL style transactions.
    [[:put-docs :table {:xt/id \"my-id\", ...}]
     [:delete-docs :table \"my-id\"]

     [:sql \"INSERT INTO foo (_id, a, b) VALUES ('foo', ?, ?)\"
      [0 1]]

     [:sql \"INSERT INTO foo (_id, a, b) VALUES ('foo', ?, ?)\"
      [2 3] [4 5] [6 7]]

     [:sql \"UPDATE foo SET b = 1\"]]

  If the transaction fails - either due to an error or a failed assert, this function will throw.
  Otherwise, returns a map with details about the submitted transaction, including system-time and tx-id.

  opts (map):
   - :system-time
     overrides system-time for the transaction,
     mustn't be earlier than any previous system-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId

   - :authn
     a map of user and password if the node requires authentication"

  (^TransactionKey [node, tx-ops] (execute-tx node tx-ops {}))
  (^TransactionKey [node, tx-ops tx-opts]
   (let [{:keys [tx-id system-time committed? error]} (xtp/execute-tx node (vec tx-ops) tx-opts)]
     (if committed?
       (serde/->TxKey tx-id system-time)
       (throw (or error (ex-info "Transaction failed." {})))))))

(defn status
  "Returns the status of this node as a map,
  including details of both the latest submitted and completed tx

  Optionally takes a map of options:
  - :auth-opts
    a map of user and password if the node requires authentication"

  ([node] (xtp/status node))
  ([node opts] (xtp/status node opts)))

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
