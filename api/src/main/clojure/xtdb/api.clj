(ns xtdb.api
  "This namespace is the main public Clojure API to XTDB.

  It lives in the `com.xtdb/xtdb-api` artifact - include this in your dependency manager of choice.

  For an in-process node, you will additionally need `xtdb.node`, in the `com.xtdb/xtdb-core` artifact.

  For a remote node, connect using the `xtdb.api/client` function."

  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [xtdb.backtick :as backtick]
            [xtdb.basis :as basis]
            [xtdb.error :as err]
            [xtdb.next.jdbc :as xt-jdbc]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops])
  (:import (clojure.lang IReduceInit)
           (java.io Writer)
           (java.sql BatchUpdateException Connection)
           [java.util.concurrent.atomic AtomicReference]
           (org.apache.arrow.adbc.core AdbcConnection AdbcDatabase)
           (xtdb.api DataSource DataSource$ConnectionBuilder TransactionKey)
           xtdb.types.ClojureForm
           xtdb.util.NormalForm))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->ClojureForm [form]
  (ClojureForm. form))

(defmethod print-dup ClojureForm [^ClojureForm clj-form ^Writer w]
  (.write w "#xt/clj-form ")
  (print-method (.form clj-form) w))

(defmethod print-method ClojureForm [clj-form w]
  (print-dup clj-form w))

(defn- open-ds-conn ^Connection [^DataSource ds {:keys [database]}]
  (-> (.createConnectionBuilder ds)
      (cond-> database (.database (cond-> database
                                    (keyword? database) (-> symbol str NormalForm/normalForm))))
      (.build)))

(defn- parse-tx-ops [ops]
  (mapv tx-ops/parse-tx-op ops))

(defn- begin-ro-sql [{:keys [default-tz await-token snapshot-token snapshot-time current-time]}]
  (let [kvs (->> [["TIMEZONE = ?" (some-> default-tz str)]
                  ["SNAPSHOT_TOKEN = ?" snapshot-token]
                  ["SNAPSHOT_TIME = ?" snapshot-time]
                  ["CLOCK_TIME = ?" current-time]
                  ["AWAIT_TOKEN = ?" await-token]]
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

(defn- ->jdbc-plan-q
  "A read-only reducible over an open JDBC connection. `open-conn!` returns `[conn close?]` so the
  connection's lifetime spans the whole reduce (opened lazily, closed after) — the connection can't
  be opened up-front and handed over, or it'd close before the reducible is reduced."
  ^clojure.lang.IReduceInit [open-conn! sql args opts]
  (reify IReduceInit
    (reduce [_ f start]
      (let [[^Connection conn close?] (open-conn!)]
        (try
          (jdbc/execute! conn (begin-ro-sql opts))
          (try
            (err/wrap-anomaly {:sql sql}
              (->> (jdbc/plan conn (into [sql] args)
                              {:builder-fn xt-jdbc/builder-fn
                               ::xt-jdbc/key-fn (:key-fn opts :kebab-case-keyword)})
                   (transduce (map #(into {} %)) (completing f) start)))
            (finally
              (jdbc/execute! conn ["ROLLBACK"])))
          (finally
            (when close? (.close conn))))))))

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
                 :else (throw (err/incorrect :unknown-query-type "Unknown query type"
                                             {:query query, :type (type query)})))]
     (xtp/-plan-q connectable query args opts))))

(defn q
  "query an XTDB node/connection.

  - query: either an XTQL or SQL query.
  - opts:
    - `:snapshot-token`: see 'Transaction Basis'
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
  this is done via a snapshot-token basis optionally supplied as part of the query map.

  In the case a basis is not provided the query is guaranteed to run sometime after
  the latest transaction submitted by this connection/node.

  Alternatively a specific snapshot-token can be supplied,
  in this case the query will be run exactly at that basis, ensuring the repeatability of queries.

  Providing a `:snapshot-time` sets an additional upper bound on the transactions visible to the query -
  transactions will not be visible if they are after either the snapshot-token or the snapshot-time.

  (q conn '(from ...)
     {:snapshot-token \"ChAKBHh0ZGISCAoGCIDCr/AF\"}))"
  ([node query] (q node query {}))

  ([node query opts]
   (into [] (plan-q node query opts))))

(defn- begin-rw-sql [{:keys [system-time default-tz async? metadata]}]
  (let [kvs (->> [["TIMEZONE = ?" (some-> default-tz str)]
                  ["SYSTEM_TIME = ?" system-time]
                  ["METADATA = ?" metadata]]
                 (into [] (filter (comp some? second))))]
    (into [(format "BEGIN READ WRITE WITH (%s, ASYNC = %s)"
                   (str/join ", " (map first kvs))
                   (boolean async?))]
          (map second)
          kvs)))

(defn- for-valid-time-sql [valid-from valid-to]
  (if (or valid-from valid-to)
    "FOR VALID_TIME FROM ? TO ?"
    ""))

(defn- put-docs! [{:keys [table-name docs valid-from valid-to]} conn]
  (let [docs (cond->> docs
               (or valid-from valid-to)
               (map (partial into (->> {:xt/valid-from valid-from, :xt/valid-to valid-to}
                                       (into {} (remove (comp nil? val)))))))
        copy-in (xt-jdbc/copy-in conn (format "COPY \"%s\".\"%s\" FROM STDIN WITH (FORMAT 'transit-json')"
                                              (namespace table-name) (name table-name)))
        bytes (serde/write-transit-seq docs :json)]
    (.writeToCopy copy-in bytes 0 (alength bytes))
    (.endCopy copy-in)))

(defn- patch-docs! [{:keys [table-name docs valid-from valid-to]} conn]
  (with-open [stmt (jdbc/prepare conn [(format "PATCH INTO \"%s\".\"%s\" %s RECORDS ?"
                                               (namespace table-name) (name table-name)
                                               (for-valid-time-sql valid-from valid-to))])]
    (jdbc/execute-batch! stmt (mapv (fn [doc]
                                      (if (or valid-from valid-to)
                                        [valid-from valid-to doc]
                                        [doc]))
                                    docs))))

(defn- delete-docs! [{:keys [table-name doc-ids valid-from valid-to]} conn]
  (with-open [stmt (jdbc/prepare conn [(format "DELETE FROM \"%s\".\"%s\" %s WHERE _id = ?"
                                               (namespace table-name) (name table-name)
                                               (for-valid-time-sql valid-from valid-to))])]
    (jdbc/execute-batch! stmt (mapv (fn [doc-id]
                                      (if (or valid-from valid-to)
                                        [valid-from valid-to doc-id]
                                        [doc-id]))
                                    doc-ids))))

(defn- erase-docs! [{:keys [table-name doc-ids]} conn]
  (with-open [stmt (jdbc/prepare conn [(format "ERASE FROM \"%s\".\"%s\" WHERE _id = ?"
                                               (namespace table-name) (name table-name))])]
    (jdbc/execute-batch! stmt (mapv vector doc-ids))))

(defn- sql! [{:keys [sql arg-rows]} conn]
  (err/wrap-anomaly {:sql sql, :arg-rows arg-rows}
    (cond
      (nil? arg-rows) (jdbc/execute! conn [sql])
      (empty? arg-rows) nil
      (= 1 (count arg-rows)) (jdbc/execute! conn (into [sql] (first arg-rows)))
      :else (with-open [stmt (jdbc/prepare conn [sql])]
              (jdbc/execute-batch! stmt arg-rows)))))

(defn- execute-op! [{:keys [op] :as tx-op} conn]
  (case op
    :put-docs (put-docs! tx-op conn)
    :patch-docs (patch-docs! tx-op conn)
    :delete-docs (delete-docs! tx-op conn)
    :erase-docs (erase-docs! tx-op conn)
    :sql (sql! tx-op conn)))

(defn- submit-tx* [conn tx-ops tx-opts]
  (try
    (err/wrap-anomaly {}
      (jdbc/execute! conn (begin-rw-sql tx-opts))
      (try
        (doseq [tx-op tx-ops]
          (execute-op! tx-op conn))
        (catch BatchUpdateException e
          (throw (ex-cause e))))

      (jdbc/execute! conn ["COMMIT"]))

    (catch Exception e
      (try
        (jdbc/execute! conn ["ROLLBACK"])
        (catch Throwable t
          (throw (doto e (.addSuppressed t)))))
      (throw e)))

  (jdbc/execute-one! conn ["SHOW LATEST_SUBMITTED_TX"]
                     {:builder-fn xt-jdbc/builder-fn}))

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

  Returns a map:
   - :tx-id (long)
     transaction ID of the submitted transaction

  opts (map):
   - :database (keyword/string)
     the database to execute the transaction on, defaults to `:xtdb`
     throws if the database does not exist, or if a Connection is provided (because the database is already selected)

     The transaction resolves table names only within this database - unlike a query, which may name another:
       - `a.b` is schema `a` of this database, never a database,
         so `INSERT INTO other_db.foo` writes to schema `other_db` here
       - a table in another database is unreachable;
         referring to one reports that the table was not found

   - :system-time
     overrides system-time for the transaction,
     mustn't be earlier than any previous system-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId

   - :metadata (v2.1+)
     attaches arbitrary metadata to the transaction.
     This is then added to the `xt.txs` table in the `user_metadata` column.
     For example, you might use this to attach upstream request IDs, correlation IDs, or other data lineage information."
  ([connectable, tx-ops] (submit-tx connectable tx-ops {}))

  ([connectable, tx-ops tx-opts]
   (xtp/-submit-tx connectable (parse-tx-ops tx-ops) tx-opts)))

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

  connectable: either
    - a javax.sql.Connection that is connected to an XTDB node

      e.g.
        ```clojure
        (with-open [node ...
                    conn (jdbc/get-connection node)]
          (xt/execute-tx conn ...))
        ````

      Use this if you are batch-submitting transactions, to avoid the overhead of opening a new connection for each transaction.

    - or a javax.sql.DataSource (e.g. an XTDB node returned from `xt/client` or `xtdb.node/start-node`), in which case a temporary connection will be created and used.

  opts (map):
   - :database (keyword/string)
     the database to execute the transaction on, defaults to `:xtdb`
     throws if the database does not exist or if a Connection is provided (because the database is already selected)

     The transaction resolves table names only within this database - unlike a query, which may name another:
       - `a.b` is schema `a` of this database, never a database,
         so `INSERT INTO other_db.foo` writes to schema `other_db` here
       - a table in another database is unreachable;
         referring to one reports that the table was not found

   - :system-time
     overrides system-time for the transaction,
     mustn't be earlier than any previous system-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId

   - :metadata (v2.1+)
     attaches arbitrary metadata to the transaction.
     This is then added to the `xt.txs` table in the `user_metadata` column.
     For example, you might use this to attach upstream request IDs, correlation IDs, or other data lineage information.

   - :authn
     a map of user and password if the node requires authentication"

  (^TransactionKey [connectable, tx-ops] (execute-tx connectable tx-ops {}))

  (^TransactionKey [connectable, tx-ops tx-opts]
   (xtp/-execute-tx connectable (parse-tx-ops tx-ops) tx-opts)))

(defn client
  "Open up a client to a (possibly) remote XTDB node

    * `:host`: the hostname or IP address of the database (default: `127.0.0.1`)
    * `:port`: the port for the database connection (default: `5432`)

    * `:user`: the username to authenticate with
    * `:password`: the password to authenticate with
    * `:dbname`: the database to connect to (default: `xtdb`)

   See `next.jdbc/get-datasource` for more options."
  ^javax.sql.DataSource [{:keys [host port user password dbname]
                          :or {host "127.0.0.1"
                               port 5432
                               dbname "xtdb"}}]

  (let [!await-token (AtomicReference.)]
    (reify DataSource
      (getAwaitToken [_] (.get !await-token))
      (setAwaitToken [_ await-token]
        (loop []
          (let [old-token (.get !await-token)]
            (when (or (nil? old-token) await-token)
              (when-not (.compareAndSet !await-token old-token (basis/merge-tx-tokens old-token await-token))
                (recur))))))

      (createConnectionBuilder [_]
        (DataSource$ConnectionBuilder. host port user password dbname)))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-adbc-conn
  "Opens an in-process ADBC connection to `db` (an XTDB node). The returned connection can be passed
  to `q`/`submit-tx`/… to run in-process, bypassing pgwire; the caller must close it."
  ^AdbcConnection [^AdbcDatabase db]
  (.connect db))

(defn status
  "Returns the status of this node as a map"
  [connectable]
  (xtp/-status connectable))

;; -- JDBC/pgwire protocol impls --
;;
;; The real work is on `java.sql.Connection` (an already-open connection). `DataSource` and the
;; catch-all `Object` (an arbitrary next.jdbc connectable) adapt by opening a connection and
;; delegating; `DataSource` additionally threads its await-token. The in-process `Xtdb.Connection`
;; impl lives in `xtdb.node.impl`.

(extend-protocol xtp/Connectable
  Connection
  (-plan-q [conn sql args opts]
    (xtp/check-no-database conn opts)
    (->jdbc-plan-q (fn [] [conn false]) sql args opts))
  (-submit-tx [conn tx-ops opts]
    (xtp/check-no-database conn opts)
    {:tx-id (:tx-id (submit-tx* conn tx-ops (assoc opts :async? true)))})
  (-execute-tx [conn tx-ops opts]
    (xtp/check-no-database conn opts)
    (let [{:keys [tx-id system-time]} (submit-tx* conn tx-ops (assoc opts :async? false))]
      (serde/->TxKey tx-id (time/->instant system-time))))
  (-status [conn] (xtp/build-status conn))

  DataSource
  (-plan-q [ds sql args opts]
    (->jdbc-plan-q (fn [] [(open-ds-conn ds opts) true])
                   sql args
                   (update opts :await-token (fnil identity (.getAwaitToken ds)))))
  (-submit-tx [ds tx-ops opts]
    (with-open [conn (open-ds-conn ds opts)]
      (let [{:keys [tx-id await-token]} (submit-tx* conn tx-ops (assoc opts :async? true))]
        (.setAwaitToken ds await-token)
        {:tx-id tx-id})))
  (-execute-tx [ds tx-ops opts]
    (with-open [conn (open-ds-conn ds opts)]
      (let [{:keys [tx-id system-time await-token]} (submit-tx* conn tx-ops (assoc opts :async? false))]
        (.setAwaitToken ds await-token)
        (serde/->TxKey tx-id (time/->instant system-time)))))
  (-status [ds]
    (with-open [conn (open-ds-conn ds {})]
      (xtp/-status conn)))

  Object
  (-plan-q [connectable sql args opts]
    (xtp/check-no-database connectable opts)
    (->jdbc-plan-q (fn [] [(jdbc/get-connection connectable) true]) sql args opts))
  (-submit-tx [connectable tx-ops opts]
    (xtp/check-no-database connectable opts)
    (with-open [^Connection conn (jdbc/get-connection connectable)]
      {:tx-id (:tx-id (submit-tx* conn tx-ops (assoc opts :async? true)))}))
  (-execute-tx [connectable tx-ops opts]
    (xtp/check-no-database connectable opts)
    (with-open [^Connection conn (jdbc/get-connection connectable)]
      (let [{:keys [tx-id system-time]} (submit-tx* conn tx-ops (assoc opts :async? false))]
        (serde/->TxKey tx-id (time/->instant system-time)))))
  (-status [connectable]
    (xtp/check-no-database connectable {})
    (with-open [^Connection conn (jdbc/get-connection connectable)]
      (xtp/-status conn))))

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
