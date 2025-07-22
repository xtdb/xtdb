(ns xtdb.api
  "This namespace is the main public Clojure API to XTDB.

  It lives in the `com.xtdb/xtdb-api` artifact - include this in your dependency manager of choice.

  For an in-process node, you will additionally need `xtdb.node`, in the `com.xtdb/xtdb-core` artifact.

  For a remote node, connect using the `xtdb.api/client` function."

  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [xtdb.backtick :as backtick]
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
           (xtdb.api DataSource DataSource$ConnectionBuilder TransactionKey)
           (xtdb.api.tx TxOp TxOp$Sql)
           (xtdb.tx_ops DeleteDocs EraseDocs PatchDocs PutDocs)
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

(defn- with-conn* [connectable f]
  (let [was-conn? (instance? Connection connectable)
        ^Connection conn (cond-> connectable
                           (not was-conn?) jdbc/get-connection)]
    (try
      (f conn)
      (finally
        (when-not was-conn?
          (.close conn))))))

(defmacro ^:private with-conn [[binding connectable] & body]
  `(with-conn* ~connectable (fn [~binding] ~@body)))

(defn- begin-ro-sql [{:keys [default-tz await-token snapshot-token current-time]}]
  (let [kvs (->> [["TIMEZONE = ?" (some-> default-tz str)]
                  ["SNAPSHOT_TOKEN = ?" snapshot-token]
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
                                             {:query query, :type (type query)})))

         opts (cond-> opts
                (instance? xtdb.api.DataSource connectable)
                (update :await-token (fnil identity (.getAwaitToken ^xtdb.api.DataSource connectable))))]
     (reify IReduceInit
       (reduce [_ f start]
         (with-conn [conn connectable]
           (jdbc/execute! conn (begin-ro-sql opts))
           (try
             (err/wrap-anomaly {:sql query}
               (->> (jdbc/plan conn (into [query] args)
                               {:builder-fn xt-jdbc/builder-fn
                                ::xt-jdbc/key-fn (:key-fn opts :kebab-case-keyword)})
                    (transduce (map #(into {} %)) (completing f) start)))
             (finally
               (jdbc/execute! conn ["ROLLBACK"])))))))))

(defn q
  "query an XTDB node/connection.

  - query: either an XTQL or SQL query.
  - opts:
    - `:snapshot-token`: see 'Transaction Basis'
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

  (q conn '(from ...)
     {:snapshot-token \"ChAKBHh0ZGISCAoGCIDCr/AF\"}))"
  ([node query] (q node query {}))

  ([node query opts]
   (into [] (plan-q node query opts))))

(defn- begin-rw-sql [{:keys [system-time default-tz async?]}]
  (let [kvs (->> [["TIMEZONE = ?" (some-> default-tz str)]
                  ["SYSTEM_TIME = ?" system-time]]
                 (into [] (filter (comp some? second))))]
    (into [(format "BEGIN READ WRITE WITH (%s, ASYNC = %s)"
                   (str/join ", " (map first kvs))
                   (boolean async?))]
          (map second)
          kvs)))

(extend-protocol xtp/ExecuteOp
  PutDocs
  (execute-op! [{:keys [table-name valid-from valid-to docs]} conn]
    (let [table-name (NormalForm/normalTableName table-name)
          docs (cond->> docs
                 (or valid-from valid-to)
                 (map (partial into (->> {:xt/valid-from valid-from, :xt/valid-to valid-to}
                                         (into {} (remove (comp nil? val)))))))
          copy-in (xt-jdbc/copy-in conn (format "COPY \"%s\".\"%s\" FROM STDIN WITH (FORMAT 'transit-json')"
                                                (namespace table-name) (name table-name)))
          bytes (serde/write-transit-seq docs :json)]
      (.writeToCopy copy-in bytes 0 (alength bytes))
      (.endCopy copy-in)))

  PatchDocs
  (execute-op! [{:keys [table-name valid-from valid-to docs]} conn]
    (let [table-name (NormalForm/normalTableName table-name)]
      (with-open [stmt (jdbc/prepare conn [(format "PATCH INTO \"%s\".\"%s\" %s RECORDS ?"
                                                   (namespace table-name) (name table-name)
                                                   (if (or valid-from valid-to)
                                                     "FOR VALID_TIME FROM ? TO ?"
                                                     ""))])]
        (jdbc/execute-batch! stmt (mapv (fn [doc]
                                          (if (or valid-from valid-to)
                                            [valid-from valid-to doc]
                                            [doc]))
                                        docs)))))

  DeleteDocs
  (execute-op! [{:keys [table-name valid-from valid-to doc-ids]} conn]
    (let [table-name (NormalForm/normalTableName table-name)]
      (with-open [stmt (jdbc/prepare conn [(format "DELETE FROM \"%s\".\"%s\" %s WHERE _id = ?"
                                                   (namespace table-name) (name table-name)
                                                   (if (or valid-from valid-to)
                                                     "FOR VALID_TIME FROM ? TO ?"
                                                     ""))])]
        (jdbc/execute-batch! stmt (mapv (fn [doc-id]
                                          (if (or valid-from valid-to)
                                            [valid-from valid-to doc-id]
                                            [doc-id]))
                                        doc-ids)))))

  EraseDocs
  (execute-op! [{:keys [table-name doc-ids]} conn]
    (let [table-name (NormalForm/normalTableName table-name)]
      (with-open [stmt (jdbc/prepare conn [(format "ERASE FROM \"%s\".\"%s\" WHERE _id = ?"
                                                   (namespace table-name) (name table-name))])]
        (jdbc/execute-batch! stmt (mapv vector doc-ids)))))

  TxOp$Sql
  (execute-op! [op conn]
    (let [sql (.sql op), arg-rows (.argRows op)]
      (err/wrap-anomaly {:sql sql, :arg-rows arg-rows}
        (cond
          (nil? arg-rows) (jdbc/execute! conn [sql])
          (empty? arg-rows) nil
          (= 1 (count arg-rows)) (jdbc/execute! conn (into [sql] (first arg-rows)))
          :else (with-open [stmt (jdbc/prepare conn [sql])]
                  (jdbc/execute-batch! stmt arg-rows)))))))

(defn- submit-tx* [conn tx-ops tx-opts]
  (try
    (err/wrap-anomaly {}
      (jdbc/execute! conn (begin-rw-sql tx-opts))
      (try
        (doseq [tx-op tx-ops
                :let [tx-op (cond-> tx-op
                              (not (instance? TxOp tx-op)) tx-ops/parse-tx-op)]]
          (xtp/execute-op! tx-op conn))
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
   - :system-time
     overrides system-time for the transaction,
     mustn't be earlier than any previous system-time

   - :default-tz
     overrides the default time zone for the transaction,
     should be an instance of java.time.ZoneId"
  ([connectable, tx-ops] (submit-tx connectable tx-ops {}))

  ([connectable, tx-ops tx-opts]
   (with-conn [conn connectable]
     (let [{:keys [tx-id await-token]} (submit-tx* conn tx-ops (-> tx-opts
                                                       (assoc :async? true)))]
       (when (instance? xtdb.api.DataSource connectable)
         (.setAwaitToken ^xtdb.api.DataSource connectable await-token))
       {:tx-id tx-id}))))

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

  (^TransactionKey [connectable, tx-ops] (execute-tx connectable tx-ops {}))

  (^TransactionKey [connectable, tx-ops tx-opts]
   (with-conn [conn connectable]
     (let [{:keys [tx-id system-time await-token]} (submit-tx* conn tx-ops (-> tx-opts (assoc :async? false)))]
       (when (instance? xtdb.api.DataSource connectable)
         (.setAwaitToken ^xtdb.api.DataSource connectable await-token))
       (serde/->TxKey tx-id (time/->instant system-time))))))

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
            (when (or (nil? old-token)
                      (and await-token
                           (< ^long (parse-long old-token) ^long (parse-long await-token))))
              (when-not (.compareAndSet !await-token old-token await-token)
                (recur))))))

      (createConnectionBuilder [_]
        (DataSource$ConnectionBuilder. host port user password dbname)))))

(defn status
  "Returns the status of this node as a map"

  ([connectable]
   (with-conn [conn connectable]
     (letfn [(status-q [conn query+args]
               (jdbc/execute! conn query+args
                              {:builder-fn xt-jdbc/builder-fn}))]

       (jdbc/execute! conn ["BEGIN READ ONLY WITH (await_token = NULL)"])

       (try
         {:metrics (-> (concat (status-q conn ["SELECT * FROM xt.metrics_counters"])
                               (status-q conn ["SELECT * FROM xt.metrics_timers"])
                               (status-q conn ["SELECT * FROM xt.metrics_gauges"]))
                       (->> (group-by :name))
                       (update-vals (fn [metrics]
                                      (mapv #(dissoc % :name) metrics))))

          :latest-completed-txs (-> (status-q conn ["SHOW LATEST_COMPLETED_TXS"])
                                    (->> (group-by :db-name))
                                    (update-vals (fn [txs]
                                                   (mapv #(serde/map->TxKey (select-keys % [:tx-id :system-time])) txs))))

          :latest-submitted-txs (-> (status-q conn ["SHOW LATEST_SUBMITTED_TXS"])
                                    (->> (group-by :db-name))
                                    (update-vals #(mapv :tx-id %)))}

         (finally
           (jdbc/execute! conn ["ROLLBACK"])))))))

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
