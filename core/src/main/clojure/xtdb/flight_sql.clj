(ns xtdb.flight-sql
  "FlightSQL server integrant component, and the Clojure client that lets `xtdb.api` drive a node over
  the FlightSQL wire.

  The client is a `xtdb.protocols/Connectable`, so `xt/q`, `xt/submit-tx`, `xt/execute-tx` and
  `xt/status` work against it exactly as they do against a node, a JDBC connection or an in-process
  `Xtdb.Connection`. Every operation is expressed as SQL, parameter binding and FlightSQL transactions."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.adbc :as adbc]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang IReduceInit)
           (java.io Closeable)
           (java.nio.charset StandardCharsets)
           (java.util Iterator)
           (java.util.concurrent ExecutionException)
           (org.apache.arrow.flight Action CallOption FlightClient FlightEndpoint FlightInfo FlightRuntimeException FlightStatusCode Location Result SetSessionOptionsRequest SessionOptionValueFactory)
           (org.apache.arrow.flight.client ClientCookieMiddleware$Factory)
           (org.apache.arrow.flight.sql FlightSqlClient FlightSqlClient$Transaction)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (org.apache.arrow.vector VectorSchemaRoot)
           (xtdb JsonSerde)
           (xtdb.api FlightSql FlightSqlConfig Xtdb$Config)
           (xtdb.flight_sql XtdbProducer)
           (xtdb.arrow Relation)))

(defmethod xtn/apply-config! :flight-sql [^Xtdb$Config config _ {:keys [host port transaction-idle-timeout]}]
  (cond-> (.getFlightSql config)
    (some? host) (.host host)
    (some? port) (.port port)
    (some? transaction-idle-timeout) (.transactionIdleTimeout (time/->duration transaction-idle-timeout))))

(defmethod ig/expand-key ::server [k config]
  {k {:node (ig/ref :xtdb/node)
      :config config}})

(defmethod ig/init-key ::server [_ {:keys [node config]}]
  (FlightSql/open node config))

(defmethod ig/halt-key! ::server [_ server]
  (util/try-close server))

;; -- client --

(def ^:private ^"[Lorg.apache.arrow.flight.CallOption;" no-call-opts
  (make-array CallOption 0))

(defn- unwrap-flight-ex ^FlightRuntimeException [^FlightRuntimeException ex]
  ;; `FlightSqlClient`'s doPut helpers rewrap whatever the server sent as a description-less CANCELLED
  ;; carrying the real error as its cause, so the status and message we want are one or two links down.
  (if (= FlightStatusCode/CANCELLED (.code (.status ex)))
    (let [cause (.getCause ex)
          cause (if (instance? ExecutionException cause) (.getCause ^ExecutionException cause) cause)]
      (if (instance? FlightRuntimeException cause)
        (recur cause)
        ex))
    ex))

;; the server maps an anomaly's category to a Flight status and its message to the description; this
;; is the inverse, so a wire error surfaces as the same category of `xtdb.error` anomaly the other
;; connectables throw. The error code doesn't cross the wire, so it comes back nil.
(extend-protocol err/ToAnomaly
  FlightRuntimeException
  (->anomaly [ex data]
    (let [ex (unwrap-flight-ex ex)
          msg (.getMessage ex)
          data (into {::err/cause ex} data)]
      (condp = (.code (.status ex))
        FlightStatusCode/INVALID_ARGUMENT (err/incorrect nil msg data)
        FlightStatusCode/UNIMPLEMENTED (err/unsupported nil msg data)
        FlightStatusCode/NOT_FOUND (err/not-found nil msg data)
        FlightStatusCode/UNAUTHENTICATED (err/forbidden nil msg data)
        FlightStatusCode/UNAUTHORIZED (err/forbidden nil msg data)
        FlightStatusCode/CANCELLED (err/interrupted nil msg data)
        FlightStatusCode/RESOURCE_EXHAUSTED (err/busy nil msg data)
        FlightStatusCode/UNAVAILABLE (err/unavailable nil msg data)
        (err/fault nil msg data)))))

;; -- SQL literals --
;;
;; SET TRANSACTION and SET AWAIT_TOKEN aren't preparable statements, so their options can't be bound as
;; parameters — they're rendered into the statement text instead.

(defn- str-literal [s]
  (str \' (str/replace (str s) "'" "''") \'))

(defn- ts-literal [t default-tz]
  (format "TIMESTAMP %s" (str-literal (str (time/->instant t {:default-tz default-tz})))))

(defn- set-tx-ro-sql [{:keys [default-tz await-token snapshot-token snapshot-time current-time]}]
  (let [opts (cond-> []
               default-tz (conj (str "TIMEZONE = " (str-literal default-tz)))
               snapshot-token (conj (str "SNAPSHOT_TOKEN = " (str-literal snapshot-token)))
               snapshot-time (conj (str "SNAPSHOT_TIME = " (ts-literal snapshot-time default-tz)))
               current-time (conj (str "CLOCK_TIME = " (ts-literal current-time default-tz)))
               await-token (conj (str "AWAIT_TOKEN = " (str-literal await-token))))]
    ;; without overrides we don't open a tx at all: the query then reads at the session's own basis.
    (when (seq opts)
      (str/join ", " (cons "SET TRANSACTION READ ONLY" opts)))))

(defn- set-tx-rw-sql [{:keys [system-time default-tz metadata async?]}]
  (when metadata
    (throw (err/unsupported :xtdb.flight-sql/metadata-unsupported
                            "`:metadata` is not supported over FlightSQL: SET TRANSACTION can't be parameterised, and a metadata map has no faithful SQL-literal rendering"
                            {:metadata metadata})))

  (str/join ", " (cond-> ["SET TRANSACTION READ WRITE" (str "ASYNC = " (boolean async?))]
                   default-tz (conj (str "TIMEZONE = " (str-literal default-tz)))
                   system-time (conj (str "SYSTEM_TIME = " (ts-literal system-time default-tz))))))

;; -- tx ops --

(defn- table-sql [table-name]
  (format "\"%s\".\"%s\"" (namespace table-name) (name table-name)))

(defn- for-valid-time-sql [valid-from valid-to]
  (if (or valid-from valid-to)
    "FOR VALID_TIME FROM ? TO ?"
    ""))

(defn- records-stmts
  "Renders a doc op as `RECORDS {\"col\": ?, …}` statements — one per run of consecutive docs sharing a
  column set, so a homogeneous batch stays a single statement while document order is preserved.

  The columns go in the statement text rather than in a `RECORDS ?` struct param, because the latter
  can only be planned once the arg struct is known: FlightSQL plans a statement at `prepare`, before the
  args arrive (the pgwire path sidesteps this by not preparing its DML at all)."
  [verb table-name for-vt vt-args docs]
  (for [run (partition-by keys (map (fn [doc]
                                      (into (sorted-map)
                                            (map (juxt (comp util/->normal-form-str key) val))
                                            doc))
                                    docs))]
    {:sql (format "%s %s %s RECORDS {%s}" verb (table-sql table-name) for-vt
                  (str/join ", " (map #(format "\"%s\": ?" (str/replace % "\"" "\"\""))
                                      (keys (first run)))))
     :arg-rows (mapv #(into (vec vt-args) (vals %)) run)}))

(defn- op->stmts
  "Renders one parsed tx-op as a seq of `{:sql .., :arg-rows ..}`.

  `:arg-rows` nil sends the statement unparameterised; otherwise the rows are bound as one batch, which
  the server accumulates into a single multi-row tx op.

  put-docs has no `FOR VALID_TIME` form on INSERT, so its valid-time bounds ride in the record itself,
  as they do on the pgwire path's COPY."
  [{:keys [op table-name docs doc-ids valid-from valid-to sql arg-rows]}]
  (let [for-vt (for-valid-time-sql valid-from valid-to)
        vt-args (when (seq for-vt) [valid-from valid-to])]
    (case op
      :put-docs (records-stmts "INSERT INTO" table-name "" nil
                               (cond->> docs
                                 (or valid-from valid-to)
                                 (map (partial into (->> {:xt/valid-from valid-from, :xt/valid-to valid-to}
                                                         (into {} (remove (comp nil? val))))))))

      :patch-docs (records-stmts "PATCH INTO" table-name for-vt vt-args docs)

      :delete-docs (when (seq doc-ids)
                     [{:sql (format "DELETE FROM %s %s WHERE _id = ?" (table-sql table-name) for-vt)
                       :arg-rows (mapv (fn [doc-id] (conj (vec vt-args) doc-id)) doc-ids)}])

      :erase-docs (when (seq doc-ids)
                    [{:sql (format "ERASE FROM %s WHERE _id = ?" (table-sql table-name))
                      :arg-rows (mapv vector doc-ids)}])

      :sql [{:sql sql, :arg-rows arg-rows}])))

;; -- statement execution --

(defn- ->param-root ^VectorSchemaRoot [^BufferAllocator allocator arg-rows]
  (util/with-open [^Relation rel (apply vw/open-args allocator arg-rows)]
    (.openAsRoot rel allocator)))

(defn- exec-update!
  "Runs a statement that returns no rows, inside [tx] if it's non-nil. The prepared statement takes ownership of
  the parameter root."
  [^BufferAllocator allocator ^FlightSqlClient client ^FlightSqlClient$Transaction tx ^String sql arg-rows]
  (if (nil? arg-rows)
    (if tx
      (.executeUpdate client sql tx no-call-opts)
      (.executeUpdate client sql no-call-opts))

    (when (seq arg-rows)
      (with-open [ps (if tx
                       (.prepare client sql tx no-call-opts)
                       (.prepare client sql no-call-opts))]
        (.setParameters ps (->param-root allocator arg-rows))
        (.executeUpdate ps no-call-opts)))))

(defn- reduce-stream [^BufferAllocator allocator ^FlightSqlClient client ^FlightInfo flight-info key-fn f start]
  (let [ticket (.getTicket ^FlightEndpoint (first (.getEndpoints flight-info)))]
    (with-open [stream (.getStream client ticket no-call-opts)]
      (let [root (.getRoot stream)]
        (with-open [rel (Relation/fromRoot allocator root)]
          (loop [acc start]
            (if (or (reduced? acc) (not (.next stream)))
              (unreduced acc)
              (do
                (.loadFromArrow rel root)
                (recur (adbc/reduce-page f acc rel key-fn))))))))))

(defn- exec-query! [^BufferAllocator allocator ^FlightSqlClient client ^FlightSqlClient$Transaction tx ^String sql args key-fn f start]
  (if (seq args)
    ;; a query binds one row of positional params, so it needs the prepared path
    (with-open [ps (if tx
                     (.prepare client sql tx no-call-opts)
                     (.prepare client sql no-call-opts))]
      (.setParameters ps (->param-root allocator [(vec args)]))
      (reduce-stream allocator client (.execute ps no-call-opts) key-fn f start))

    (reduce-stream allocator client
                   (if tx
                     (.execute client sql tx no-call-opts)
                     (.execute client sql no-call-opts))
                   key-fn f start)))

(defn- rollback!
  "Best-effort: a rollback that fails mustn't mask the failure that provoked it."
  [^FlightSqlClient client ^FlightSqlClient$Transaction tx]
  (try
    (.rollback client tx no-call-opts)
    (catch Throwable t
      (log/warn t "couldn't roll back the FlightSQL transaction"))))

(defn- ->anomaly [{:strs [category code message data]}]
  (let [code (some-> code keyword)
        data (update-keys (into {} data) keyword)]
    (case category
      "incorrect" (err/incorrect code message data)
      "conflict" (err/conflict code message data)
      "unsupported" (err/unsupported code message data)
      "interrupted" (err/interrupted code message data)
      "not-found" (err/not-found code message data)
      "forbidden" (err/forbidden code message data)
      "busy" (err/busy code message data)
      "unavailable" (err/unavailable code message data)
      (err/fault code message data))))

(defn- ->commit-outcome [^Result result]
  (let [{:strs [txId systemTime error awaitToken]} (JsonSerde/decode (String. (.getBody result) StandardCharsets/UTF_8))]
    {:tx-id txId
     :system-time (some-> systemTime time/->instant)
     :error (some-> error ->anomaly)
     :await-token awaitToken}))

(defn- commit!
  "Commits [tx], returning the commit's outcome, or nil if it submitted nothing.
  An aborted tx's outcome carries its error, which the server sends after the outcome."
  [^FlightClient flight-client ^FlightSqlClient$Transaction tx]
  (let [^Iterator results (.doAction flight-client
                                     (Action. XtdbProducer/COMMIT_TRANSACTION_ACTION (.getTransactionId tx))
                                     no-call-opts)
        !outcome (volatile! nil)]
    (try
      (while (.hasNext results)
        (vreset! !outcome (->commit-outcome (.next results))))
      @!outcome
      (catch FlightRuntimeException e
        (or @!outcome (throw e))))))

(defn- plan-q ^clojure.lang.IReduceInit [^BufferAllocator allocator ^FlightSqlClient client sql args {:keys [key-fn] :as opts}]
  (let [key-fn (serde/read-key-fn (or key-fn :kebab-case-keyword))]
    (reify IReduceInit
      (reduce [_ f start]
        (err/wrap-anomaly {:sql sql}
          (if-let [set-tx-sql (set-tx-ro-sql opts)]
            (let [tx (.beginTransaction client no-call-opts)]
              (try
                (exec-update! allocator client tx set-tx-sql nil)
                (exec-query! allocator client tx sql args key-fn f start)
                (finally
                  (rollback! client tx))))

            (exec-query! allocator client nil sql args key-fn f start)))))))

(defn- submit-tx*
  "Runs the ops in a FlightSQL transaction and commits it, returning the commit's outcome.

  An async commit returns before the tx is indexed, so the session takes the tx's await token: its later reads then
  see the write."
  [^BufferAllocator allocator ^FlightSqlClient client ^FlightClient flight-client tx-ops {:keys [async?] :as tx-opts}]
  (err/wrap-anomaly {}
    (let [tx (.beginTransaction client no-call-opts)]
      (try
        (exec-update! allocator client tx (set-tx-rw-sql tx-opts) nil)
        (doseq [tx-op tx-ops
                {:keys [sql arg-rows]} (op->stmts tx-op)]
          (err/wrap-anomaly {:sql sql}
            (exec-update! allocator client tx sql arg-rows)))
        (catch Throwable t
          (rollback! client tx)
          (throw t)))

      (let [{:keys [await-token] :as outcome} (commit! flight-client tx)]
        (when (and async? await-token)
          (exec-update! allocator client nil (str "SET AWAIT_TOKEN = " (str-literal await-token)) nil))
        outcome))))

(deftype FlightSqlConn [^BufferAllocator allocator, ^FlightSqlClient client, ^FlightClient flight-client]
  xtp/Connectable
  ;; already bound to its database (chosen at open), so it rejects `:database` as the other
  ;; connection-level impls do.
  (-plan-q [this sql args opts]
    (xtp/check-no-database this opts)
    (plan-q allocator client sql args opts))

  (-submit-tx [this tx-ops opts]
    (xtp/check-no-database this opts)
    {:tx-id (:tx-id (submit-tx* allocator client flight-client tx-ops (assoc opts :async? true)))})

  (-execute-tx [this tx-ops opts]
    (xtp/check-no-database this opts)
    (let [{:keys [tx-id system-time error]} (submit-tx* allocator client flight-client tx-ops (assoc opts :async? false))]
      (when error
        (throw error))

      (serde/->TxKey tx-id system-time)))

  (-status [this] (xtp/build-status this))

  Closeable
  (close [_]
    (util/close [client allocator])))

(defn open-conn
  "Opens a FlightSQL connection to an XTDB node, for use with `xtdb.api`'s `q`/`submit-tx`/`execute-tx`/
  `status`. The caller closes it.

    * `:host`: the host the FlightSQL server is on (default `127.0.0.1`)
    * `:port`: the FlightSQL port — see `Xtdb.getFlightSqlPort`
    * `:dbname`: the database to connect to (default `xtdb`)

  The connection carries a FlightSQL session cookie, so it gets a server-side connection of its own —
  reads see its own writes, and its transactions aren't shared with other clients."
  ^java.io.Closeable [{:keys [^String host ^long port dbname]
                       :or {host "127.0.0.1", dbname "xtdb"}}]
  (let [allocator (RootAllocator.)]
    (try
      (let [flight-client (-> (FlightClient/builder allocator (Location/forGrpcInsecure host port))
                              (.intercept (ClientCookieMiddleware$Factory.))
                              (.build))
            client (FlightSqlClient. flight-client)]
        (try
          ;; mints the session (and hence the server-side connection), as well as selecting the database
          (let [res (.setSessionOptions client
                                        (SetSessionOptionsRequest.
                                         {"catalog" (SessionOptionValueFactory/makeSessionOptionValue ^String dbname)})
                                        no-call-opts)]
            (when (.hasErrors res)
              (throw (err/incorrect :xtdb/unknown-db (str "Unknown database: " dbname) {:db-name dbname}))))

          (->FlightSqlConn allocator client flight-client)

          (catch Throwable t
            (util/try-close client)
            (throw t))))

      (catch Throwable t
        (util/try-close allocator)
        (throw t)))))
