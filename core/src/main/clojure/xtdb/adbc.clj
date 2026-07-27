(ns xtdb.adbc
  "In-process implementations of the `xtdb.api` operations (query / submit-tx / execute-tx) over an
  ADBC `Xtdb.Connection`. These are the bodies behind the `xtdb.protocols/Connectable`
  impl that `xtdb.node.impl` extends onto `Xtdb.Connection` — kept in core because the native path
  is expressed against the core `TxOp` / `ResultCursor` / `Relation` types.

  The caller owns the connection: these fns borrow it (a query threads the connection's own
  await-basis; a write advances it), never close it."
  (:require [xtdb.basis :as basis]
            [xtdb.error :as err]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang IReduceInit)
           (java.time.format DateTimeParseException)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.api ResultCursor Xtdb$Connection)
           (xtdb.arrow RelationReader)
           (xtdb.tx TxOp$PatchDocs TxOp$PutDocs TxOp$Sql TxOpts)))

(defn- coerce-instant
  "Coerces a user-supplied time value to an Instant, surfacing a bad value as an `:incorrect`
  anomaly rather than a raw parse exception (the pgwire path gets this from SQL coercion)."
  [x default-tz]
  (when x
    (try
      (time/->instant x {:default-tz default-tz})
      (catch DateTimeParseException e
        (throw (err/incorrect :xtdb/invalid-time (ex-message e) {:value x}))))))

;; -- reads --

(defn- with-cursor
  "Opens a read-only query cursor at the connection's basis and calls `(f cursor)`, closing the
  statement and cursor afterwards. The connection is borrowed, never closed."
  [^Xtdb$Connection conn sql args {:keys [await-token snapshot-token snapshot-time current-time default-tz]} f]
  ;; only advance the connection's await-basis when the caller supplied one; its own writes already
  ;; sit in `awaitToken`. `prepare` awaits it, so seed it before opening the statement.
  (when await-token
    (.setAwaitToken conn (basis/merge-tx-tokens (.getAwaitToken conn) await-token)))

  ;; a caller-supplied basis is pinned on a read-only tx, as `BEGIN READ ONLY WITH (…)` does for the JDBC
  ;; path — reads then open through the connection's own gate, so they're traced and metered like every
  ;; other frontend's. Without overrides we don't open one: the query reads at the connection's basis,
  ;; leaving an already-open tx (a caller's own) to supply it.
  (let [pinned? (boolean (or snapshot-token snapshot-time current-time default-tz))]
    (when pinned?
      (.beginReadOnly conn (or default-tz (.getDefaultTz conn)) snapshot-token
                      (coerce-instant snapshot-time default-tz)
                      (coerce-instant current-time default-tz)))
    (try
      (with-open [stmt (.createStatement conn sql)]
        (.prepare stmt)
        (when (seq args)
          ;; positional `?` params, matched by ordinal; `bind` opens its own slice, so close ours after
          (with-open [rel (vw/open-args (.getAllocator conn) args)]
            (.bind stmt rel)))

        (with-open [cursor (.openQuery stmt)]
          (f cursor)))
      (finally
        (when pinned?
          (.rollbackTx conn))))))

(defn- reduce-page
  "Reduces `f`/`acc` over one page's rows, preserving a `reduced` short-circuit so the cursor loop
  stops (`transduce` can't — it unwraps `reduced` per page, so early termination wouldn't cross a
  page boundary). `->clj` turns the decode's java.util list/struct/set collections into Clojure
  persistent ones, matching the pgwire+next.jdbc path so results compare `=` and hash the same."
  [f acc ^RelationReader rel key-fn]
  (reduce (fn [acc row]
            (let [acc (f acc (util/->clj row))]
              (cond-> acc (reduced? acc) reduced)))
          acc (.toMaps rel key-fn)))

(defn plan-q ^clojure.lang.IReduceInit [^Xtdb$Connection conn sql args {:keys [key-fn] :as opts}]
  (let [key-fn (serde/read-key-fn (or key-fn :kebab-case-keyword))]
    (reify IReduceInit
      (reduce [_ f start]
        (err/wrap-anomaly {:sql sql}
          (with-cursor conn sql args opts
            (fn [^ResultCursor cursor]
              ;; the page RelationReader is only valid inside the tryAdvance callback (the cursor
              ;; reuses/frees the buffer after), so we reduce it there, into the accumulator.
              (let [!acc (volatile! start)]
                (loop []
                  (when (and (not (reduced? @!acc))
                             (.tryAdvance cursor (fn [^RelationReader rel]
                                                   (vreset! !acc (reduce-page f @!acc rel key-fn)))))
                    (recur)))

                (unreduced @!acc)))))))))

;; -- writes --

(defn- op->tx-ops
  "Converts one parsed tx-op map to core `TxOp`s. put/patch build native doc ops; delete/erase/sql
  become a single `TxOp.Sql` (indexed as SQL DML, as the pgwire path does) whose arg-rows are one
  multi-row `RelationReader` — a batch, matching how the connection's own DML coalescing works."
  [^BufferAllocator al {:keys [op table-name docs doc-ids valid-from valid-to sql arg-rows]}]
  (let [[schema table] (when table-name [(namespace table-name) (name table-name)]) ; nil for :sql
        for-vt (when (or valid-from valid-to) "FOR VALID_TIME FROM ? TO ?")]
    (case op
      :put-docs [(TxOp$PutDocs/openFromRows al schema table docs valid-from valid-to)]
      :patch-docs [(TxOp$PatchDocs/openFromRows al schema table docs valid-from valid-to)]

      :delete-docs (when (seq doc-ids)
                     (let [dsql (format "DELETE FROM \"%s\".\"%s\" %s WHERE _id = ?" schema table (or for-vt ""))]
                       [(TxOp$Sql. dsql (apply vw/open-args al (map #(if for-vt [valid-from valid-to %] [%]) doc-ids)))]))

      :erase-docs (when (seq doc-ids)
                    (let [esql (format "ERASE FROM \"%s\".\"%s\" WHERE _id = ?" schema table)]
                      [(TxOp$Sql. esql (apply vw/open-args al (map vector doc-ids)))]))

      :sql (cond
             (nil? arg-rows) [(TxOp$Sql. sql nil)]
             (empty? arg-rows) []
             :else [(TxOp$Sql. sql (apply vw/open-args al arg-rows))]))))

(defn- ->tx-ops
  "Converts all parsed `tx-ops` to core `TxOp`s, then eagerly expands static SQL DML (INSERT/PATCH
  RECORDS) to native doc ops via the connection's own `expandStaticOps` — the statement path the
  pgwire frontend takes. A raw `TxOp.Sql` PATCH expanded at index time mishandles ids (`[B`), so we
  must expand before submit; UPDATE/DELETE/ERASE and parameterised SQL fall through as `TxOp.Sql`.
  `safe-mapv` (with `util/close`'s recursion into collections) closes any already-built ops if a
  later conversion throws; `expandStaticOps` takes ownership of the list it's handed."
  ^java.util.List [^Xtdb$Connection conn tx-ops opts]
  (let [al (.getAllocator conn)
        tz (or (:default-tz opts) (.getDefaultTz conn))]
    (.expandStaticOps conn (into [] cat (util/safe-mapv #(op->tx-ops al %) tx-ops)) tz)))

(defn- ->tx-opts ^TxOpts [{:keys [system-time default-tz metadata]}]
  ;; no dbName — a connection is already bound to its database (cf. the JDBC path rejecting :database)
  (TxOpts. default-tz (coerce-instant system-time default-tz) nil metadata nil))

(defn submit-tx
  "Converts parsed `tx-ops` to native `TxOp`s and submits them asynchronously."
  [^Xtdb$Connection conn tx-ops opts]
  (err/wrap-anomaly {}
    (util/with-open [core-ops (->tx-ops conn tx-ops opts)]
      {:tx-id (.getTxId (.submitTx conn core-ops (->tx-opts opts)))})))

(defn execute-tx
  "Converts parsed `tx-ops` to native `TxOp`s and submits them, blocking for indexing; throws on abort."
  ^xtdb.api.TransactionKey [^Xtdb$Connection conn tx-ops opts]
  (err/wrap-anomaly {}
    (util/with-open [core-ops (->tx-ops conn tx-ops opts)]
      (let [executed (.executeTx conn core-ops (->tx-opts opts))]
        (when-not (.getCommitted executed)
          (throw (or (.getError executed)
                     (err/fault :xtdb/tx-aborted "Transaction aborted"))))
        (serde/->TxKey (.getTxId executed) (.getSystemTime executed))))))
