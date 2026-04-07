(ns xtdb.indexer
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.authn.crypt :as authn.crypt]
            [xtdb.basis :as basis]
            [xtdb.error :as err]
            [xtdb.indexer.crash-logger :refer [with-crash-log]]
            [xtdb.log :as xt-log]
            [xtdb.logical-plan :as lp]
            [xtdb.metrics :as metrics]
            [xtdb.query :as q]
            [xtdb.serde :as serde]
            [xtdb.serde.types :as st]
            [xtdb.sql :as sql]
            [xtdb.sql.parse :as parse-sql]
            [xtdb.table :as table]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (clojure.lang MapEntry)
           (io.micrometer.core.instrument Counter Timer)
           (io.micrometer.tracing Tracer)
           (java.nio ByteBuffer)
           (java.time Instant)
           (org.apache.arrow.memory BufferAllocator)
           xtdb.api.TransactionKey
           (xtdb.api.log ReplicaMessage$ResolvedTx)
           (xtdb.arrow Relation Relation$ILoader RelationAsStructReader RelationReader RowCopier SingletonListReader VectorReader)
           (xtdb.error Anomaly$Caller Interrupted)
           (xtdb.database DatabaseState)
           (xtdb.indexer CrashLogger Indexer Indexer$Factory Indexer$ForDatabase LiveIndex OpenTx OpenTx$Table OpIndexer RelationIndexer Snapshot Snapshot$Source)
           (xtdb.table TableRef)
           xtdb.NodeBase
           (xtdb.query IQuerySource IQuerySource$QueryCatalog PreparedQuery)))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private skipped-exn (err/fault :xtdb/skipped-tx "Transaction was skipped"))

(defn- assert-timestamp-col-type [^VectorReader rdr]
  (when-not (or (nil? rdr) (= types/temporal-arrow-type (.getArrowType rdr)))
    (throw (err/fault :xtdb/invalid-timestamp-col-type "Invalid timestamp col type"
                      {:field (.getField rdr)}))))

(defn- ->put-docs-indexer ^xtdb.indexer.OpIndexer [^LiveIndex live-idx, ^OpenTx open-tx, ^VectorReader tx-ops-rdr,
                                                   ^Instant system-time
                                                   {:keys [^CrashLogger crash-logger tx-key ^Tracer tracer default-db]}]
  (let [db-name default-db
        put-leg (.vectorFor tx-ops-rdr "put-docs")
        iids-rdr (.vectorFor put-leg "iids")
        iid-rdr (.getListElements iids-rdr)
        docs-rdr (.vectorFor put-leg "documents")

        valid-from-rdr (.vectorFor put-leg "_valid_from")
        valid-to-rdr (.vectorFor put-leg "_valid_to")
        system-time-µs (time/instant->micros system-time)
        tables (->> (.getLegNames docs-rdr)
                    (into {} (keep (fn [^String table-name]
                                     (when (pos? (.getValueCount (.vectorFor docs-rdr table-name)))
                                       (let [table (table/->ref db-name table-name)]
                                         (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

                                         (let [table-docs-rdr (.vectorFor docs-rdr table-name)
                                               doc-rdr (.getListElements table-docs-rdr)
                                               ks (.getKeyNames doc-rdr)]
                                           (when-let [forbidden-cols (not-empty (->> ks
                                                                                     (into #{} (filter (every-pred #(str/starts-with? % "_")
                                                                                                                   (complement #{"_id" "_fn" "_valid_from" "_valid_to"}))))))]
                                             (throw (err/incorrect :xtdb/forbidden-columns (str "Cannot put documents with columns: " (pr-str forbidden-cols))
                                                                   {:table table
                                                                    :forbidden-cols forbidden-cols})))
                                           (let [table-doc-rdr (RelationAsStructReader.
                                                                "docs"
                                                                (vr/rel-reader (for [^String sk ks,
                                                                                     :when (not (contains? #{"_valid_from" "_valid_to"} sk))]
                                                                                 (.vectorFor doc-rdr sk))
                                                                               (.getValueCount doc-rdr)))
                                                 open-tx-table (.table open-tx table)]
                                             (MapEntry/create table
                                                              {:id-rdr (.vectorFor doc-rdr "_id")

                                                               :open-tx-table open-tx-table

                                                               :row-valid-from-rdr (doto (.vectorForOrNull doc-rdr "_valid_from")
                                                                                     (assert-timestamp-col-type))
                                                               :row-valid-to-rdr (doto (.vectorForOrNull doc-rdr "_valid_to")
                                                                                   (assert-timestamp-col-type))

                                                               :docs-rdr table-docs-rdr

                                                               :doc-copier (.rowCopier table-doc-rdr
                                                                                       (.getDocWriter open-tx-table))})))))))))]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [^TableRef table (table/->ref db-name (.getLeg docs-rdr tx-op-idx))

              {:keys [^VectorReader docs-rdr, ^VectorReader id-rdr, ^OpenTx$Table open-tx-table, ^RowCopier doc-copier
                      ^VectorReader row-valid-from-rdr, ^VectorReader row-valid-to-rdr]}
              (get tables table)

              valid-from (if (.isNull valid-from-rdr tx-op-idx)
                           system-time-µs
                           (.getLong valid-from-rdr tx-op-idx))
              valid-to (if (.isNull valid-to-rdr tx-op-idx)
                         Long/MAX_VALUE
                         (.getLong valid-to-rdr tx-op-idx))

              doc-start-idx (.getListStartIndex docs-rdr tx-op-idx)
              ^long iid-start-idx (or (some-> iids-rdr (.getListStartIndex tx-op-idx))
                                      Long/MIN_VALUE)]
          (dotimes [row-idx (.getListCount docs-rdr tx-op-idx)]
              (let [doc-idx (+ doc-start-idx row-idx)
                    valid-from (if (and row-valid-from-rdr (not (.isNull row-valid-from-rdr doc-idx)))
                                 (.getLong row-valid-from-rdr doc-idx)
                                 valid-from)
                    valid-to (if (and row-valid-to-rdr (not (.isNull row-valid-to-rdr doc-idx)))
                               (.getLong row-valid-to-rdr doc-idx)
                               valid-to)]

                (when-not (> valid-to valid-from)
                  (throw (err/incorrect :xtdb.indexer/invalid-valid-times
                                        "Invalid valid times"
                                        {:valid-from (time/micros->instant valid-from)
                                         :valid-to (time/micros->instant valid-to)})))

                (with-crash-log crash-logger "error putting document"
                  {:table table, :tx-key tx-key, :tx-op-idx tx-op-idx, :doc-idx doc-idx}
                  {:live-idx live-idx, :open-tx-table open-tx-table, :tx-ops-rdr tx-ops-rdr}

                  (.logPut open-tx-table
                           (if iid-rdr
                             (.getBytes iid-rdr (+ iid-start-idx row-idx))
                             (ByteBuffer/wrap (util/->iid (.getObject id-rdr doc-idx))))
                           valid-from valid-to
                           #(.copyRow doc-copier doc-idx))))))

        nil))))

(defn- ->delete-docs-indexer ^xtdb.indexer.OpIndexer [^LiveIndex live-idx, ^OpenTx open-tx, ^VectorReader tx-ops-rdr,
                                                      ^Instant current-time
                                                      {:keys [^CrashLogger crash-logger tx-key ^Tracer tracer default-db]}]
  (let [db-name default-db
        delete-leg (.vectorFor tx-ops-rdr "delete-docs")
        table-rdr (.vectorFor delete-leg "table")
        iids-rdr (.vectorFor delete-leg "iids")
        iid-rdr (.getListElements iids-rdr)
        valid-from-rdr (.vectorFor delete-leg "_valid_from")
        valid-to-rdr (.vectorFor delete-leg "_valid_to")
        current-time-µs (time/instant->micros current-time)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [^TableRef table (table/->ref db-name (.getObject table-rdr tx-op-idx))
              open-tx-table (.table open-tx table)
              valid-from (if (.isNull valid-from-rdr tx-op-idx)
                           current-time-µs
                           (.getLong valid-from-rdr tx-op-idx))
              valid-to (if (.isNull valid-to-rdr tx-op-idx)
                         Long/MAX_VALUE
                         (.getLong valid-to-rdr tx-op-idx))]

          (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

          (when (> valid-from valid-to)
            (throw (err/incorrect :xtdb.indexer/invalid-valid-times "Invalid valid times"
                                  {:valid-from (time/micros->instant valid-from)
                                   :valid-to (time/micros->instant valid-to)})))

          (let [iids-start-idx (.getListStartIndex iids-rdr tx-op-idx)]
            (dotimes [iid-idx (.getListCount iids-rdr tx-op-idx)]
              (with-crash-log crash-logger "error deleting documents"
                {:table table, :tx-key tx-key, :tx-op-idx tx-op-idx, :iid-idx iid-idx}
                {:live-idx live-idx, :open-tx-table open-tx-table, :tx-ops-rdr tx-ops-rdr}

                (.logDelete open-tx-table (.getBytes iid-rdr (+ iids-start-idx iid-idx))
                            valid-from valid-to)))))

        nil))))

(defn- ->erase-docs-indexer ^xtdb.indexer.OpIndexer [^LiveIndex live-idx, ^OpenTx open-tx, ^VectorReader tx-ops-rdr
                                                     {:keys [^CrashLogger crash-logger tx-key ^Tracer tracer default-db]}]
  (let [db-name default-db
        erase-leg (.vectorFor tx-ops-rdr "erase-docs")
        table-rdr (.vectorFor erase-leg "table")
        iids-rdr (.vectorFor erase-leg "iids")
        iid-rdr (.getListElements iids-rdr)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [^TableRef table (table/->ref db-name (.getObject table-rdr tx-op-idx))
              live-table (.table open-tx table)
              iids-start-idx (.getListStartIndex iids-rdr tx-op-idx)]

          (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

          (dotimes [iid-idx (.getListCount iids-rdr tx-op-idx)]
            (with-crash-log crash-logger "error erasing documents"
              {:table table, :tx-key tx-key, :tx-op-idx tx-op-idx, :iid-idx iid-idx}
              {:live-idx live-idx, :open-tx-table live-table, :tx-ops-rdr tx-ops-rdr}

              (.logErase live-table (.getBytes iid-rdr (+ iids-start-idx iid-idx))))))

        nil))))

(defn- ->upsert-rel-indexer ^xtdb.indexer.RelationIndexer [^LiveIndex live-idx, ^OpenTx open-tx, ^VectorReader tx-ops-rdr,
                                                           {:keys [^Instant current-time, ^CrashLogger crash-logger tx-key]}]

  (let [current-time-µs (time/instant->micros current-time)]
    (reify RelationIndexer
      (indexOp [_ in-rel {:keys [table]}]
        (let [row-count (.getRowCount in-rel)]
          (when (pos? row-count)
            (let [content-rel (vr/rel-reader (->> in-rel
                                                  (remove (comp types/temporal-col-name? #(.getName ^VectorReader %))))
                                             (.getRowCount in-rel))
                  _ (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

                  id-col (.vectorForOrNull in-rel "_id")
                  valid-from-rdr (.vectorForOrNull in-rel "_valid_from")
                  valid-to-rdr (.vectorForOrNull in-rel "_valid_to")

                  open-tx-table (.table open-tx table)]

              (with-crash-log crash-logger "error upserting rows"
                {:table table, :tx-key tx-key}
                {:live-idx live-idx, :open-tx-table open-tx-table, :query-rel in-rel, :tx-ops-rdr tx-ops-rdr}
                (let [live-idx-table-copier (.rowCopier (RelationAsStructReader. "content" content-rel)
                                                        (.getDocWriter open-tx-table))]
                  (when-not id-col
                    (throw (err/incorrect :xtdb.indexer/missing-xt-id-column
                                          "Missing ID column"
                                          {:column-names (vec (for [^VectorReader col in-rel] (.getName col)))})))

                  (dotimes [idx row-count]
                    (err/wrap-anomaly {:table table, :tx-key tx-key, :row-idx idx}
                                      (let [eid (.getObject id-col idx)
                                            valid-from (if (and valid-from-rdr (not (.isNull valid-from-rdr idx)))
                                                         (.getLong valid-from-rdr idx)
                                                         current-time-µs)
                                            valid-to (if (and valid-to-rdr (not (.isNull valid-to-rdr idx)))
                                                       (.getLong valid-to-rdr idx)
                                                       Long/MAX_VALUE)]
                                        (when (> valid-from valid-to)
                                          (throw (err/incorrect :xtdb.indexer/invalid-valid-times
                                                                "Invalid valid times"
                                                                {:valid-from (time/micros->instant valid-from)
                                                                 :valid-to (time/micros->instant valid-to)})))

                                        ;; FIXME something in the generated SQL generates rows with `(= vf vt)`, which is also unacceptable
                                        (when (< valid-from valid-to)
                                          (.logPut open-tx-table (ByteBuffer/wrap (util/->iid eid)) valid-from valid-to #(.copyRow live-idx-table-copier idx)))))))))))))))

(defn- ->delete-rel-indexer ^xtdb.indexer.RelationIndexer [^LiveIndex live-idx, ^OpenTx open-tx, ^VectorReader tx-ops-rdr, {:keys [^CrashLogger crash-logger tx-key]}]
  (reify RelationIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [row-count (.getRowCount in-rel)
            iid-rdr (.vectorFor in-rel "_iid")
            valid-from-rdr (.vectorFor in-rel "_valid_from")
            valid-to-rdr (.vectorFor in-rel "_valid_to")]

        (when (xt-log/forbidden-table? table)
          (throw (xt-log/forbidden-table-ex table)))

        (dotimes [idx row-count]
          (with-crash-log crash-logger "error deleting rows"
            {:table table, :tx-key tx-key, :row-idx idx}
            {:live-idx live-idx, :open-tx-table open-tx, :query-rel in-rel, :tx-ops-rdr tx-ops-rdr}

            (let [iid (.getBytes iid-rdr idx)
                  valid-from (.getLong valid-from-rdr idx)
                  valid-to (if (.isNull valid-to-rdr idx)
                             Long/MAX_VALUE
                             (.getLong valid-to-rdr idx))]
              (when-not (< valid-from valid-to)
                (throw (err/incorrect :xtdb.indexer/invalid-valid-times
                                      "Invalid valid times"
                                      {:valid-from (time/micros->instant valid-from)
                                       :valid-to (time/micros->instant valid-to)})))

              (-> (.table open-tx table)
                  (.logDelete iid valid-from valid-to)))))))))

(defn- ->erase-rel-indexer ^xtdb.indexer.RelationIndexer [^LiveIndex live-idx, ^OpenTx open-tx, ^VectorReader tx-ops-rdr, {:keys [^CrashLogger crash-logger tx-key]}]
  (reify RelationIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [row-count (.getRowCount in-rel)
            iid-rdr (.vectorForOrNull in-rel "_iid")]

        (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

        (dotimes [idx row-count]
          (with-crash-log crash-logger "error erasing rows"
            {:table table, :tx-key tx-key, :row-idx idx}
            {:live-idx live-idx, :open-tx-table open-tx, :query-rel in-rel, :tx-ops-rdr tx-ops-rdr}

            (let [iid (.getBytes iid-rdr idx)]
              (-> (.table open-tx table)
                  (.logErase iid)))))))))

(defn- wrap-sql-args [f ^long param-count]
  (fn [^RelationReader args]
    (if (not args)
      (if (zero? param-count)
        (f nil)
        (throw (err/incorrect :xtdb.indexer/missing-sql-args
                              "Arguments list was expected but not provided"
                              {:param-count param-count})))

      (let [arg-count (count args)]
        (if (not= arg-count param-count)
          (throw (err/incorrect :xtdb.indexer/incorrect-sql-arg-count
                                (format "Parameter error: %d provided, %d expected" arg-count param-count)
                                {:param-count param-count, :arg-count arg-count}))

          (f args))))))

(defn- ->assert-idxer ^xtdb.indexer.RelationIndexer [^IQuerySource q-src, db-cat, tx-opts, {:keys [stmt message]}]
  (let [^PreparedQuery pq (.prepareQuery q-src stmt db-cat tx-opts)]
    (-> (fn eval-query [^RelationReader args]
          (with-open [res (.openQuery pq (-> (select-keys tx-opts [:snapshot-token :current-time :default-tz :tracer :query-text])
                                             (assoc :args args, :close-args? false)))]

            (letfn [(throw-assert-failed []
                      (throw (err/conflict :xtdb/assert-failed (or message "Assert failed"))))]
              (or (.tryAdvance res
                               (fn [^RelationReader in-rel]
                                 (when-not (pos? (.getRowCount in-rel))
                                   (throw-assert-failed))))

                  (throw-assert-failed)))))

        (wrap-sql-args (.getParamCount pq)))))

(defn- query-indexer [allocator, ^IQuerySource q-src, db-cat, ^RelationIndexer rel-idxer, tx-opts, {:keys [stmt] :as query-opts}]
  (let [^PreparedQuery pq (.prepareQuery q-src stmt db-cat tx-opts)]
    (-> (fn eval-query [^RelationReader args]
          (with-open [res (-> (.openQuery pq (-> (select-keys tx-opts [:snapshot-token :current-time :default-tz :tracer :query-text])
                                                 (assoc :args args, :close-args? false))))]
            (.forEachRemaining res
                               (fn [^RelationReader in-rel]
                                 (.indexOp rel-idxer in-rel query-opts)))))

        (wrap-sql-args (.getParamCount pq)))))

(defn- open-args-rdr ^xtdb.arrow.Relation$Loader [^BufferAllocator allocator, ^VectorReader args-rdr, ^long tx-op-idx]
  (when-not (.isNull args-rdr tx-op-idx)
    (Relation/streamLoader allocator ^bytes (.getObject args-rdr tx-op-idx))))

(defn- foreach-arg-row [^BufferAllocator al, ^Relation$ILoader loader, eval-query]
  (if-not loader
    (eval-query nil)

    (with-open [param-rel (Relation. al (.getSchema loader))]
      (while (.loadNextPage loader param-rel)
        (let [selection (int-array 1)]
          (dotimes [idx (.getRowCount param-rel)]
            (err/wrap-anomaly {:arg-idx idx}
                              (aset selection 0 idx)
                              (eval-query (-> param-rel (.select selection))))))))))

(defn- patch-rel! [table, ^LiveIndex live-idx, ^OpenTx$Table live-table, ^RelationReader rel {:keys [^CrashLogger crash-logger tx-key]}]
  (let [row-count (.getRowCount rel)]
    (when (pos? row-count)
      (let [iid-rdr (.vectorForOrNull rel "_iid")
            doc-copier (.rowCopier (.vectorForOrNull rel "doc") (.getDocWriter live-table))
            from-rdr (.vectorForOrNull rel "_valid_from")
            to-rdr (.vectorForOrNull rel "_valid_to")]
        (dotimes [idx row-count]
          (with-crash-log crash-logger "error patching rows"
            {:table table, :tx-key tx-key, :row-idx idx}
            {:live-idx live-idx, :open-tx-table live-table, :query-rel rel}

            (.logPut live-table
                     (.getBytes iid-rdr idx)
                     (.getLong from-rdr idx)
                     (.getLong to-rdr idx)
                     #(.copyRow doc-copier idx))))))))

(defn- ->patch-docs-indexer [^LiveIndex live-idx, ^OpenTx open-tx, ^VectorReader tx-ops-rdr,
                             ^IQuerySource q-src, ^IQuerySource$QueryCatalog db-cat, ^Instant system-time
                             {:keys [^String default-db, ^Tracer tracer] :as tx-opts}]
  (let [patch-leg (.vectorFor tx-ops-rdr "patch-docs")
        iids-rdr (.vectorFor patch-leg "iids")
        iid-rdr (.getListElements iids-rdr)
        docs-rdr (.vectorFor patch-leg "documents")

        valid-from-rdr (.vectorFor patch-leg "_valid_from")
        valid-to-rdr (.vectorFor patch-leg "_valid_to")

        system-time-µs (time/instant->micros system-time)]
    (letfn [(->table-idxer [^String table-name]
              (let [^TableRef table (table/->ref default-db table-name)]
                (when (xt-log/forbidden-table? table)
                  (throw (xt-log/forbidden-table-ex table)))

                (let [table-docs-rdr (.vectorFor docs-rdr table-name)
                      doc-rdr (.getListElements table-docs-rdr)
                      ks (.getKeyNames doc-rdr)]
                  (when-let [forbidden-cols (not-empty (->> ks
                                                            (into #{} (filter (every-pred #(str/starts-with? % "_")
                                                                                          (complement #{"_id" "_fn"}))))))]
                    (throw (err/incorrect :xtdb/forbidden-columns
                                          (str "Cannot patch documents with columns: " (pr-str forbidden-cols))
                                          {:table table, :forbidden-cols forbidden-cols})))

                  (let [live-table (.table open-tx table)]
                    (reify OpIndexer
                      (indexOp [_ tx-op-idx]
                        (err/wrap-anomaly {:tx-op-idx tx-op-idx, :tx-key (:tx-key tx-opts)}
                          (let [valid-from (time/micros->instant (if (.isNull valid-from-rdr tx-op-idx)
                                                                   system-time-µs
                                                                   (.getLong valid-from-rdr tx-op-idx)))
                                valid-to (when-not (.isNull valid-to-rdr tx-op-idx)
                                           (time/micros->instant (.getLong valid-to-rdr tx-op-idx)))]
                            (when-not (or (nil? valid-to) (pos? (compare valid-to valid-from)))
                              (throw (err/incorrect :xtdb.indexer/invalid-valid-times
                                                    "Invalid valid times"
                                                    {:valid-from valid-from, :valid-to valid-to})))
                            (metrics/with-span tracer "xtdb.transaction.patch-docs" {:attributes {:db (.getDbName table)
                                                                                                  :schema (.getSchemaName table)
                                                                                                  :table (.getTableName table)}}
                              (let [table-info (-> (util/with-open [snap (.openSnapshot (.databaseOrNull db-cat default-db))]
                                                     (.getSchema snap))
                                                   (sql/xform-table-info [default-db] default-db))
                                    pq (.prepareQuery q-src (-> (sql/plan-patch {:table-info table-info}
                                                                                {:table table
                                                                                 :valid-from valid-from
                                                                                 :valid-to valid-to
                                                                                 :patch-rel (sql/->QueryExpr '[:table {:output-cols [_iid doc]
                                                                                                                       :param ?patch_docs}]
                                                                                                             '[_iid doc])})
                                                                (lp/rewrite-plan))
                                                      db-cat tx-opts)
                                    args (vr/rel-reader [(SingletonListReader.
                                                           "?patch_docs"
                                                           (RelationAsStructReader.
                                                             "patch_doc"
                                                             (vr/rel-reader [(-> (.select iid-rdr (.getListStartIndex iids-rdr tx-op-idx) (.getListCount iids-rdr tx-op-idx))
                                                                                 (.withName "_iid"))
                                                                             (-> (.select doc-rdr (.getListStartIndex table-docs-rdr tx-op-idx) (.getListCount table-docs-rdr tx-op-idx))
                                                                                 (.withName "doc"))])))])]

                                (util/with-open [res (.openQuery pq (-> (select-keys tx-opts [:snapshot-token :current-time :default-tz])
                                                                        (assoc :args args, :close-args? false)))]
                                  (.forEachRemaining res
                                                     (fn [^RelationReader rel]
                                                       (patch-rel! table live-idx live-table rel tx-opts))))))))))))))]

      (let [tables (->> (.getLegNames docs-rdr)
                        (into {} (keep (fn [table-name]
                                         (when (pos? (.getValueCount (.vectorFor docs-rdr table-name)))
                                           [table-name (->table-idxer table-name)])))))]
        (reify OpIndexer
          (indexOp [_ tx-op-idx]
            (.indexOp ^OpIndexer (get tables (.getLeg docs-rdr tx-op-idx))
                      tx-op-idx)))))))

(def ^:private ^:const user-table #xt/table pg_catalog/pg_user)

(defn- update-pg-user! [^OpenTx open-tx, ^TransactionKey tx-key, user, password]
  (let [system-time-µs (time/instant->micros (.getSystemTime tx-key))

        live-table (.table open-tx user-table)
        doc-writer (.getDocWriter live-table)]

    (.logPut live-table (ByteBuffer/wrap (util/->iid user)) system-time-µs Long/MAX_VALUE
             (fn write-doc! []
               (doto (.vectorFor doc-writer "_id" #xt.arrow/type :utf8 false)
                 (.writeObject user))

               (doto (.vectorFor doc-writer "username" #xt.arrow/type :utf8 false)
                 (.writeObject user))

               (doto (.vectorFor doc-writer "usesuper" #xt.arrow/type :bool false)
                 (.writeObject false))

               (doto (.vectorFor doc-writer "passwd" #xt.arrow/type :utf8 true)
                 (.writeObject (authn.crypt/encrypt-pw password)))

               (.endStruct doc-writer)))))

(defn- ->sql-indexer ^xtdb.indexer.OpIndexer [^BufferAllocator allocator, ^LiveIndex live-idx, ^OpenTx open-tx
                                              ^VectorReader tx-ops-rdr, ^IQuerySource q-src, db-cat,
                                              {:keys [default-db tx-key ^Tracer tracer] :as tx-opts}]
  (let [sql-leg (.vectorFor tx-ops-rdr "sql")
        query-rdr (.vectorFor sql-leg "query")
        args-rdr (.vectorFor sql-leg "args")
        upsert-idxer (->upsert-rel-indexer live-idx open-tx tx-ops-rdr tx-opts)
        patch-idxer (reify RelationIndexer
                      (indexOp [_ rel {:keys [table]}]
                        (patch-rel! table live-idx (.table open-tx table) rel tx-opts)))
        delete-idxer (->delete-rel-indexer live-idx open-tx tx-ops-rdr tx-opts)
        erase-idxer (->erase-rel-indexer live-idx open-tx tx-ops-rdr tx-opts)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [query-str (.getObject query-rdr tx-op-idx)]
          (err/wrap-anomaly {:sql query-str, :tx-op-idx tx-op-idx, :tx-key tx-key}
                            (util/with-open [^Relation$ILoader args-loader (open-args-rdr allocator args-rdr tx-op-idx)]
                              (metrics/with-span tracer "xtdb.transaction.sql" {:attributes {:query.text query-str}}
                                (let [[q-tag q-args] (parse-sql/parse-statement query-str {:default-db default-db})
                                      tx-opts (assoc tx-opts
                                                     :arg-fields (some-> args-loader (.getSchema) (.getFields))
                                                     :query-text query-str)]
                                  (case q-tag
                                    :insert (foreach-arg-row allocator args-loader
                                                             (query-indexer allocator q-src db-cat upsert-idxer tx-opts q-args))
                                    :patch (foreach-arg-row allocator args-loader
                                                            (query-indexer allocator q-src db-cat patch-idxer tx-opts q-args))
                                    :update (foreach-arg-row allocator args-loader
                                                             (query-indexer allocator q-src db-cat upsert-idxer tx-opts q-args))
                                    :delete (foreach-arg-row allocator args-loader
                                                             (query-indexer allocator q-src db-cat delete-idxer tx-opts q-args))
                                    :erase (foreach-arg-row allocator args-loader
                                                            (query-indexer allocator q-src db-cat erase-idxer tx-opts q-args))
                                    :assert (foreach-arg-row allocator args-loader
                                                             (->assert-idxer q-src db-cat tx-opts q-args))

                                    :create-user (let [{:keys [username password]} q-args]
                                                   (update-pg-user! open-tx tx-key username password))

                                    :alter-user (let [{:keys [username password]} q-args]
                                                  (update-pg-user! open-tx tx-key username password))

                                    (throw (err/incorrect ::invalid-sql-tx-op "Invalid SQL query sent as transaction operation"
                                                          {:query query-str}))))))))

        nil))))

(defn- add-tx-row! [db-name ^OpenTx open-tx, ^TransactionKey tx-key, ^Throwable t, user-metadata]
  (Indexer/addTxRow open-tx db-name tx-key t user-metadata))


(defn- build-table-data [^OpenTx open-tx]
  (let [m (java.util.HashMap.)]
    (doseq [^java.util.Map$Entry entry (.getTables open-tx)]
      (let [^TableRef table-ref (.getKey entry)
            ^OpenTx$Table table-tx (.getValue entry)]
        (when-let [data (.serializeTxData table-tx)]
          (.put m (.getSchemaAndTable table-ref) data))))
    m))

(defn- ->resolved-tx [^TransactionKey tx-key committed? error table-data]
  (ReplicaMessage$ResolvedTx. (.getTxId tx-key)
                              (.getSystemTime tx-key)
                              committed?
                              error
                              table-data
                              nil
                              nil))

(defn- commit [tx-key ^OpenTx open-tx committed? error]
  (let [table-data (build-table-data open-tx)]
    (.commit open-tx)
    (->resolved-tx tx-key committed? error table-data)))

(defrecord IndexerForDatabase [^BufferAllocator allocator, node-id, ^IQuerySource q-src
                               db-name, db-storage, db-state
                               ^LiveIndex live-index, table-catalog
                               ^CrashLogger crash-logger
                               ^Timer tx-timer
                               ^Counter tx-error-counter
                               ^Tracer tracer]
  Indexer$ForDatabase
  (close [_]
    (util/close allocator))

  (indexTx [this msg-id msg-ts tx-ops-rdr
            system-time default-tz _user user-metadata]
    (metrics/with-span tracer "xtdb.transaction" {:attributes {:operations.count (str (.getValueCount tx-ops-rdr))}}
      (let [lc-tx (.getLatestCompletedTx live-index)
            default-system-time (or (when-let [lc-sys-time (some-> lc-tx (.getSystemTime))]
                                      (when-not (neg? (compare lc-sys-time msg-ts))
                                        (.plusNanos lc-sys-time 1000)))
                                    msg-ts)]

        (if (and system-time lc-tx
                 (neg? (compare system-time (.getSystemTime lc-tx))))
          (let [tx-key (serde/->TxKey msg-id default-system-time)
                err (err/incorrect :invalid-system-time "specified system-time older than current tx"
                                   {:tx-key (serde/->TxKey msg-id system-time)
                                    :latest-completed-tx (.getLatestCompletedTx live-index)})]
            (log/warnf "specified system-time '%s' older than current tx '%s'"
                       (pr-str system-time)
                       (pr-str (.getLatestCompletedTx live-index)))

            (util/with-open [open-tx (.startTx live-index tx-key)]
              (when tx-error-counter
                (.increment tx-error-counter))
              (add-tx-row! db-name open-tx tx-key err user-metadata)
              (commit tx-key open-tx false err)))

          (let [system-time (or system-time default-system-time)
                tx-key (serde/->TxKey msg-id system-time)]
            (util/with-open [open-tx (.startTx live-index tx-key)]
              (if (nil? tx-ops-rdr)
                (do
                  (.abort open-tx)
                  (util/with-open [open-tx (.startTx live-index tx-key)]
                    (add-tx-row! db-name open-tx tx-key skipped-exn user-metadata)
                    (commit tx-key open-tx false skipped-exn)))

                (let [db-cat (Indexer/queryCatalog db-storage db-state
                                                   (reify Snapshot$Source
                                                     (openSnapshot [_]
                                                       (util/with-close-on-catch [live-index-snap (.openSnapshot open-tx)]
                                                         (Snapshot. tx-key live-index-snap
                                                                    (update-vals (LiveIndex/buildSchema live-index-snap table-catalog) set))))))

                      tx-opts {:snapshot-token (basis/->time-basis-str {db-name [system-time]})
                               :current-time system-time
                               :default-tz default-tz
                               :tx-key tx-key
                               :crash-logger crash-logger
                               :default-db db-name
                               :user-metadata user-metadata
                               :tracer tracer}

                      !put-docs-idxer (delay (->put-docs-indexer live-index open-tx tx-ops-rdr system-time tx-opts))
                      !patch-docs-idxer (delay (->patch-docs-indexer live-index open-tx tx-ops-rdr q-src db-cat system-time tx-opts))
                      !delete-docs-idxer (delay (->delete-docs-indexer live-index open-tx tx-ops-rdr system-time tx-opts))
                      !erase-docs-idxer (delay (->erase-docs-indexer live-index open-tx tx-ops-rdr tx-opts))
                      !sql-idxer (delay (->sql-indexer allocator live-index open-tx tx-ops-rdr q-src db-cat tx-opts))]

                  (if-let [e (try
                               (err/wrap-anomaly {}
                                                 (dotimes [tx-op-idx (.getValueCount tx-ops-rdr)]
                                                   (.recordCallable tx-timer
                                                                    #(case (.getLeg tx-ops-rdr tx-op-idx)
                                                                       "xtql" (throw (err/unsupported :xtdb/xtql-dml-removed
                                                                                                      (str/join ["XTQL DML is no longer supported, as of 2.0.0-beta7. "
                                                                                                                 "Please use SQL DML statements instead - "
                                                                                                                 "see the release notes for more information."])))
                                                                       "sql" (.indexOp ^OpIndexer @!sql-idxer tx-op-idx)
                                                                       "put-docs" (.indexOp ^OpIndexer @!put-docs-idxer tx-op-idx)
                                                                       "patch-docs" (.indexOp ^OpIndexer @!patch-docs-idxer tx-op-idx)
                                                                       "delete-docs" (.indexOp ^OpIndexer @!delete-docs-idxer tx-op-idx)
                                                                       "erase-docs" (.indexOp ^OpIndexer @!erase-docs-idxer tx-op-idx)
                                                                       "call" (throw (err/unsupported :xtdb/tx-fns-removed
                                                                                                      (str/join ["tx-fns are no longer supported, as of 2.0.0-beta7. "
                                                                                                                 "Please use ASSERTs and SQL DML statements instead - "
                                                                                                                 "see the release notes for more information."]))))))
                                                 nil)

                               (catch Anomaly$Caller e e))]

                    (do
                      (log/debug e "aborted tx")
                      (.abort open-tx)

                      (util/with-open [open-tx (.startTx live-index tx-key)]
                        (when tx-error-counter
                          (.increment tx-error-counter))
                        (add-tx-row! db-name open-tx tx-key e user-metadata)
                        (commit tx-key open-tx false e)))

                    (do
                      (add-tx-row! db-name open-tx tx-key nil user-metadata)
                      (commit tx-key open-tx true nil)))))))))))

  (addTxRow [_ tx-key e]
    (util/with-open [open-tx (.startTx live-index tx-key)]
      (add-tx-row! db-name open-tx tx-key e {})
      (commit tx-key open-tx (nil? e) e))))

(defn ->factory ^xtdb.indexer.Indexer$Factory []
  (reify Indexer$Factory
    (^xtdb.indexer.Indexer
     create [_ ^NodeBase base]
      (let [config (.getConfig base)
            metrics-registry (.getMeterRegistry base)
            q-src (.getQuerySource base)
            ^Tracer tracer (when (.getTransactionTracing (.getTracer config)) (.getTracer base))
            tx-timer (metrics/add-timer metrics-registry "tx.op.timer"
                                        {:description "indicates the timing and number of transactions"})
            tx-error-counter (metrics/add-counter metrics-registry "tx.error")]
        (reify Indexer
          (openForDatabase [_ allocator db-storage db-state live-index crash-logger]
            (let [db-name (.getName db-state)]
              (util/with-close-on-catch [allocator (util/->child-allocator allocator (str "indexer/" db-name))]
                (->IndexerForDatabase allocator (.getNodeId config) q-src
                                      db-name db-storage db-state
                                      live-index (.getTableCatalog db-state)
                                      crash-logger
                                      tx-timer tx-error-counter tracer))))

          (close [_]))))))

