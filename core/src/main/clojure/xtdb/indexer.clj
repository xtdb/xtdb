(ns xtdb.indexer
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.basis :as basis]
            [xtdb.error :as err]
            [xtdb.indexer.crash-logger :refer [with-crash-log]]
            [xtdb.log :as xt-log]
            [xtdb.metrics :as metrics]
            [xtdb.query :as q]
            [xtdb.serde :as serde]
            [xtdb.serde.types :as st]
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
           (xtdb.indexer CrashLogger Indexer Indexer$Factory Indexer$ForDatabase LiveIndex OpenTx OpenTx$Table OpIndexer Snapshot Snapshot$Source TxIndexer TxIndexer$QueryOpts)
           (xtdb.table TableRef)
           xtdb.NodeBase
           (xtdb.query IQuerySource IQuerySource$QueryCatalog QueryOpts)))

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
        system-time-µs (time/instant->micros system-time)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [^String leg-name (.getLeg docs-rdr tx-op-idx)
              ^TableRef table (table/->ref db-name leg-name)]
          (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

          (let [table-docs-rdr (.vectorFor docs-rdr leg-name)
                doc-rdr (.getListElements table-docs-rdr)
                ks (.getKeyNames doc-rdr)]
            (when-let [forbidden-cols (not-empty (->> ks
                                                      (into #{} (filter (every-pred #(str/starts-with? % "_")
                                                                                    (complement #{"_id" "_fn" "_valid_from" "_valid_to"}))))))]
              (throw (err/incorrect :xtdb/forbidden-columns (str "Cannot put documents with columns: " (pr-str forbidden-cols))
                                    {:table table, :forbidden-cols forbidden-cols})))

            (let [doc-start-idx (.getListStartIndex table-docs-rdr tx-op-idx)
                  doc-count (.getListCount table-docs-rdr tx-op-idx)

                  doc-struct (-> (RelationAsStructReader.
                                   "doc"
                                   (vr/rel-reader (for [^String sk ks
                                                        :when (not (contains? #{"_valid_from" "_valid_to"} sk))]
                                                    (.select (.vectorFor doc-rdr sk) doc-start-idx doc-count))
                                                  doc-count))
                                 (.withName "doc"))

                  iid-col (if (not (.isNull iids-rdr tx-op-idx))
                            (-> (.select iid-rdr (.getListStartIndex iids-rdr tx-op-idx) doc-count)
                                (.withName "_iid"))
                            (-> (.select (.vectorFor doc-rdr "_id") doc-start-idx doc-count)
                                (.withName "_id")))

                  row-vf-col (when-let [vf (.vectorForOrNull doc-rdr "_valid_from")]
                               (assert-timestamp-col-type vf)
                               (-> (.select vf doc-start-idx doc-count) (.withName "_valid_from")))
                  row-vt-col (when-let [vt (.vectorForOrNull doc-rdr "_valid_to")]
                               (assert-timestamp-col-type vt)
                               (-> (.select vt doc-start-idx doc-count) (.withName "_valid_to")))

                  put-rel (vr/rel-reader (filter some? [iid-col doc-struct row-vf-col row-vt-col])
                                         doc-count)

                  valid-from (if (.isNull valid-from-rdr tx-op-idx)
                               system-time-µs
                               (.getLong valid-from-rdr tx-op-idx))
                  valid-to (if (.isNull valid-to-rdr tx-op-idx)
                             Long/MAX_VALUE
                             (.getLong valid-to-rdr tx-op-idx))

                  live-table (.table open-tx table)]
              (with-crash-log crash-logger "error putting documents"
                {:table table, :tx-key tx-key, :tx-op-idx tx-op-idx}
                {:live-idx live-idx, :open-tx-table live-table, :tx-ops-rdr tx-ops-rdr}
                (.logPuts live-table valid-from valid-to put-rel)))))

        nil))))

(defn- ->delete-docs-indexer ^xtdb.indexer.OpIndexer [^LiveIndex live-idx, ^OpenTx open-tx, ^VectorReader tx-ops-rdr,
                                                      ^Instant current-time
                                                      {:keys [^CrashLogger crash-logger tx-key default-db]}]
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
              open-tx-table (.table open-tx table)]
          (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

          (with-crash-log crash-logger "error deleting documents"
            {:table table, :tx-key tx-key, :tx-op-idx tx-op-idx}
            {:live-idx live-idx, :open-tx-table open-tx-table, :tx-ops-rdr tx-ops-rdr}
            (.logDeletes open-tx-table
                         (if (.isNull valid-from-rdr tx-op-idx)
                           current-time-µs
                           (.getLong valid-from-rdr tx-op-idx))
                         (if (.isNull valid-to-rdr tx-op-idx)
                           Long/MAX_VALUE
                           (.getLong valid-to-rdr tx-op-idx))
                         (vr/rel-reader [(-> (.select iid-rdr
                                                      (.getListStartIndex iids-rdr tx-op-idx)
                                                      (.getListCount iids-rdr tx-op-idx))
                                             (.withName "_iid"))]))))

        nil))))

(defn- ->erase-docs-indexer ^xtdb.indexer.OpIndexer [^LiveIndex live-idx, ^OpenTx open-tx, ^VectorReader tx-ops-rdr
                                                     {:keys [^CrashLogger crash-logger tx-key default-db]}]
  (let [db-name default-db
        erase-leg (.vectorFor tx-ops-rdr "erase-docs")
        table-rdr (.vectorFor erase-leg "table")
        iids-rdr (.vectorFor erase-leg "iids")
        iid-rdr (.getListElements iids-rdr)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [^TableRef table (table/->ref db-name (.getObject table-rdr tx-op-idx))
              live-table (.table open-tx table)]
          (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

          (with-crash-log crash-logger "error erasing documents"
            {:table table, :tx-key tx-key, :tx-op-idx tx-op-idx}
            {:live-idx live-idx, :open-tx-table live-table, :tx-ops-rdr tx-ops-rdr}
            (.logErases live-table
                        (.select iid-rdr
                                 (.getListStartIndex iids-rdr tx-op-idx)
                                 (.getListCount iids-rdr tx-op-idx)))))

        nil))))

(defn- ->query-opts ^xtdb.query.QueryOpts [{:keys [current-time default-tz snapshot-token snapshot-time tracer]}]
  (QueryOpts. current-time default-tz snapshot-token snapshot-time tracer))

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
                              (eval-query (-> param-rel (.select selection) (.openSlice al))))))))))

(defn- ->patch-docs-indexer [^BufferAllocator allocator, ^LiveIndex live-idx, ^OpenTx open-tx, ^VectorReader tx-ops-rdr,
                             ^IQuerySource q-src, ^IQuerySource$QueryCatalog db-cat, ^Instant system-time
                             {:keys [^String default-db, ^CrashLogger crash-logger, tx-key, ^Tracer tracer] :as tx-opts}]
  (let [patch-leg (.vectorFor tx-ops-rdr "patch-docs")
        iids-rdr (.vectorFor patch-leg "iids")
        iid-rdr (.getListElements iids-rdr)
        docs-rdr (.vectorFor patch-leg "documents")
        valid-from-rdr (.vectorFor patch-leg "_valid_from")
        valid-to-rdr (.vectorFor patch-leg "_valid_to")
        system-time-µs (time/instant->micros system-time)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [^String leg-name (.getLeg docs-rdr tx-op-idx)
              ^TableRef table (table/->ref default-db leg-name)]
          (when (xt-log/forbidden-table? table)
            (throw (xt-log/forbidden-table-ex table)))

          (let [table-docs-rdr (.vectorFor docs-rdr leg-name)
                doc-rdr (.getListElements table-docs-rdr)
                ks (.getKeyNames doc-rdr)]
            (when-let [forbidden-cols (not-empty (->> ks
                                                      (into #{} (filter (every-pred #(str/starts-with? % "_")
                                                                                    (complement #{"_id" "_fn"}))))))]
              (throw (err/incorrect :xtdb/forbidden-columns
                                    (str "Cannot patch documents with columns: " (pr-str forbidden-cols))
                                    {:table table, :forbidden-cols forbidden-cols})))

            (err/wrap-anomaly {:tx-op-idx tx-op-idx, :tx-key tx-key}
              (let [valid-from-µs (if (.isNull valid-from-rdr tx-op-idx)
                                    system-time-µs
                                    (.getLong valid-from-rdr tx-op-idx))
                    valid-to-µs (if (.isNull valid-to-rdr tx-op-idx)
                                  Long/MAX_VALUE
                                  (.getLong valid-to-rdr tx-op-idx))
                    valid-from (time/micros->instant valid-from-µs)
                    valid-to (when-not (.isNull valid-to-rdr tx-op-idx)
                               (time/micros->instant valid-to-µs))
                    live-table (.table open-tx table)]
                (metrics/with-span tracer "xtdb.transaction.patch-docs" {:attributes {:db (.getDbName table)
                                                                                      :schema (.getSchemaName table)
                                                                                      :table (.getTableName table)}}
                  (let [pq (.preparePatchDocsQuery q-src table valid-from valid-to db-cat tx-opts)
                        args (vr/rel-reader [(SingletonListReader.
                                               "?patch_docs"
                                               (RelationAsStructReader.
                                                 "patch_doc"
                                                 (vr/rel-reader [(-> (.select iid-rdr (.getListStartIndex iids-rdr tx-op-idx) (.getListCount iids-rdr tx-op-idx))
                                                                     (.withName "_iid"))
                                                                 (-> (.select doc-rdr (.getListStartIndex table-docs-rdr tx-op-idx) (.getListCount table-docs-rdr tx-op-idx))
                                                                     (.withName "doc"))])))])]
                    (util/with-open [res (.openQuery pq (.openSlice args allocator) (->query-opts tx-opts))]
                      (.forEachRemaining res
                                         (fn [^RelationReader rel]
                                           (with-crash-log crash-logger "error patching documents"
                                             {:table table, :tx-key tx-key, :tx-op-idx tx-op-idx}
                                             {:live-idx live-idx, :open-tx-table live-table, :query-rel rel, :tx-ops-rdr tx-ops-rdr}
                                             (.logPuts live-table valid-from-µs valid-to-µs rel))))))))))

          nil)))))

(defn- ->sql-indexer ^xtdb.indexer.OpIndexer [^BufferAllocator allocator, ^OpenTx open-tx,
                                              ^VectorReader tx-ops-rdr,
                                              {:keys [current-time default-tz tx-key ^Tracer tracer]}]
  (let [sql-leg (.vectorFor tx-ops-rdr "sql")
        query-rdr (.vectorFor sql-leg "query")
        args-rdr (.vectorFor sql-leg "args")
        q-opts (TxIndexer$QueryOpts. current-time default-tz)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [query-str (.getObject query-rdr tx-op-idx)]
          (err/wrap-anomaly {:sql query-str, :tx-op-idx tx-op-idx, :tx-key tx-key}
                            (util/with-open [^Relation$ILoader args-loader (open-args-rdr allocator args-rdr tx-op-idx)]
                              (metrics/with-span tracer "xtdb.transaction.sql" {:attributes {:query.text query-str}}
                                (foreach-arg-row allocator args-loader
                                                 (fn [^RelationReader args]
                                                   (.executeDml open-tx query-str args q-opts)))))))

        nil))))

(defn- add-tx-row! [db-name ^OpenTx open-tx, ^TransactionKey tx-key, ^Throwable t, user-metadata]
  (Indexer/addTxRow open-tx db-name tx-key t user-metadata))


(defn- commit [^LiveIndex live-index ^OpenTx open-tx committed? error]
  (let [table-data (.serializeTableData open-tx)
        ^TransactionKey tx-key (.getTxKey open-tx)]
    (.commitTx live-index open-tx)
    (ReplicaMessage$ResolvedTx. (.getTxId tx-key)
                                (.getSystemTime tx-key)
                                (boolean committed?)
                                error
                                table-data
                                nil nil)))

(defrecord IndexerForDatabase [^BufferAllocator allocator, node-id, ^IQuerySource q-src
                               db-name, db-storage, db-state
                               ^LiveIndex live-index, table-catalog
                               ^CrashLogger crash-logger
                               ^TxIndexer tx-indexer
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

            (util/with-open [open-tx (.startTx tx-indexer tx-key)]
              (when tx-error-counter
                (.increment tx-error-counter))
              (add-tx-row! db-name open-tx tx-key err user-metadata)
              (commit live-index open-tx false err)))

          (let [system-time (or system-time default-system-time)
                tx-key (serde/->TxKey msg-id system-time)]
            (util/with-open [open-tx (.startTx tx-indexer tx-key)]
              (if (nil? tx-ops-rdr)
                (do
                                    (util/with-open [open-tx (.startTx tx-indexer tx-key)]
                    (add-tx-row! db-name open-tx tx-key skipped-exn user-metadata)
                    (commit live-index open-tx false skipped-exn)))

                (let [db-cat (Indexer/queryCatalog db-storage db-state
                                                   (reify Snapshot$Source
                                                     (openSnapshot [_]
                                                       (Snapshot/open allocator table-catalog live-index open-tx))))

                      tx-opts {:snapshot-token (basis/->time-basis-str {db-name [system-time]})
                               :current-time system-time
                               :default-tz default-tz
                               :tx-key tx-key
                               :crash-logger crash-logger
                               :default-db db-name
                               :user-metadata user-metadata
                               :tracer tracer}

                      !put-docs-idxer (delay (->put-docs-indexer live-index open-tx tx-ops-rdr system-time tx-opts))
                      !patch-docs-idxer (delay (->patch-docs-indexer allocator live-index open-tx tx-ops-rdr q-src db-cat system-time tx-opts))
                      !delete-docs-idxer (delay (->delete-docs-indexer live-index open-tx tx-ops-rdr system-time tx-opts))
                      !erase-docs-idxer (delay (->erase-docs-indexer live-index open-tx tx-ops-rdr tx-opts))
                      !sql-idxer (delay (->sql-indexer allocator open-tx tx-ops-rdr tx-opts))]

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
                      
                      (util/with-open [open-tx (.startTx tx-indexer tx-key)]
                        (when tx-error-counter
                          (.increment tx-error-counter))
                        (add-tx-row! db-name open-tx tx-key e user-metadata)
                        (commit live-index open-tx false e)))

                    (do
                      (add-tx-row! db-name open-tx tx-key nil user-metadata)
                      (commit live-index open-tx true nil)))))))))))

  (addTxRow [_ tx-key e]
    (util/with-open [open-tx (.startTx tx-indexer tx-key)]
      (add-tx-row! db-name open-tx tx-key e {})
      (commit live-index open-tx (nil? e) e))))

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
          (openForDatabase [_ allocator db-storage db-state live-index crash-logger tx-indexer]
            (let [db-name (.getName db-state)]
              (util/with-close-on-catch [allocator (util/->child-allocator allocator (str "indexer/" db-name))]
                (->IndexerForDatabase allocator (.getNodeId config) q-src
                                      db-name db-storage db-state
                                      live-index (.getTableCatalog db-state)
                                      crash-logger tx-indexer
                                      tx-timer tx-error-counter tracer))))

          (close [_]))))))

