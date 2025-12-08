(ns xtdb.indexer
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.authn.crypt :as authn.crypt]
            [xtdb.basis :as basis]
            [xtdb.error :as err]
            [xtdb.indexer.live-index :as li]
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
           (java.time Instant InstantSource)
           (java.time.temporal ChronoUnit)
           (java.util List)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.types.pojo FieldType)
           xtdb.api.TransactionKey
           (xtdb.arrow Relation Relation$ILoader RelationAsStructReader RelationReader RowCopier SingletonListReader VectorReader)
           (xtdb.database Database Database$Catalog)
           (xtdb.error Anomaly$Caller Interrupted)
           (xtdb.indexer Indexer Indexer$ForDatabase Indexer$TxSink LiveIndex LiveIndex$Tx LiveTable$Tx OpIndexer RelationIndexer Snapshot Snapshot$Source)
           (xtdb.table TableRef)
           (xtdb.query IQuerySource PreparedQuery)))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private skipped-exn (Exception. "skipped"))

(def ^:dynamic ^java.time.InstantSource *crash-log-clock* (InstantSource/system))

(defn crash-log! [{:keys [^BufferAllocator allocator ^Database db node-id]} ex {:keys [table] :as data} {:keys [^LiveIndex live-idx, ^LiveTable$Tx live-table-tx, ^RelationReader query-rel, ^VectorReader tx-ops-rdr]}]
  (let [buffer-pool (.getBufferPool db)
        ts (str (.truncatedTo (.instant *crash-log-clock*) ChronoUnit/SECONDS))
        crash-dir (util/->path (format "crashes/%s/%s" node-id ts))]
    (log/warn "writing a crash log:" (str crash-dir) (pr-str data))

    (let [^String crash-edn (with-out-str
                              (pp/pprint (-> data (assoc :ex ex))))]
      (.putObject buffer-pool (.resolve crash-dir "crash.edn")
                  (ByteBuffer/wrap (.getBytes crash-edn))))

    (when (and live-idx table)
      (.putObject buffer-pool (.resolve crash-dir "live-trie.binpb")
                  (ByteBuffer/wrap (.getAsProto (.getLiveTrie (.liveTable live-idx table))))))

    (when live-table-tx
      (with-open [live-rel (.openDirectSlice (.getLiveRelation live-table-tx) allocator)
                  wtr (.openArrowWriter buffer-pool (.resolve crash-dir "live-table-tx.arrow") live-rel)]
        (.writePage wtr)
        (.end wtr))

      (.putObject buffer-pool (.resolve crash-dir "live-trie-tx.binpb")
                  (ByteBuffer/wrap (.getAsProto (.getTransientTrie live-table-tx)))))

    (when query-rel
      (with-open [query-rel (.openDirectSlice query-rel allocator)
                  wtr (.openArrowWriter buffer-pool (.resolve crash-dir "query-rel.arrow") query-rel)]
        (.writePage wtr)
        (.end wtr)))

    (when tx-ops-rdr
      (let [^List vec-list [tx-ops-rdr]
            tx-ops-rel (Relation. allocator vec-list (int (.getValueCount tx-ops-rdr)))]
        (with-open [wtr (.openArrowWriter buffer-pool (.resolve crash-dir "tx-ops.arrow") tx-ops-rel)]
          (.writePage wtr)
          (.end wtr))))

    (log/info "crash log written:" (str crash-dir))))

(defmacro with-crash-log [indexer msg data state & body]
  `(let [data# ~data]
     (try
       (err/wrap-anomaly data#
                         ~@body)
       (catch Interrupted e# (throw e#))
       (catch Anomaly$Caller e# (throw e#))
       (catch Throwable e#
         (let [{node-id# :node-id, :as indexer#} ~indexer
               msg# ~msg
               data# (-> data#
                         (assoc :node-id node-id#))]
           (try
             (crash-log! ~indexer e# data# ~state)
             (catch Throwable t#
               (.addSuppressed e# t#)))

           (throw (ex-info msg# data# e#)))))))

(defn- assert-timestamp-col-type [^VectorReader rdr]
  (when-not (or (nil? rdr) (= types/temporal-arrow-type (.getType (.getField rdr))))
    (throw (err/illegal-arg :xtdb/invalid-timestamp-col-type
                            {:col-name (.getName rdr)
                             :field (pr-str (.getType (.getField rdr)))
                             :col-type (types/field->col-type (.getField rdr))}))))

(defn- ->put-docs-indexer ^xtdb.indexer.OpIndexer [^LiveIndex live-idx, ^LiveIndex$Tx live-idx-tx, ^VectorReader tx-ops-rdr,
                                                   ^Database db, ^Instant system-time
                                                   {:keys [indexer tx-key ^Tracer tracer]}]
  (let [db-name (.getName db)
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
                                                 live-table-tx (.liveTable live-idx-tx table)]
                                             (MapEntry/create table
                                                              {:id-rdr (.vectorFor doc-rdr "_id")

                                                               :live-table-tx live-table-tx

                                                               :row-valid-from-rdr (doto (.vectorForOrNull doc-rdr "_valid_from")
                                                                                     (assert-timestamp-col-type))
                                                               :row-valid-to-rdr (doto (.vectorForOrNull doc-rdr "_valid_to")
                                                                                   (assert-timestamp-col-type))

                                                               :docs-rdr table-docs-rdr

                                                               :doc-copier (.rowCopier table-doc-rdr
                                                                                       (.getDocWriter live-table-tx))})))))))))]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [^TableRef table (table/->ref db-name (.getLeg docs-rdr tx-op-idx))

              {:keys [^VectorReader docs-rdr, ^VectorReader id-rdr, ^LiveTable$Tx live-table-tx, ^RowCopier doc-copier
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
          (metrics/with-span tracer "xtdb.transaction.put-docs" {:attributes {:db (.getDbName table)
                                                                              :schema (.getSchemaName table)
                                                                              :table (.getTableName table)}}
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

                (with-crash-log indexer "error putting document"
                  {:table table, :tx-key tx-key, :tx-op-idx tx-op-idx, :doc-idx doc-idx}
                  {:live-idx live-idx, :live-table-tx live-table-tx, :tx-ops-rdr tx-ops-rdr}

                  (.logPut live-table-tx
                           (if iid-rdr
                             (.getBytes iid-rdr (+ iid-start-idx row-idx))
                             (ByteBuffer/wrap (util/->iid (.getObject id-rdr doc-idx))))
                           valid-from valid-to
                           #(.copyRow doc-copier doc-idx)))))))

        nil))))

(defn- ->delete-docs-indexer ^xtdb.indexer.OpIndexer [^LiveIndex live-idx, ^LiveIndex$Tx live-idx-tx, ^VectorReader tx-ops-rdr,
                                                      ^Database db, ^Instant current-time
                                                      {:keys [indexer tx-key ^Tracer tracer]}]
  (let [db-name (.getName db)
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
              live-table-tx (.liveTable live-idx-tx table)
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

          (metrics/with-span tracer "xtdb.transaction.delete-docs" {:attributes {:db (.getDbName table)
                                                                                 :schema (.getSchemaName table)
                                                                                 :table (.getTableName table)}}
            (let [iids-start-idx (.getListStartIndex iids-rdr tx-op-idx)]
              (dotimes [iid-idx (.getListCount iids-rdr tx-op-idx)]
                (with-crash-log indexer "error deleting documents"
                  {:table table, :tx-key tx-key, :tx-op-idx tx-op-idx, :iid-idx iid-idx}
                  {:live-idx live-idx, :live-table-tx live-table-tx, :tx-ops-rdr tx-ops-rdr}

                  (.logDelete live-table-tx (.getBytes iid-rdr (+ iids-start-idx iid-idx))
                              valid-from valid-to))))))

        nil))))

(defn- ->erase-docs-indexer ^xtdb.indexer.OpIndexer [^LiveIndex live-idx, ^LiveIndex$Tx live-idx-tx, ^VectorReader tx-ops-rdr
                                                     ^Database db, {:keys [indexer tx-key ^Tracer tracer]}]
  (let [db-name (.getName db)
        erase-leg (.vectorFor tx-ops-rdr "erase-docs")
        table-rdr (.vectorFor erase-leg "table")
        iids-rdr (.vectorFor erase-leg "iids")
        iid-rdr (.getListElements iids-rdr)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [^TableRef table (table/->ref db-name (.getObject table-rdr tx-op-idx))
              live-table (.liveTable live-idx-tx table)
              iids-start-idx (.getListStartIndex iids-rdr tx-op-idx)]

          (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

          (metrics/with-span tracer "xtdb.transaction.erase-docs" {:attributes {:db (.getDbName table)
                                                                                :schema (.getSchemaName table)
                                                                                :table (.getTableName table)}}
            (dotimes [iid-idx (.getListCount iids-rdr tx-op-idx)]
              (with-crash-log indexer "error erasing documents"
                {:table table, :tx-key tx-key, :tx-op-idx tx-op-idx, :iid-idx iid-idx}
                {:live-idx live-idx, :live-table live-table, :tx-ops-rdr tx-ops-rdr}

                (.logErase live-table (.getBytes iid-rdr (+ iids-start-idx iid-idx)))))))

        nil))))

(defn- ->upsert-rel-indexer ^xtdb.indexer.RelationIndexer [^LiveIndex live-idx, ^LiveIndex$Tx live-idx-tx, ^VectorReader tx-ops-rdr,
                                                           {:keys [^Instant current-time, indexer tx-key]}]

  (let [current-time-µs (time/instant->micros current-time)]
    (reify RelationIndexer
      (indexOp [_ in-rel {:keys [table]}]
        (let [row-count (.getRowCount in-rel)]
          (when (pos? row-count)
            (let [content-rel (vr/rel-reader (->> in-rel
                                                  (remove (comp types/temporal-column? #(.getName ^VectorReader %))))
                                             (.getRowCount in-rel))
                  _ (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

                  id-col (.vectorForOrNull in-rel "_id")
                  valid-from-rdr (.vectorForOrNull in-rel "_valid_from")
                  valid-to-rdr (.vectorForOrNull in-rel "_valid_to")

                  live-table-tx (.liveTable live-idx-tx table)]

              (with-crash-log indexer "error upserting rows"
                {:table table, :tx-key tx-key}
                {:live-idx live-idx, :live-table-tx live-table-tx, :query-rel in-rel, :tx-ops-rdr tx-ops-rdr}
                (let [live-idx-table-copier (.rowCopier (RelationAsStructReader. "content" content-rel)
                                                        (.getDocWriter live-table-tx))]
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
                                          (.logPut live-table-tx (ByteBuffer/wrap (util/->iid eid)) valid-from valid-to #(.copyRow live-idx-table-copier idx)))))))))))))))

(defn- ->delete-rel-indexer ^xtdb.indexer.RelationIndexer [^LiveIndex live-idx, ^LiveIndex$Tx live-idx-tx, ^VectorReader tx-ops-rdr, {:keys [indexer tx-key]}]
  (reify RelationIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [row-count (.getRowCount in-rel)
            iid-rdr (.vectorFor in-rel "_iid")
            valid-from-rdr (.vectorFor in-rel "_valid_from")
            valid-to-rdr (.vectorFor in-rel "_valid_to")]

        (when (xt-log/forbidden-table? table)
          (throw (xt-log/forbidden-table-ex table)))

        (dotimes [idx row-count]
          (with-crash-log indexer "error deleting rows"
            {:table table, :tx-key tx-key, :row-idx idx}
            {:live-idx live-idx, :live-table-tx live-idx-tx, :query-rel in-rel, :tx-ops-rdr tx-ops-rdr}

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

              (-> (.liveTable live-idx-tx table)
                  (.logDelete iid valid-from valid-to)))))))))

(defn- ->erase-rel-indexer ^xtdb.indexer.RelationIndexer [^LiveIndex live-idx, ^LiveIndex$Tx live-idx-tx, ^VectorReader tx-ops-rdr, {:keys [indexer tx-key]}]
  (reify RelationIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [row-count (.getRowCount in-rel)
            iid-rdr (.vectorForOrNull in-rel "_iid")]

        (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

        (dotimes [idx row-count]
          (with-crash-log indexer "error erasing rows"
            {:table table, :tx-key tx-key, :row-idx idx}
            {:live-idx live-idx, :live-table-tx live-idx-tx, :query-rel in-rel, :tx-ops-rdr tx-ops-rdr}

            (let [iid (.getBytes iid-rdr idx)]
              (-> (.liveTable live-idx-tx table)
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

(defn- patch-rel! [table, ^LiveIndex live-idx, ^LiveTable$Tx live-table, ^RelationReader rel {:keys [indexer tx-key]}]
  (let [row-count (.getRowCount rel)]
    (when (pos? row-count)
      (let [iid-rdr (.vectorForOrNull rel "_iid")
            doc-copier (.rowCopier (.vectorForOrNull rel "doc") (.getDocWriter live-table))
            from-rdr (.vectorForOrNull rel "_valid_from")
            to-rdr (.vectorForOrNull rel "_valid_to")]
        (dotimes [idx row-count]
          (with-crash-log indexer "error patching rows"
            {:table table, :tx-key tx-key, :row-idx idx}
            {:live-idx live-idx, :live-table live-table, :query-rel rel}

            (.logPut live-table
                     (.getBytes iid-rdr idx)
                     (.getLong from-rdr idx)
                     (.getLong to-rdr idx)
                     #(.copyRow doc-copier idx))))))))

(defn- ->patch-docs-indexer [^LiveIndex live-idx, ^LiveIndex$Tx live-idx-tx, ^VectorReader tx-ops-rdr,
                             ^IQuerySource q-src, ^Database$Catalog db-cat, ^Instant system-time
                             {:keys [default-db ^Tracer tracer] :as tx-opts}]
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
                                          (str "Cannot put documents with columns: " (pr-str forbidden-cols))
                                          {:table table, :forbidden-cols forbidden-cols})))

                  (let [live-table (.liveTable live-idx-tx table)]
                    (reify OpIndexer
                      (indexOp [_ tx-op-idx]
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
                            (let [table-info (-> (with-open [snap (.openSnapshot (.getSnapSource (.databaseOrNull db-cat default-db)))]
                                                   (.getSchema snap))
                                                 (sql/xform-table-info default-db))
                                  pq (.prepareQuery q-src (-> (sql/plan-patch {:table-info table-info}
                                                                              {:table table
                                                                               :valid-from valid-from
                                                                               :valid-to valid-to
                                                                               :patch-rel (sql/->QueryExpr '[:table [_iid doc]
                                                                                                             ?patch_docs]
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

                              (with-open [res (.openQuery pq (-> (select-keys tx-opts [:snapshot-token :current-time :default-tz])
                                                                 (assoc :args args, :close-args? false)))]
                                (.forEachRemaining res
                                                   (fn [^RelationReader rel]
                                                     (patch-rel! table live-idx live-table rel tx-opts)))))))))))))]

      (let [tables (->> (.getLegNames docs-rdr)
                        (into {} (keep (fn [table-name]
                                         (when (pos? (.getValueCount (.vectorFor docs-rdr table-name)))
                                           [table-name (->table-idxer table-name)])))))]
        (reify OpIndexer
          (indexOp [_ tx-op-idx]
            (.indexOp ^OpIndexer (get tables (.getLeg docs-rdr tx-op-idx))
                      tx-op-idx)))))))

(def ^:private ^:const user-table #xt/table pg_catalog/pg_user)

(defn- update-pg-user! [^LiveIndex$Tx live-idx-tx, ^TransactionKey tx-key, user, password]
  (let [system-time-µs (time/instant->micros (.getSystemTime tx-key))

        live-table (.liveTable live-idx-tx user-table)
        doc-writer (.getDocWriter live-table)]

    (.logPut live-table (ByteBuffer/wrap (util/->iid user)) system-time-µs Long/MAX_VALUE
             (fn write-doc! []
               (doto (.vectorFor doc-writer "_id" (FieldType/notNullable #xt.arrow/type :utf8))
                 (.writeObject user))

               (doto (.vectorFor doc-writer "username" (FieldType/notNullable #xt.arrow/type :utf8))
                 (.writeObject user))

               (doto (.vectorFor doc-writer "usesuper" (FieldType/notNullable #xt.arrow/type :bool))
                 (.writeObject false))

               (doto (.vectorFor doc-writer "passwd" (FieldType/nullable #xt.arrow/type :utf8))
                 (.writeObject (authn.crypt/encrypt-pw password)))

               (.endStruct doc-writer)))))

(defn- ->sql-indexer ^xtdb.indexer.OpIndexer [^BufferAllocator allocator, ^LiveIndex live-idx, ^LiveIndex$Tx live-idx-tx
                                              ^VectorReader tx-ops-rdr, ^IQuerySource q-src, db-cat,
                                              {:keys [default-db tx-key ^Tracer tracer] :as tx-opts}]
  (let [sql-leg (.vectorFor tx-ops-rdr "sql")
        query-rdr (.vectorFor sql-leg "query")
        args-rdr (.vectorFor sql-leg "args")
        upsert-idxer (->upsert-rel-indexer live-idx live-idx-tx tx-ops-rdr tx-opts)
        patch-idxer (reify RelationIndexer
                      (indexOp [_ rel {:keys [table]}]
                        (patch-rel! table live-idx (.liveTable live-idx-tx table) rel tx-opts)))
        delete-idxer (->delete-rel-indexer live-idx live-idx-tx tx-ops-rdr tx-opts)
        erase-idxer (->erase-rel-indexer live-idx live-idx-tx tx-ops-rdr tx-opts)]
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
                                                   (update-pg-user! live-idx-tx tx-key username password))

                                    :alter-user (let [{:keys [username password]} q-args]
                                                  (update-pg-user! live-idx-tx tx-key username password))

                                    (throw (err/incorrect ::invalid-sql-tx-op "Invalid SQL query sent as transaction operation"
                                                          {:query query-str}))))))))

        nil))))

(defn- add-tx-row! [db-name ^LiveIndex$Tx live-idx-tx, ^TransactionKey tx-key, ^Throwable t, user-metadata]
  (let [tx-id (.getTxId tx-key)
        system-time-µs (time/instant->micros (.getSystemTime tx-key))

        live-table (.liveTable live-idx-tx (table/->ref db-name 'xt/txs))
        doc-writer (.getDocWriter live-table)]

    (.logPut live-table (ByteBuffer/wrap (util/->iid tx-id)) system-time-µs Long/MAX_VALUE
             (fn write-doc! []
               (doto (.vectorFor doc-writer "_id" (FieldType/notNullable #xt.arrow/type :i64))
                 (.writeLong tx-id))

               (doto (.vectorFor doc-writer "system_time" (FieldType/notNullable (st/->arrow-type types/temporal-col-type)))
                 (.writeLong system-time-µs))

               (doto (.vectorFor doc-writer "committed" (FieldType/notNullable #xt.arrow/type :bool))
                 (.writeBoolean (nil? t)))

               (doto (.vectorFor doc-writer "user_metadata" (.getFieldType #xt/type [:? :struct]))
                 (.writeObject user-metadata))

               (let [e-wtr (.vectorFor doc-writer "error" (FieldType/nullable #xt.arrow/type :transit))]
                 (if (nil? t)
                   (.writeNull e-wtr)
                   (try
                     (.writeObject e-wtr t)
                     (catch Exception e
                       (log/warnf (doto t (.addSuppressed e)) "Error serializing error, tx %d" tx-id)
                       (.writeObject e-wtr (xt/->ClojureForm "error serializing error - see server logs"))))))

               (.endStruct doc-writer)))))

(defmethod ig/expand-key :xtdb/indexer [k opts]
  {k (merge {:config (ig/ref :xtdb/config)
             :q-src (ig/ref ::q/query-source)
             :metrics-registry (ig/ref :xtdb.metrics/registry)
             :tracer (ig/ref :xtdb/tracer)}
            opts)})

(defn- commit [tx-key ^LiveIndex$Tx live-idx-tx ^Indexer$TxSink tx-sink]
  (.commit live-idx-tx)
  (some-> tx-sink (.onCommit tx-key live-idx-tx)))

(defrecord IndexerForDatabase [^BufferAllocator allocator, node-id, ^IQuerySource q-src
                               ^Database db, ^LiveIndex live-index, table-catalog
                               ^Timer tx-timer
                               ^Counter tx-error-counter
                               ^Tracer tracer]
  Indexer$ForDatabase
  (close [_]
    (util/close allocator))

  (indexTx [this msg-id msg-ts tx-ops-rdr
            system-time default-tz _user user-metadata]
    (let [db-name (.getName db)
          lc-tx (.getLatestCompletedTx live-index)
          default-system-time (or (when-let [lc-sys-time (some-> lc-tx (.getSystemTime))]
                                    (when-not (neg? (compare lc-sys-time msg-ts))
                                      (.plusNanos lc-sys-time 1000)))
                                  msg-ts)]

      (if (and system-time lc-tx
               (neg? (compare system-time (.getSystemTime lc-tx))))
        (let [tx-key (serde/->TxKey msg-id default-system-time)
              err (err/illegal-arg :invalid-system-time
                                   {::err/message "specified system-time older than current tx"
                                    :tx-key (serde/->TxKey msg-id system-time)
                                    :latest-completed-tx (.getLatestCompletedTx live-index)})]
          (log/warnf "specified system-time '%s' older than current tx '%s'"
                     (pr-str system-time)
                     (pr-str (.getLatestCompletedTx live-index)))

          (util/with-open [live-idx-tx (.startTx live-index tx-key)]
            (when tx-error-counter
              (.increment tx-error-counter))
            (add-tx-row! db-name live-idx-tx tx-key err user-metadata)
            (commit tx-key live-idx-tx (.getTxSink db)))

          (serde/->tx-aborted msg-id default-system-time err))

        (let [system-time (or system-time default-system-time)
              tx-key (serde/->TxKey msg-id system-time)]
          (util/with-open [live-idx-tx (.startTx live-index tx-key)]
            (if (nil? tx-ops-rdr)
              (do
                (.abort live-idx-tx)
                (util/with-open [live-idx-tx (.startTx live-index tx-key)]
                  (add-tx-row! db-name live-idx-tx tx-key skipped-exn user-metadata)
                  (commit tx-key live-idx-tx (.getTxSink db)))

                (serde/->tx-aborted msg-id system-time skipped-exn))

              (let [db (-> db
                           (.withSnapSource
                            (reify Snapshot$Source
                              (openSnapshot [_]
                                (util/with-close-on-catch [live-index-snap (.openSnapshot live-idx-tx)]
                                  (Snapshot. tx-key live-index-snap
                                             (li/->schema live-index-snap table-catalog)))))))

                    db-cat (Database$Catalog/singleton db)

                    tx-opts {:snapshot-token (basis/->time-basis-str {db-name [system-time]})
                             :current-time system-time
                             :default-tz default-tz
                             :tx-key tx-key
                             :indexer this
                             :default-db (.getName db)
                             :user-metadata user-metadata
                             :tracer tracer}

                    !put-docs-idxer (delay (->put-docs-indexer live-index live-idx-tx tx-ops-rdr db system-time tx-opts))
                    !patch-docs-idxer (delay (->patch-docs-indexer live-index live-idx-tx tx-ops-rdr q-src db-cat system-time tx-opts))
                    !delete-docs-idxer (delay (->delete-docs-indexer live-index live-idx-tx tx-ops-rdr db system-time tx-opts))
                    !erase-docs-idxer (delay (->erase-docs-indexer live-index live-idx-tx tx-ops-rdr db tx-opts))
                    !sql-idxer (delay (->sql-indexer allocator live-index live-idx-tx tx-ops-rdr q-src db-cat tx-opts))]

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
                    (.abort live-idx-tx)

                    (util/with-open [live-idx-tx (.startTx live-index tx-key)]
                      (when tx-error-counter
                        (.increment tx-error-counter))
                      (add-tx-row! db-name live-idx-tx tx-key e user-metadata)
                      (commit tx-key live-idx-tx (.getTxSink db)))

                    (serde/->tx-aborted msg-id system-time e))

                  (do
                    (add-tx-row! db-name live-idx-tx tx-key nil user-metadata)
                    (commit tx-key live-idx-tx (.getTxSink db))
                    (serde/->tx-committed msg-id system-time))))))))))

  (addTxRow [_ tx-key e]
    (util/with-open [live-idx-tx (.startTx live-index tx-key)]
      (add-tx-row! (.getName db) live-idx-tx tx-key e {})
      (commit tx-key live-idx-tx (.getTxSink db)))))

(defmethod ig/init-key :xtdb/indexer [_ {:keys [config, q-src, metrics-registry, ^Tracer tracer]}]
  (let [tx-timer (metrics/add-timer metrics-registry "tx.op.timer"
                                    {:description "indicates the timing and number of transactions"})
        tx-error-counter (metrics/add-counter metrics-registry "tx.error")]
    (reify Indexer
      (openForDatabase [_ db]
        (util/with-close-on-catch [allocator (-> (.getAllocator db) (util/->child-allocator (str "indexer/" (.getName db))))]
          ;; TODO add db-name to allocator gauge
          (metrics/add-allocator-gauge metrics-registry "indexer.allocator.allocated_memory" allocator)

          (->IndexerForDatabase allocator (:node-id config) q-src
                                db (.getLiveIndex db) (.getTableCatalog db)
                                tx-timer tx-error-counter tracer)))

      (close [_]))))

(defmethod ig/halt-key! :xtdb/indexer [_ indexer]
  (util/close indexer))

(defmethod ig/expand-key ::for-db [k {:keys [base]}]
  {k {:base base
      :query-db (ig/ref :xtdb.db-catalog/for-query)
      :tx-sink (ig/ref :xtdb.tx-sink/for-db)}})

(defmethod ig/init-key ::for-db [_ {{:keys [^Indexer indexer]} :base,
                                    :keys [query-db]}]
  (.openForDatabase indexer query-db))

(defmethod ig/halt-key! ::for-db [_ indexer-for-db]
  (util/close indexer-for-db))

(defn <-node ^xtdb.indexer.Indexer [node]
  (util/component node :xtdb/indexer))
