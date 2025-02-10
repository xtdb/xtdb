(ns xtdb.indexer
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [sci.core :as sci]
            [xtdb.api :as xt]
            [xtdb.authn :as authn]
            [xtdb.error :as err]
            [xtdb.indexer.live-index :as li]
            [xtdb.log :as xt-log]
            [xtdb.logical-plan :as lp]
            [xtdb.metadata :as meta]
            [xtdb.metrics :as metrics]
            [xtdb.operator.scan :as scan]
            [xtdb.query :as q]
            [xtdb.rewrite :as r :refer [zmatch]]
            [xtdb.serde :as serde]
            [xtdb.sql :as sql]
            [xtdb.sql.plan :as plan]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            [xtdb.xtql.plan :as xtql])
  (:import (clojure.lang MapEntry)
           (io.micrometer.core.instrument Counter Timer)
           (java.io ByteArrayInputStream Closeable)
           (java.nio ByteBuffer)
           java.nio.channels.ClosedByInterruptException
           (java.time Instant InstantSource ZoneId)
           (java.time.temporal ChronoUnit)
           (java.util.function Consumer)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector)
           (org.apache.arrow.vector.ipc ArrowReader ArrowStreamReader)
           (org.apache.arrow.vector.types.pojo FieldType)
           xtdb.api.TransactionKey
           (xtdb.api.tx TxOp)
           (xtdb.arrow RowCopier)
           xtdb.BufferPool
           (xtdb.indexer IIndexer LiveIndex LiveIndex$Tx LiveTable$Tx OpIndexer RelationIndexer Watermark Watermark$Source)
           xtdb.metadata.IMetadataManager
           (xtdb.query IQuerySource PreparedQuery)
           xtdb.types.ClojureForm
           (xtdb.vector IVectorReader RelationAsStructReader RelationReader SingletonListReader)))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private abort-exn (err/runtime-err :abort-exn))

(def ^:dynamic ^java.time.InstantSource *crash-log-clock* (InstantSource/system))

(defn crash-log! [{:keys [allocator, ^BufferPool buffer-pool, node-id]} ex data {:keys [^LiveTable$Tx live-table-tx, ^RelationReader query-rel]}]
  (let [ts (str (.truncatedTo (.instant *crash-log-clock*) ChronoUnit/SECONDS))
        crash-dir (util/->path (format "crashes/%s/%s" node-id ts))]
    (log/warn "writing a crash log:" (str crash-dir) (pr-str data))

    (let [^String crash-edn (with-out-str
                              (pp/pprint (-> data (assoc :ex ex))))]
      (.putObject buffer-pool (.resolve crash-dir "crash.edn")
                  (ByteBuffer/wrap (.getBytes crash-edn))))

    (when live-table-tx
      (with-open [live-rel (.openAsRelation (doto (.getLiveRelation live-table-tx)
                                              (.syncRowCount)))
                  wtr (.openArrowWriter buffer-pool (.resolve crash-dir "live-table.arrow") live-rel)]
        (.writePage wtr)
        (.end wtr)))

    (when query-rel
      (with-open [query-rel (.openAsRelation query-rel allocator)
                  wtr (.openArrowWriter buffer-pool (.resolve crash-dir "query-rel.arrow") query-rel)]
        (.writePage wtr)
        (.end wtr)))

    (log/info "crash log written:" (str crash-dir))))

(defmacro with-crash-log [indexer msg data state & body]
  `(try
     ~@body
     (catch InterruptedException e# (throw e#))
     (catch ClosedByInterruptException e# (throw e#))
     (catch Exception e#
       (let [{node-id# :node-id, :as indexer#} ~indexer
             msg# ~msg
             data# (-> ~data
                       (assoc :node-id node-id#))]
         (try
           (crash-log! ~indexer e# data# ~state)
           (catch Throwable t#
             (.addSuppressed e# t#)))

         (throw (ex-info msg# data# e#))))))

(defn- ->put-docs-indexer ^xtdb.indexer.OpIndexer [^LiveIndex$Tx live-idx-tx,
                                                   ^IVectorReader tx-ops-rdr, ^Instant system-time
                                                   {:keys [indexer tx-key]}]
  (let [put-leg (.legReader tx-ops-rdr "put-docs")
        iids-rdr (.structKeyReader put-leg "iids")
        iid-rdr (.listElementReader iids-rdr)
        docs-rdr (.structKeyReader put-leg "documents")

        ;; HACK: can remove this once we're sure a few more people have migrated their logs
        ^IVectorReader valid-from-rdr (or (.structKeyReader put-leg "_valid_from")
                                          (when (.structKeyReader put-leg "xt$valid_from")
                                            (throw (IllegalStateException. "Legacy log format - see https://github.com/xtdb/xtdb/pull/3675 for more details"))))

        valid-to-rdr (.structKeyReader put-leg "_valid_to")
        system-time-µs (time/instant->micros system-time)
        tables (->> (.legs docs-rdr)
                    (into {} (map (fn [table-name]
                                    (when (xt-log/forbidden-table? table-name) (throw (xt-log/forbidden-table-ex table-name)))

                                    (let [table-docs-rdr (.legReader docs-rdr table-name)
                                          doc-rdr (.listElementReader table-docs-rdr)
                                          ks (.structKeys doc-rdr)]
                                      (when-let [forbidden-cols (not-empty (->> ks
                                                                                (into #{} (filter (every-pred #(str/starts-with? % "_")
                                                                                                              (complement #{"_id" "_fn"}))))))]
                                        (throw (err/illegal-arg :xtdb/forbidden-columns
                                                                {::err/message (str "Cannot put documents with columns: " (pr-str forbidden-cols))
                                                                 :table-name table-name
                                                                 :forbidden-cols forbidden-cols})))
                                      (let [^RelationReader table-rel-rdr (vr/rel-reader (for [sk ks]
                                                                                           (.structKeyReader doc-rdr sk))
                                                                                         (.valueCount doc-rdr))
                                            live-table-tx (.liveTable live-idx-tx table-name)]
                                        (MapEntry/create table-name
                                                         {:id-rdr (.structKeyReader doc-rdr "_id")

                                                          :live-table-tx live-table-tx

                                                          :docs-rdr table-docs-rdr

                                                          :doc-copier (-> (.getDocWriter live-table-tx)
                                                                          (.rowCopier table-rel-rdr))})))))))]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [table-name (.getLeg docs-rdr tx-op-idx)

              {:keys [^IVectorReader docs-rdr, ^IVectorReader id-rdr, ^LiveTable$Tx live-table-tx, ^RowCopier doc-copier]}
              (get tables table-name)

              valid-from (if (.isNull valid-from-rdr tx-op-idx)
                           system-time-µs
                           (.getLong valid-from-rdr tx-op-idx))
              valid-to (if (.isNull valid-to-rdr tx-op-idx)
                         Long/MAX_VALUE
                         (.getLong valid-to-rdr tx-op-idx))]
          (when-not (> valid-to valid-from)
            (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                    {:valid-from (time/micros->instant valid-from)
                                     :valid-to (time/micros->instant valid-to)})))

          (let [doc-start-idx (.getListStartIndex docs-rdr tx-op-idx)
                ^long iid-start-idx (or (some-> iids-rdr (.getListStartIndex tx-op-idx))
                                        Long/MIN_VALUE)]
            (dotimes [row-idx (.getListCount docs-rdr tx-op-idx)]
              (let [doc-idx (+ doc-start-idx row-idx)]
                (with-crash-log indexer "error putting document"
                    {:table-name table-name :tx-key tx-key, :tx-op-idx tx-op-idx, :doc-idx doc-idx}
                    {:live-table-tx live-table-tx}

                  (.logPut live-table-tx
                           (if iid-rdr
                             (.getBytes iid-rdr (+ iid-start-idx row-idx))
                             (util/->iid (.getObject id-rdr doc-idx)))
                           valid-from valid-to
                           #(.copyRow doc-copier doc-idx)))))))

        nil))))

(defn- ->delete-docs-indexer ^xtdb.indexer.OpIndexer [^LiveIndex$Tx live-idx-tx, ^IVectorReader tx-ops-rdr, ^Instant current-time
                                                      {:keys [indexer tx-key]}]
  (let [delete-leg (.legReader tx-ops-rdr "delete-docs")
        table-rdr (.structKeyReader delete-leg "table")
        iids-rdr (.structKeyReader delete-leg "iids")
        iid-rdr (.listElementReader iids-rdr)
        valid-from-rdr (.structKeyReader delete-leg "_valid_from")
        valid-to-rdr (.structKeyReader delete-leg "_valid_to")
        current-time-µs (time/instant->micros current-time)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [table (.getObject table-rdr tx-op-idx)
              live-table-tx (.liveTable live-idx-tx table)
              valid-from (if (.isNull valid-from-rdr tx-op-idx)
                           current-time-µs
                           (.getLong valid-from-rdr tx-op-idx))
              valid-to (if (.isNull valid-to-rdr tx-op-idx)
                         Long/MAX_VALUE
                         (.getLong valid-to-rdr tx-op-idx))]

          (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

          (when (> valid-from valid-to)
            (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                    {:valid-from (time/micros->instant valid-from)
                                     :valid-to (time/micros->instant valid-to)})))

          (let [iids-start-idx (.getListStartIndex iids-rdr tx-op-idx)]
            (dotimes [iid-idx (.getListCount iids-rdr tx-op-idx)]
              (with-crash-log indexer "error deleting documents"
                  {:table-name table :tx-key tx-key, :tx-op-idx tx-op-idx, :iid-idx iid-idx}
                  {:live-table-tx live-table-tx}

                (.logDelete live-table-tx (.getBytes iid-rdr (+ iids-start-idx iid-idx))
                            valid-from valid-to)))))

        nil))))

(defn- ->erase-docs-indexer ^xtdb.indexer.OpIndexer [^LiveIndex$Tx live-idx-tx, ^IVectorReader tx-ops-rdr
                                                     {:keys [indexer tx-key]}]
  (let [erase-leg (.legReader tx-ops-rdr "erase-docs")
        table-rdr (.structKeyReader erase-leg "table")
        iids-rdr (.structKeyReader erase-leg "iids")
        iid-rdr (.listElementReader iids-rdr)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [table (.getObject table-rdr tx-op-idx)
              live-table (.liveTable live-idx-tx table)
              iids-start-idx (.getListStartIndex iids-rdr tx-op-idx)]

          (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

          (dotimes [iid-idx (.getListCount iids-rdr tx-op-idx)]
            (with-crash-log indexer "error erasing documents"
                {:table-name table :tx-key tx-key, :tx-op-idx tx-op-idx, :iid-idx iid-idx}
                {:live-table live-table}

              (.logErase live-table (.getBytes iid-rdr (+ iids-start-idx iid-idx))))))

        nil))))

(defn- find-fn [allocator ^IQuerySource q-src, wm-src, sci-ctx tx-opts fn-iid]
  (let [lp '[:scan {:table xt/tx_fns} [{_iid (= _iid ?iid)} _id fn]]
        ^PreparedQuery pq (.prepareRaQuery q-src lp wm-src tx-opts)]
    (with-open [^RelationReader
                args (vr/rel-reader [(-> (vw/open-vec allocator '?iid [fn-iid])
                                         (vr/vec->reader))]
                                    1)

                bq (.bind pq
                          (-> (select-keys tx-opts [:snapshot-time :current-time :default-tz])
                              (assoc :args args, :close-args? false)))

                res (.openCursor bq)]

      (let [!fn-doc (object-array 1)]
        (.tryAdvance res
                     (reify Consumer
                       (accept [_ in-rel]
                         (when (pos? (.rowCount ^RelationReader in-rel))
                           (aset !fn-doc 0 (first (vr/rel->rows in-rel)))))))

        (let [{fn-id :xt/id, fn-body :fn, :as fn-doc} (or (aget !fn-doc 0)
                                                          (throw (err/runtime-err :xtdb.call/no-such-tx-fn
                                                                                  {:fn-iid (util/byte-buffer->uuid fn-iid)})))]

          (when-not (instance? ClojureForm fn-body)
            (throw (err/illegal-arg :xtdb.call/invalid-tx-fn {:fn-doc (dissoc fn-doc :xt/iid)})))

          (let [fn-form (.form ^ClojureForm fn-body)]
            (try
              {:tx-fn (sci/eval-form sci-ctx fn-form)
               :fn-id fn-id}

              (catch Throwable t
                (throw (err/runtime-err :xtdb.call/error-compiling-tx-fn {:fn-form fn-form} t))))))))))

(defn- tx-fn-q [allocator ^IQuerySource q-src wm-src tx-opts]
  (fn tx-fn-q*
    ([query] (tx-fn-q* query {}))

    ([query opts]
     (let [{:keys [args] :as query-opts} (-> (reduce into [{:key-fn :kebab-case-keyword} tx-opts opts])
                                             (update :key-fn serde/read-key-fn))
           prepared-query (.prepareRaQuery q-src (.planQuery q-src query wm-src query-opts) wm-src query-opts)]

       (util/with-open [args (when args
                               (vw/open-args allocator args))
                        res (-> (.bind prepared-query (assoc query-opts :args args, :close-args? false))
                                (q/open-cursor-as-stream query-opts))]
         (vec (.toList res)))))))

(def ^:private !last-tx-fn-error (atom nil))

(defn reset-tx-fn-error! []
  (first (reset-vals! !last-tx-fn-error nil)))

(def ^:private xt-sci-ns
  (-> (sci/copy-ns xtdb.api (sci/create-ns 'xtdb.api))
      (select-keys ['put 'put-fn
                    'during 'starting-at 'until])))

(defn- ->call-indexer ^xtdb.indexer.OpIndexer [allocator, q-src, wm-src
                                               ^IVectorReader tx-ops-rdr, {:keys [tx-key] :as tx-opts}]
  (let [call-leg (.legReader tx-ops-rdr "call")
        fn-id-rdr (.structKeyReader call-leg "fn-id")
        fn-iid-rdr (.structKeyReader call-leg "fn-iid")
        args-rdr (.structKeyReader call-leg "args")

        ;; TODO confirm/expand API that we expose to tx-fns
        sci-ctx (sci/init {:bindings {'q (tx-fn-q allocator q-src wm-src tx-opts)
                                      'sleep (fn [^long n] (Thread/sleep n))
                                      '*current-tx* tx-key}
                           :namespaces {'xt xt-sci-ns}})]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (try
          (let [fn-iid (if fn-iid-rdr
                         (.getBytes fn-iid-rdr tx-op-idx)
                         (util/->iid (.getObject fn-id-rdr tx-op-idx)))
                {:keys [fn-id tx-fn]} (find-fn allocator q-src wm-src (sci/fork sci-ctx) tx-opts fn-iid)
                args (.form ^ClojureForm (.getObject args-rdr tx-op-idx))

                res (try
                      (sci/binding [sci/out *out*
                                    sci/in *in*]
                        (let [res (apply tx-fn args)]
                          (cond->> res
                            (seqable? res) (mapv (fn [tx-op]
                                                   (cond-> tx-op
                                                     (not (instance? TxOp tx-op)) tx-ops/parse-tx-op))))))
                      (catch InterruptedException ie (throw ie))
                      (catch xtdb.IllegalArgumentException e
                        (log/warn e "unhandled error evaluating tx fn")
                        (throw e))
                      (catch xtdb.RuntimeException e
                        (log/warn e "unhandled error evaluating tx fn")
                        (throw e))
                      (catch Throwable t
                        (log/warn t "unhandled error evaluating tx fn")
                        (throw (err/runtime-err :xtdb.call/error-evaluating-tx-fn
                                                {:fn-id fn-id, :args args}
                                                t))))]
            (when (false? res)
              (throw abort-exn))

            ;; if the user returns `nil` or `true`, we just continue with the rest of the transaction
            (when-not (or (nil? res) (true? res))
              ;; HACK an adapter 'til this is all migrated to xtdb.arrow
              (util/with-open [tx-ops-rel (xt-log/open-tx-ops-rel allocator)]
                (let [xt-vec (.get tx-ops-rel "tx-ops")]
                  (xt-log/write-tx-ops! allocator xt-vec res tx-opts)
                  (.setRowCount tx-ops-rel (.getValueCount xt-vec)))
                (-> (.openAsRoot tx-ops-rel allocator)
                    (.getVector "tx-ops")))))

          (catch Throwable t
            (reset! !last-tx-fn-error t)
            (throw t)))))))

(defn- ->upsert-rel-indexer ^xtdb.indexer.RelationIndexer [^LiveIndex$Tx live-idx-tx
                                                           {:keys [^Instant current-time, indexer tx-key]}]

  (let [current-time-µs (time/instant->micros current-time)]
    (reify RelationIndexer
      (indexOp [_ in-rel {:keys [table]}]
        (let [row-count (.rowCount in-rel)
              ^RelationReader content-rel (vr/rel-reader (->> in-rel
                                                              (remove (comp types/temporal-column? #(.getName ^IVectorReader %))))
                                                         (.rowCount in-rel))
              table (str table)

              _ (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

              id-col (.readerForName in-rel "_id")
              valid-from-rdr (.readerForName in-rel "_valid_from")
              valid-to-rdr (.readerForName in-rel "_valid_to")

              live-table-tx (.liveTable live-idx-tx table)
              live-idx-table-copier (-> (.getDocWriter live-table-tx)
                                        (.rowCopier content-rel))]

          (when-not id-col
            (throw (err/runtime-err :xtdb.indexer/missing-xt-id-column
                                    {:column-names (vec (for [^IVectorReader col in-rel] (.getName col)))})))

          (dotimes [idx row-count]
            (with-crash-log indexer "error upserting rows"
                {:table-name table, :tx-key tx-key, :row-idx idx}
                {:live-table-tx live-table-tx, :query-rel in-rel}

              (let [eid (.getObject id-col idx)
                    valid-from (if (and valid-from-rdr (not (.isNull valid-from-rdr idx)))
                                 (.getLong valid-from-rdr idx)
                                 current-time-µs)
                    valid-to (if (and valid-to-rdr (not (.isNull valid-to-rdr idx)))
                               (.getLong valid-to-rdr idx)
                               Long/MAX_VALUE)]
                (when (> valid-from valid-to)
                  (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                          {:valid-from (time/micros->instant valid-from)
                                           :valid-to (time/micros->instant valid-to)})))

                ;; FIXME something in the generated SQL generates rows with `(= vf vt)`, which is also unacceptable
                (when (< valid-from valid-to)
                  (.logPut live-table-tx (util/->iid eid) valid-from valid-to #(.copyRow live-idx-table-copier idx)))))))))))

(defn- ->delete-rel-indexer ^xtdb.indexer.RelationIndexer [^LiveIndex$Tx live-idx-tx
                                                           {:keys [indexer tx-key]}]
  (reify RelationIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [table (str table)
            row-count (.rowCount in-rel)
            iid-rdr (.readerForName in-rel "_iid")
            valid-from-rdr (.readerForName in-rel "_valid_from")
            valid-to-rdr (.readerForName in-rel "_valid_to")]

        (when (xt-log/forbidden-table? table)
          (throw (xt-log/forbidden-table-ex table)))

        (dotimes [idx row-count]
          (with-crash-log indexer "error deleting rows"
              {:table-name table, :tx-key tx-key, :row-idx idx}
              {:live-table-tx live-idx-tx, :query-rel in-rel}

            (let [iid (.getBytes iid-rdr idx)
                  valid-from (.getLong valid-from-rdr idx)
                  valid-to (if (.isNull valid-to-rdr idx)
                             Long/MAX_VALUE
                             (.getLong valid-to-rdr idx))]
              (when-not (< valid-from valid-to)
                (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                        {:valid-from (time/micros->instant valid-from)
                                         :valid-to (time/micros->instant valid-to)})))

              (-> (.liveTable live-idx-tx table)
                  (.logDelete iid valid-from valid-to)))))))))

(defn- ->erase-rel-indexer ^xtdb.indexer.RelationIndexer [^LiveIndex$Tx live-idx-tx {:keys [indexer tx-key]}]
  (reify RelationIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [table (str table)
            row-count (.rowCount in-rel)
            iid-rdr (.readerForName in-rel "_iid")]

        (when (xt-log/forbidden-table? table) (throw (xt-log/forbidden-table-ex table)))

        (dotimes [idx row-count]
          (with-crash-log indexer "error erasing rows"
              {:table-name table, :tx-key tx-key, :row-idx idx}
              {:live-table-tx live-idx-tx, :query-rel in-rel}

            (let [iid (.getBytes iid-rdr idx)]
            (-> (.liveTable live-idx-tx table)
                (.logErase iid)))))))))

(defn- ->assert-idxer ^xtdb.indexer.RelationIndexer [^IQuerySource q-src, wm-src, query, tx-opts]
  (let [^PreparedQuery pq (.prepareRaQuery q-src query wm-src tx-opts)]
    (fn eval-query [^RelationReader args]
      (with-open [res (-> (.bind pq (-> (select-keys tx-opts [:snapshot-time :current-time :default-tz])
                                        (assoc :args args, :close-args? false)))
                          (.openCursor))]

        (letfn [(throw-assert-failed []
                  (throw (err/runtime-err :xtdb/assert-failed
                                          {::err/message "Assert failed"})))]
          (or (.tryAdvance res
                           (reify Consumer
                             (accept [_ in-rel]
                               (let [^RelationReader in-rel in-rel]
                                 (when-not (pos? (.rowCount in-rel))
                                   (throw-assert-failed))))))

              (throw-assert-failed)))

        (assert (not (.tryAdvance res nil))
                "only expecting one batch in assert")))))

(defn- query-indexer [^IQuerySource q-src, wm-src, ^RelationIndexer rel-idxer, query, tx-opts, query-opts]
  (let [^PreparedQuery pq (.prepareRaQuery q-src query wm-src tx-opts)]
    (fn eval-query [^RelationReader args]
      (with-open [res (-> (.bind pq (-> (select-keys tx-opts [:snapshot-time :current-time :default-tz])
                                        (assoc :args args, :close-args? false)))
                          (.openCursor))]

        (.forEachRemaining res
                           (reify Consumer
                             (accept [_ in-rel]
                               (.indexOp rel-idxer in-rel query-opts))))))))

(defn- open-args-rdr ^org.apache.arrow.vector.ipc.ArrowReader [^BufferAllocator allocator, ^IVectorReader args-rdr, ^long tx-op-idx]
  (when-not (.isNull args-rdr tx-op-idx)
    (let [is (ByteArrayInputStream. (.array ^ByteBuffer (.getObject args-rdr tx-op-idx)))] ; could try to use getBytes
      (ArrowStreamReader. is allocator))))

(defn- foreach-arg-row [^ArrowReader asr, eval-query]
  (if-not asr
    (eval-query nil)

    (let [param-root (.getVectorSchemaRoot asr)]
      (while (.loadNextBatch asr)
        (let [param-rel (vr/<-root param-root)
              selection (int-array 1)]
          (dotimes [idx (.rowCount param-rel)]
            (aset selection 0 idx)
            (eval-query (-> param-rel (.select selection)))))))))

(defn- patch-rel! [table-name ^LiveTable$Tx live-table, ^RelationReader rel {:keys [indexer tx-key]}]
  (let [iid-rdr (.readerForName rel "_iid")
        doc-copier (.rowCopier (.readerForName rel "doc") (.getDocWriter live-table))
        from-rdr (.readerForName rel "_valid_from")
        to-rdr (.readerForName rel "_valid_to")]
    (dotimes [idx (.rowCount rel)]
      (with-crash-log indexer "error patching rows"
          {:table-name table-name, :tx-key tx-key, :row-idx idx}
          {:live-table live-table, :query-rel rel}

        (.logPut live-table
                 (.getBytes iid-rdr idx)
                 (.getLong from-rdr idx)
                 (.getLong to-rdr idx)
                 #(.copyRow doc-copier idx))))))

(defn- ->patch-docs-indexer [^LiveIndex$Tx live-idx-tx, ^IVectorReader tx-ops-rdr,
                             ^IQuerySource q-src, wm-src,
                             {:keys [snapshot-time] :as tx-opts}]
  (let [patch-leg (.legReader tx-ops-rdr "patch-docs")
        iids-rdr (.structKeyReader patch-leg "iids")
        iid-rdr (.listElementReader iids-rdr)
        docs-rdr (.structKeyReader patch-leg "documents")

        valid-from-rdr (.structKeyReader patch-leg "_valid_from")
        valid-to-rdr (.structKeyReader patch-leg "_valid_to")

        system-time-µs (time/instant->micros snapshot-time)]
    (letfn [(->table-idxer [table-name]
              (when (xt-log/forbidden-table? table-name)
                (throw (xt-log/forbidden-table-ex table-name)))

              (let [table-docs-rdr (.legReader docs-rdr table-name)
                    doc-rdr (.listElementReader table-docs-rdr)
                    ks (.structKeys doc-rdr)]
                (when-let [forbidden-cols (not-empty (->> ks
                                                          (into #{} (filter (every-pred #(str/starts-with? % "_")
                                                                                        (complement #{"_id" "_fn"}))))))]
                  (throw (err/illegal-arg :xtdb/forbidden-columns
                                          {::err/message (str "Cannot put documents with columns: " (pr-str forbidden-cols))
                                           :table-name table-name
                                           :forbidden-cols forbidden-cols})))

                (let [live-table (.liveTable live-idx-tx table-name)]
                  (reify OpIndexer
                    (indexOp [_ tx-op-idx]
                      (let [valid-from (time/micros->instant (if (.isNull valid-from-rdr tx-op-idx)
                                                               system-time-µs
                                                               (.getLong valid-from-rdr tx-op-idx)))
                            valid-to (when-not (.isNull valid-to-rdr tx-op-idx)
                                       (time/micros->instant (.getLong valid-to-rdr tx-op-idx)))]
                        (when-not (or (nil? valid-to) (pos? (compare valid-to valid-from)))
                          (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                                  {:valid-from valid-from
                                                   :valid-to valid-to})))

                        (let [pq (.prepareRaQuery q-src (-> (plan/plan-patch {:table-info (plan/xform-table-info (scan/tables-with-cols wm-src))}
                                                                             {:table (symbol table-name)
                                                                              :valid-from valid-from
                                                                              :valid-to valid-to
                                                                              :patch-rel (plan/->QueryExpr '[:table [_iid doc]
                                                                                                             ?patch_docs]
                                                                                                           '[_iid doc])})
                                                            (lp/rewrite-plan))
                                                  wm-src tx-opts)
                              args (vr/rel-reader [(SingletonListReader.
                                                    "?patch_docs"
                                                    (RelationAsStructReader.
                                                     "patch_doc"
                                                     (vr/rel-reader [(-> (.select iid-rdr (.getListStartIndex iids-rdr tx-op-idx) (.getListCount iids-rdr tx-op-idx))
                                                                         (.withName "_iid"))
                                                                     (-> (.select doc-rdr (.getListStartIndex table-docs-rdr tx-op-idx) (.getListCount table-docs-rdr tx-op-idx))
                                                                         (.withName "doc"))])))])]

                          (with-open [bq (.bind pq
                                                (-> (select-keys tx-opts [:snapshot-time :current-time :default-tz])
                                                    (assoc :args args
                                                           :close-args? false)))
                                      res (.openCursor bq)]
                            (.forEachRemaining res
                                               (fn [^RelationReader rel]
                                                 (patch-rel! table-name live-table rel tx-opts)))))))))))]

      (let [tables (->> (.legs docs-rdr)
                        (into {} (map (juxt identity ->table-idxer))))]
        (reify OpIndexer
          (indexOp [_ tx-op-idx]
            (.indexOp ^OpIndexer (get tables (.getLeg docs-rdr tx-op-idx))
                      tx-op-idx)))))))

(defn- wrap-sql-args [f ^long param-count]
  (fn [^RelationReader args]
    (if (not args)
      (if (zero? param-count)
        (f nil)
        (throw (err/runtime-err :xtdb.indexer/missing-sql-args
                                {::err/message "Arguments list was expected but not provided"
                                 :param-count param-count})))

      (let [arg-count (count (seq args))]
        (if (not= arg-count param-count)
          (throw (err/runtime-err :xtdb.indexer/incorrect-sql-arg-count
                                  {::err/message (format "Parameter error: %d provided, %d expected" arg-count param-count)
                                   :param-count param-count
                                   :arg-count arg-count}))
          (f (vr/rel-reader (->> args
                                 (map-indexed (fn [idx ^IVectorReader col]
                                                (.withName col (str "?_" idx))))))))))))

(def ^:private ^:const ^String user-table "pg_catalog/pg_user")

(defn- update-pg-user! [^LiveIndex$Tx live-idx-tx, ^TransactionKey tx-key, user, password]
  (let [system-time-µs (time/instant->micros (.getSystemTime tx-key))

        live-table (.liveTable live-idx-tx user-table)
        doc-writer (.getDocWriter live-table)]

    (.logPut live-table (util/->iid user) system-time-µs Long/MAX_VALUE
             (fn write-doc! []
               (.startStruct doc-writer)
               (doto (.structKeyWriter doc-writer "_id" (FieldType/notNullable #xt.arrow/type :utf8))
                 (.writeObject user))

               (doto (.structKeyWriter doc-writer "username" (FieldType/notNullable #xt.arrow/type :utf8))
                 (.writeObject user))

               (doto (.structKeyWriter doc-writer "usesuper" (FieldType/notNullable #xt.arrow/type :bool))
                 (.writeObject false))

               (doto (.structKeyWriter doc-writer "passwd" (FieldType/nullable #xt.arrow/type :utf8))
                 (.writeObject (authn/encrypt-pw password)))

               (.endStruct doc-writer)))))

(defn- ->sql-indexer ^xtdb.indexer.OpIndexer [^BufferAllocator allocator, ^LiveIndex$Tx live-idx-tx
                                              ^IVectorReader tx-ops-rdr, ^IQuerySource q-src, wm-src,
                                              {:keys [tx-key] :as tx-opts}]
  (let [sql-leg (.legReader tx-ops-rdr "sql")
        query-rdr (.structKeyReader sql-leg "query")
        args-rdr (.structKeyReader sql-leg "args")
        upsert-idxer (->upsert-rel-indexer live-idx-tx tx-opts)
        patch-idxer (reify RelationIndexer
                      (indexOp [_ rel {:keys [table]}]
                        (patch-rel! table (.liveTable live-idx-tx (str table)) rel tx-opts)))
        delete-idxer (->delete-rel-indexer live-idx-tx tx-opts)
        erase-idxer (->erase-rel-indexer live-idx-tx tx-opts)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (util/with-open [^ArrowReader args-arrow-rdr (open-args-rdr allocator args-rdr tx-op-idx)]
          (let [query-str (.getObject query-rdr tx-op-idx)
                compiled-query (sql/compile-query query-str {:table-info (scan/tables-with-cols wm-src)
                                                             :arg-fields (some-> args-arrow-rdr
                                                                                 .getVectorSchemaRoot
                                                                                 .getSchema
                                                                                 (.getFields))})
                param-count (:param-count (meta compiled-query))]

            (zmatch (r/vector-zip compiled-query)
              [:insert query-opts inner-query]
              (foreach-arg-row args-arrow-rdr
                               (-> (query-indexer q-src wm-src upsert-idxer inner-query tx-opts query-opts)
                                   (wrap-sql-args param-count)))

              [:patch query-opts inner-query]
              (foreach-arg-row args-arrow-rdr
                               (-> (query-indexer q-src wm-src patch-idxer inner-query tx-opts query-opts)
                                   (wrap-sql-args param-count)))

              [:update query-opts inner-query]
              (foreach-arg-row args-arrow-rdr
                               (-> (query-indexer q-src wm-src upsert-idxer inner-query tx-opts query-opts)
                                   (wrap-sql-args param-count)))

              [:delete query-opts inner-query]
              (foreach-arg-row args-arrow-rdr
                               (-> (query-indexer q-src wm-src delete-idxer inner-query tx-opts query-opts)
                                   (wrap-sql-args param-count)))

              [:erase query-opts inner-query]
              (foreach-arg-row args-arrow-rdr
                               (-> (query-indexer q-src wm-src erase-idxer inner-query tx-opts query-opts)
                                   (wrap-sql-args param-count)))

              [:assert _query-opts inner-query]
              (foreach-arg-row args-arrow-rdr
                               (-> (->assert-idxer q-src wm-src inner-query tx-opts)
                                   (wrap-sql-args param-count)))

              [:create-user user password]
              (update-pg-user! live-idx-tx tx-key user password)

              [:alter-user user password]
              (update-pg-user! live-idx-tx tx-key user password)

              (throw (err/illegal-arg ::invalid-sql-tx-op {::err/message "Invalid SQL query sent as transaction operation"
                                                           :query query-str})))))

        nil))))

(defn- wrap-xtql-args [f]
  (fn [^RelationReader args]
    (f (when args
         (vr/rel-reader (for [^IVectorReader col args]
                          (.withName col (str "?" (.getName col)))))))))

(defn- ->xtql-indexer ^xtdb.indexer.OpIndexer [^BufferAllocator allocator, ^LiveIndex$Tx live-idx-tx
                                               ^IVectorReader tx-ops-rdr, ^IQuerySource q-src, wm-src,
                                               tx-opts]
  (let [xtql-leg (.legReader tx-ops-rdr "xtql")
        op-rdr (.structKeyReader xtql-leg "op")
        args-rdr (.structKeyReader xtql-leg "args")
        upsert-idxer (->upsert-rel-indexer live-idx-tx tx-opts)
        delete-idxer (->delete-rel-indexer live-idx-tx tx-opts)
        erase-idxer (->erase-rel-indexer live-idx-tx tx-opts)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [xtql-op (.form ^ClojureForm (.getObject op-rdr tx-op-idx))]
          (util/with-open [args-arrow-rdr (open-args-rdr allocator args-rdr tx-op-idx)]
            (zmatch (xtql/compile-dml xtql-op (assoc tx-opts :table-info (scan/tables-with-cols wm-src)))
              [:insert query-opts inner-query]
              (foreach-arg-row args-arrow-rdr
                               (-> (query-indexer q-src wm-src upsert-idxer inner-query tx-opts query-opts)
                                   (wrap-xtql-args)))

              [:update query-opts inner-query]
              (foreach-arg-row args-arrow-rdr
                               (-> (query-indexer q-src wm-src upsert-idxer inner-query tx-opts query-opts)
                                   (wrap-xtql-args)))

              [:delete query-opts inner-query]
              (foreach-arg-row args-arrow-rdr
                               (-> (query-indexer q-src wm-src delete-idxer inner-query tx-opts query-opts)
                                   (wrap-xtql-args)))

              [:erase query-opts inner-query]
              (foreach-arg-row args-arrow-rdr
                               (-> (query-indexer q-src wm-src erase-idxer inner-query tx-opts query-opts)
                                   (wrap-xtql-args)))

              [:assert _query-opts inner-query]
              (foreach-arg-row args-arrow-rdr
                               (-> (->assert-idxer q-src wm-src inner-query tx-opts)
                                   (wrap-xtql-args)))

              (throw (UnsupportedOperationException. "xtql query")))))

        nil))))

(def ^:private ^:const ^String txs-table "xt/txs")

(defn- add-tx-row! [^LiveIndex$Tx live-idx-tx, ^TransactionKey tx-key, ^Throwable t]
  (let [tx-id (.getTxId tx-key)
        system-time-µs (time/instant->micros (.getSystemTime tx-key))

        live-table (.liveTable live-idx-tx txs-table)
        doc-writer (.getDocWriter live-table)]

    (.logPut live-table (util/->iid tx-id) system-time-µs Long/MAX_VALUE
             (fn write-doc! []
               (.startStruct doc-writer)
               (doto (.structKeyWriter doc-writer "_id" (FieldType/notNullable #xt.arrow/type :i64))
                 (.writeLong tx-id))

               (doto (.structKeyWriter doc-writer "system_time" (FieldType/notNullable (types/->arrow-type types/temporal-col-type)))
                 (.writeLong system-time-µs))

               (doto (.structKeyWriter doc-writer "committed" (FieldType/notNullable #xt.arrow/type :bool))
                 (.writeBoolean (nil? t)))

               (let [e-wtr (.structKeyWriter doc-writer "error" (FieldType/nullable #xt.arrow/type :transit))]
                 (if (or (nil? t) (= t abort-exn))
                   (.writeNull e-wtr)
                   (try
                     (.writeObject e-wtr t)
                     (catch Exception e
                       (log/warnf (doto t (.addSuppressed e)) "Error serializing error, tx %d" tx-id)
                       (.writeObject e-wtr (xt/->ClojureForm "error serializing error - see server logs"))))))

               (.endStruct doc-writer)))))

(defrecord Indexer [^BufferAllocator allocator
                    node-id
                    ^BufferPool buffer-pool
                    ^IMetadataManager metadata-mgr
                    ^IQuerySource q-src
                    ^LiveIndex live-idx
                    ^Timer tx-timer
                    ^Counter tx-error-counter]
  IIndexer
  (indexTx [this tx-id msg-ts tx-root]
    (let [lc-tx (.getLatestCompletedTx live-idx)
          default-system-time (or (when-let [lc-sys-time (some-> lc-tx (.getSystemTime))]
                                    (when-not (neg? (compare lc-sys-time msg-ts))
                                      (.plusNanos lc-sys-time 1000)))
                                  msg-ts)

          system-time (let [sys-time-vec (vr/vec->reader (.getVector tx-root "system-time"))]
                        (some-> (.getObject sys-time-vec 0) time/->instant))]

      (if (and system-time lc-tx
               (neg? (compare system-time (.getSystemTime lc-tx))))
        (let [tx-key (serde/->TxKey tx-id default-system-time)
              err (err/illegal-arg :invalid-system-time
                                   {::err/message "specified system-time older than current tx"
                                    :tx-key (serde/->TxKey tx-id system-time)
                                    :latest-completed-tx (.getLatestCompletedTx live-idx)})]
          (log/warnf "specified system-time '%s' older than current tx '%s'"
                     (pr-str system-time)
                     (pr-str (.getLatestCompletedTx live-idx)))

          (util/with-open [live-idx-tx (.startTx live-idx tx-key)]
            (when tx-error-counter
              (.increment tx-error-counter))
            (add-tx-row! live-idx-tx tx-key err)
            (.commit live-idx-tx))

          (serde/->tx-aborted tx-id default-system-time err))

        (let [system-time (or system-time default-system-time)
              tx-key (serde/->TxKey tx-id system-time)]
          (util/with-open [live-idx-tx (.startTx live-idx tx-key)]
            (let [^DenseUnionVector tx-ops-vec (-> ^ListVector (.getVector tx-root "tx-ops")
                                                   (.getDataVector))

                  wm-src (reify Watermark$Source
                           (openWatermark [_]
                             (util/with-close-on-catch [live-index-wm (.openWatermark live-idx-tx)]
                               (Watermark. nil live-index-wm
                                           (li/->schema live-index-wm metadata-mgr)))))

                  tx-opts {:snapshot-time system-time
                           :current-time system-time
                           :default-tz (ZoneId/of (str (-> (.getVector tx-root "default-tz")
                                                           (.getObject 0))))
                           :tx-key tx-key
                           :indexer this}]

              (letfn [(index-tx-ops [^DenseUnionVector tx-ops-vec]
                        (let [tx-ops-rdr (vr/vec->reader tx-ops-vec)
                              !put-docs-idxer (delay (->put-docs-indexer live-idx-tx tx-ops-rdr system-time tx-opts))
                              !patch-docs-idxer (delay (->patch-docs-indexer live-idx-tx tx-ops-rdr q-src wm-src tx-opts))
                              !delete-docs-idxer (delay (->delete-docs-indexer live-idx-tx tx-ops-rdr system-time tx-opts))
                              !erase-docs-idxer (delay (->erase-docs-indexer live-idx-tx tx-ops-rdr tx-opts))
                              !call-idxer (delay (->call-indexer allocator q-src wm-src tx-ops-rdr tx-opts))
                              !xtql-idxer (delay (->xtql-indexer allocator live-idx-tx tx-ops-rdr q-src wm-src tx-opts))
                              !sql-idxer (delay (->sql-indexer allocator live-idx-tx tx-ops-rdr q-src wm-src tx-opts))]
                          (dotimes [tx-op-idx (.valueCount tx-ops-rdr)]
                            (when-let [more-tx-ops
                                       (.recordCallable tx-timer
                                                        #(case (.getLeg tx-ops-rdr tx-op-idx)
                                                           "xtql" (.indexOp ^OpIndexer @!xtql-idxer tx-op-idx)
                                                           "sql" (.indexOp ^OpIndexer @!sql-idxer tx-op-idx)
                                                           "put-docs" (.indexOp ^OpIndexer @!put-docs-idxer tx-op-idx)
                                                           "patch-docs" (.indexOp ^OpIndexer @!patch-docs-idxer tx-op-idx)
                                                           "delete-docs" (.indexOp ^OpIndexer @!delete-docs-idxer tx-op-idx)
                                                           "erase-docs" (.indexOp ^OpIndexer @!erase-docs-idxer tx-op-idx)
                                                           "call" (.indexOp ^OpIndexer @!call-idxer tx-op-idx)
                                                           "abort" (throw abort-exn)))]
                              (try
                                (index-tx-ops more-tx-ops)
                                (finally
                                  (util/try-close more-tx-ops)))))))]
                (let [e (try
                          (index-tx-ops tx-ops-vec)
                          (catch xtdb.RuntimeException e e)
                          (catch xtdb.IllegalArgumentException e e))]
                  (if e
                    (do
                      (when-not (= e abort-exn)
                        (log/debug e "aborted tx")
                        (.abort live-idx-tx))

                      (util/with-open [live-idx-tx (.startTx live-idx tx-key)]
                        (when tx-error-counter
                          (.increment tx-error-counter))
                        (add-tx-row! live-idx-tx tx-key e)
                        (.commit live-idx-tx))

                      (serde/->tx-aborted tx-id system-time
                                          (when-not (= e abort-exn)
                                            e)))

                    (do
                      (add-tx-row! live-idx-tx tx-key nil)
                      (.commit live-idx-tx)
                      (serde/->tx-committed tx-id system-time)))))))))))

  Closeable
  (close [_]
    (util/close allocator)))

(defmethod ig/prep-key :xtdb/indexer [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :config (ig/ref :xtdb/config)
          :buffer-pool (ig/ref :xtdb/buffer-pool)
          :metadata-mgr (ig/ref ::meta/metadata-manager)
          :live-index (ig/ref :xtdb.indexer/live-index)
          :q-src (ig/ref ::q/query-source)
          :metrics-registry (ig/ref :xtdb.metrics/registry)}
         opts))

(defmethod ig/init-key :xtdb/indexer [_ {:keys [allocator config buffer-pool, metadata-mgr, q-src, live-index metrics-registry]}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "indexer")]
    (metrics/add-allocator-gauge metrics-registry "indexer.allocator.allocated_memory" allocator)
    (->Indexer allocator (:node-id config) buffer-pool metadata-mgr q-src live-index

               (metrics/add-timer metrics-registry "tx.op.timer"
                                  {:description "indicates the timing and number of transactions"})
               (metrics/add-counter metrics-registry "tx.error"))))

(defmethod ig/halt-key! :xtdb/indexer [_ indexer]
  (util/close indexer))
