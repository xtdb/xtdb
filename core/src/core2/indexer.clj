(ns core2.indexer
  (:require [clojure.tools.logging :as log]
            [core2.api :as c2]
            [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            [core2.buffer-pool :as bp]
            [core2.datalog :as d]
            [core2.error :as err]
            core2.indexer.internal-id-manager
            [core2.indexer.log-indexer :as log-idx]
            [core2.metadata :as meta]
            core2.object-store
            [core2.operator :as op]
            core2.operator.scan
            [core2.rewrite :refer [zmatch]]
            [core2.sql :as sql]
            [core2.temporal :as temporal]
            [core2.tx-producer :as txp]
            [core2.types :as t]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [core2.watermark :as wm]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [sci.core :as sci])
  (:import clojure.lang.MapEntry
           (core2.api ClojureForm TransactionInstant)
           core2.buffer_pool.IBufferPool
           core2.ICursor
           core2.indexer.internal_id_manager.IInternalIdManager
           (core2.indexer.log_indexer ILogIndexer ILogOpIndexer)
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           core2.operator.scan.ScanSource
           (core2.temporal ITemporalManager ITemporalTxIndexer)
           (core2.vector IIndirectRelation IIndirectVector IVectorWriter)
           (core2.watermark ISharedWatermark IWatermarkManager)
           (java.io ByteArrayInputStream Closeable)
           java.lang.AutoCloseable
           (java.time Instant ZoneId)
           (java.util Collections HashMap Map TreeMap)
           (java.util.concurrent CompletableFuture ConcurrentHashMap ConcurrentSkipListMap)
           (java.util.concurrent.locks StampedLock)
           (java.util.function Consumer Function)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector TimeStampVector ValueVector VarBinaryVector VarCharVector VectorLoader VectorSchemaRoot VectorUnloader)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector)
           (org.apache.arrow.vector.ipc ArrowStreamReader)
           org.roaringbitmap.longlong.Roaring64Bitmap
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defmethod ig/prep-key :core2/row-counts [_ opts]
  (merge {:max-rows-per-block 1000
          :max-rows-per-chunk 100000}
         opts))

(defmethod ig/init-key :core2/row-counts [_ {:keys [max-rows-per-chunk] :as opts}]
  (let [bloom-false-positive-probability (bloom/bloom-false-positive-probability? max-rows-per-chunk)]
    (when (> bloom-false-positive-probability 0.05)
      (log/warn "Bloom should be sized for large chunks:" max-rows-per-chunk
                "false positive probability:" bloom-false-positive-probability
                "bits:" bloom/bloom-bits
                "can be set via system property core2.bloom.bits")))

  opts)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveColumn
  (^void writeRowId [^long rowId])
  (^boolean containsRowId [^long rowId])
  (^core2.vector.IVectorWriter contentWriter [])
  (^org.apache.arrow.vector.VectorSchemaRoot liveRoot [])
  (^org.apache.arrow.vector.VectorSchemaRoot txLiveRoot [])
  (^void commit [])
  (^void abort [])
  (^void close []))

(deftype LiveColumn [^BigIntVector row-id-vec, ^ValueVector content-vec, ^Roaring64Bitmap row-id-bitmap
                     ^BigIntVector transient-row-id-vec, ^ValueVector transient-content-vec, ^Roaring64Bitmap transient-row-id-bitmap]
  ILiveColumn
  (writeRowId [_ row-id]
    (.addLong transient-row-id-bitmap row-id)

    (let [dest-idx (.getValueCount transient-row-id-vec)]
      (.setValueCount transient-row-id-vec (inc dest-idx))
      (.set transient-row-id-vec dest-idx row-id)))

  (containsRowId [_ row-id]
    (or (.contains row-id-bitmap row-id)
        (.contains transient-row-id-bitmap row-id)))

  (contentWriter [_] (vw/vec->writer transient-content-vec))

  (liveRoot [_]
    (let [^Iterable vs [row-id-vec content-vec]]
      (VectorSchemaRoot. vs)))

  (txLiveRoot [_]
    (let [^Iterable vs [transient-row-id-vec transient-content-vec]]
      (VectorSchemaRoot. vs)))

  (commit [_]
    (doto (vw/vec->writer content-vec)
      (vw/append-vec (iv/->direct-vec transient-content-vec)))

    (doto (vw/vec->writer row-id-vec)
      (vw/append-vec (iv/->direct-vec transient-row-id-vec)))

    (doto row-id-bitmap (.or transient-row-id-bitmap))

    (.clear transient-row-id-vec)
    (.clear transient-content-vec)
    (.clear transient-row-id-bitmap))

  (abort [_]
    (.clear transient-row-id-vec)
    (.clear transient-content-vec)
    (.clear transient-row-id-bitmap))

  Closeable
  (close [_]
    (util/try-close transient-row-id-vec)
    (util/try-close transient-content-vec)

    (util/try-close content-vec)
    (util/try-close row-id-vec)))

(defn ->live-column [^BufferAllocator allocator, ^String field-name]
  (LiveColumn. (-> t/row-id-field (.createVector allocator))
               (-> (t/->field field-name t/dense-union-type false) (.createVector allocator))
               (Roaring64Bitmap.)

               (-> t/row-id-field (.createVector allocator))
               (-> (t/->field field-name t/dense-union-type false) (.createVector allocator))
               (Roaring64Bitmap.)))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IDocumentIndexer
  (^core2.indexer.ILiveColumn getLiveColumn [^String table, ^String fieldName])
  (^java.util.Map liveColumnsWith [^String table, ^long rowId])
  (^long nextRowId [])
  (^long commit [] "returns: count of rows in this tx")
  (^void abort []))

(deftype DocumentIndexer [^BufferAllocator allocator, ^Map live-columns
                          ^long start-row-id, ^:unsynchronized-mutable ^long tx-row-count]
  IDocumentIndexer
  (getLiveColumn [_ table field-name]
    (-> ^Map (.computeIfAbsent live-columns table
                               (util/->jfn (fn [_table] (HashMap.))))
        (.computeIfAbsent field-name
                          (util/->jfn (fn [field-name]
                                        (->live-column allocator field-name))))))

  (liveColumnsWith [_ table row-id]
    (->> (.get live-columns table)
         (into {} (filter (comp (fn [^LiveColumn live-col]
                                  (.containsRowId live-col row-id))
                                val)))))

  (nextRowId [this]
    (let [tx-row-count tx-row-count]
      (set! (.tx-row-count this) (inc tx-row-count))
      (+ start-row-id tx-row-count)))

  (commit [_]
    (doseq [^ILiveColumn live-col (->> (vals live-columns) (mapcat vals))]
      (.commit live-col))

    tx-row-count)

  (abort [_]
    (doseq [^ILiveColumn live-col (->> (vals live-columns) (mapcat vals))]
      (.abort live-col))))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface TransactionIndexer
  (^core2.watermark.IWatermark openWatermark [^core2.api.TransactionInstant tx])
  (^core2.api.TransactionInstant indexTx [^core2.api.TransactionInstant tx
                                          ^org.apache.arrow.vector.VectorSchemaRoot txRoot])
  (^core2.api.TransactionInstant latestCompletedTx []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IndexerPrivate
  (^java.nio.ByteBuffer writeColumn [^core2.indexer.LiveColumn live-column])
  (^void closeCols []))

(defprotocol Finish
  (^void finish-chunk! [_]))

(defn- snapshot-live-cols [^Map live-columns]
  (Collections/unmodifiableSortedMap
   (reduce-kv (fn [^Map acc table-name ^Map table-cols]
                (doto acc (.put table-name
                                (reduce-kv (fn [^Map acc col-name ^LiveColumn col]
                                             (doto acc (.put col-name (util/slice-root (.liveRoot col)))))
                                           (TreeMap.)
                                           table-cols))))
              (TreeMap.)
              live-columns)))

(def ^:private abort-exn (err/runtime-err :abort-exn))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface OpIndexer
  (^org.apache.arrow.vector.complex.DenseUnionVector indexOp [^long tx-op-idx]
   "returns a tx-ops-vec of more operations (mostly for `:call`)"))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ContentRowCopier
  (^void copyContentRow [^long rowId, ^int srcIdx]))

(defn- content-row-copier ^core2.indexer.ContentRowCopier [^IDocumentIndexer doc-idxer, ^String table, ^IIndirectVector col-rdr]
  (let [col-name (.getName col-rdr)
        ^LiveColumn live-col (.getLiveColumn doc-idxer table col-name)
        ^IVectorWriter vec-writer (.contentWriter live-col)
        row-copier (.rowCopier col-rdr vec-writer)]
    (reify ContentRowCopier
      (copyContentRow [_ row-id src-idx]
        (when (.isPresent col-rdr src-idx)
          (.writeRowId live-col row-id)
          (.startValue vec-writer)
          (.copyRow row-copier src-idx)
          (.endValue vec-writer))))))

(defn- put-row-copier ^core2.indexer.ContentRowCopier [^IDocumentIndexer indexer, ^VarCharVector table-vec, ^DenseUnionVector id-duv, ^DenseUnionVector doc-duv]
  (let [id-rdr (iv/->direct-vec id-duv)
        doc-rdr (.structReader (iv/->direct-vec doc-duv))
        table->content-copiers (HashMap.)]
    (reify ContentRowCopier
      (copyContentRow [_ row-id src-idx]
        (let [table-name (t/get-object table-vec src-idx)
              ^Map field->content-copiers (.computeIfAbsent table->content-copiers table-name
                                                            (util/->jfn (fn [_table-name]
                                                                          (doto (HashMap.)
                                                                            (.put "id" (content-row-copier indexer table-name id-rdr))))))]

          (-> ^ContentRowCopier (.get field->content-copiers "id")
              (.copyContentRow row-id src-idx))

          (doseq [^String col-name (.structKeys doc-rdr)
                  :let [col-rdr (.readerForKey doc-rdr col-name)]
                  :when (.isPresent col-rdr src-idx)]
            (-> ^ContentRowCopier
                (.computeIfAbsent field->content-copiers col-name
                                                         (util/->jfn (fn [_col-name]
                                                                       (content-row-copier indexer table-name col-rdr))))
                (.copyContentRow row-id src-idx))))))))

(defn- ->put-indexer ^core2.indexer.OpIndexer [^IInternalIdManager iid-mgr ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^IDocumentIndexer doc-idxer
                                               ^DenseUnionVector tx-ops-vec, ^Instant current-time]
  (let [put-vec (.getStruct tx-ops-vec 1)
        ^VarCharVector table-vec (.getChild put-vec "table" VarCharVector)
        ^DenseUnionVector id-duv (.getChild put-vec "id" DenseUnionVector)
        ^DenseUnionVector doc-duv (.getChild put-vec "document" DenseUnionVector)
        ^TimeStampVector app-time-start-vec (.getChild put-vec "application_time_start")
        ^TimeStampVector app-time-end-vec (.getChild put-vec "application_time_end")
        row-copier (put-row-copier doc-idxer table-vec id-duv doc-duv)
        current-time-µs (util/instant->micros current-time)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [row-id (.nextRowId doc-idxer)
              put-offset (.getOffset tx-ops-vec tx-op-idx)]

          (.copyContentRow row-copier row-id put-offset)

          (let [table (t/get-object table-vec put-offset)
                eid (t/get-object id-duv put-offset)
                new-entity? (not (.isKnownId iid-mgr table eid))
                iid (.getOrCreateInternalId iid-mgr table eid row-id)
                start-app-time (if (.isNull app-time-start-vec put-offset)
                                 current-time-µs
                                 (.get app-time-start-vec put-offset))
                end-app-time (if (.isNull app-time-end-vec put-offset)
                               util/end-of-time-μs
                               (.get app-time-end-vec put-offset))]
            (.logPut log-op-idxer iid row-id start-app-time end-app-time)
            (.indexPut temporal-idxer iid row-id start-app-time end-app-time new-entity?)))

        nil))))

(defn- ->delete-indexer
  ^core2.indexer.OpIndexer
  [^IInternalIdManager iid-mgr, ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^IDocumentIndexer doc-idxer
   ^DenseUnionVector tx-ops-vec, ^Instant current-time]
  (let [delete-vec (.getStruct tx-ops-vec 2)
        ^VarCharVector table-vec (.getChild delete-vec "table" VarCharVector)
        ^DenseUnionVector id-vec (.getChild delete-vec "id" DenseUnionVector)
        ^TimeStampVector app-time-start-vec (.getChild delete-vec "application_time_start")
        ^TimeStampVector app-time-end-vec (.getChild delete-vec "application_time_end")
        current-time-µs (util/instant->micros current-time)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [row-id (.nextRowId doc-idxer)
              delete-offset (.getOffset tx-ops-vec tx-op-idx)
              table (t/get-object table-vec delete-offset)
              eid (t/get-object id-vec delete-offset)
              new-entity? (not (.isKnownId iid-mgr table eid))
              iid (.getOrCreateInternalId iid-mgr table eid row-id)
              start-app-time (if (.isNull app-time-start-vec delete-offset)
                               current-time-µs
                               (.get app-time-start-vec delete-offset))
              end-app-time (if (.isNull app-time-end-vec delete-offset)
                             util/end-of-time-μs
                             (.get app-time-end-vec delete-offset))]
          (.logDelete log-op-idxer iid start-app-time end-app-time)
          (.indexDelete temporal-idxer iid row-id start-app-time end-app-time new-entity?))

        nil))))

(defn- ->evict-indexer
  ^core2.indexer.OpIndexer
  [^IInternalIdManager iid-mgr ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^IDocumentIndexer doc-idxer
   ^DenseUnionVector tx-ops-vec]

  (let [evict-vec (.getStruct tx-ops-vec 3)
        ^VarCharVector table-vec (.getChild evict-vec "_table" VarCharVector)
        ^DenseUnionVector id-vec (.getChild evict-vec "id" DenseUnionVector)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [row-id (.nextRowId doc-idxer)
              evict-offset (.getOffset tx-ops-vec tx-op-idx)
              table (or (t/get-object table-vec evict-offset) "xt_docs")
              eid (t/get-object id-vec evict-offset)
              iid (.getOrCreateInternalId iid-mgr table eid row-id)]
          (.logEvict log-op-idxer iid)
          (.indexEvict temporal-idxer iid))

        nil))))

(defn- find-fn [allocator sci-ctx scan-src ^ConcurrentHashMap prepare-ra-cache tx-opts fn-id]
  ;; HACK: assume xt_docs here...
  ;; TODO confirm fn-body doc key

  (let [lp '[:scan xt_docs [{id (= id ?id)} fn]]
        ;; prepare-ra-cache a bit overkill here for now
        ^core2.operator.PreparedQuery pq (.computeIfAbsent prepare-ra-cache
                                                           lp
                                                           (reify Function
                                                             (apply [_ _]
                                                               (op/prepare-ra lp))))]
    (with-open [bq (.bind pq (into (select-keys tx-opts [:current-time :default-tz])
                                   {:srcs {'$ scan-src},
                                    :params (iv/->indirect-rel [(-> (vw/open-vec allocator '?id [fn-id])
                                                                    (iv/->direct-vec))]
                                                               1)
                                    :app-time-as-of-now? true}))
                res (.openCursor bq)]

      (let [!fn-doc (object-array 1)]
        (.tryAdvance res
                     (reify Consumer
                       (accept [_ in-rel]
                         (when (pos? (.rowCount ^IIndirectRelation in-rel))
                           (aset !fn-doc 0 (first (iv/rel->rows in-rel)))))))

        (let [fn-doc (or (aget !fn-doc 0)
                         (throw (err/runtime-err :core2.call/no-such-tx-fn {:fn-id fn-id})))
              fn-body (:fn fn-doc)]

          (when-not (instance? ClojureForm fn-body)
            (throw (err/illegal-arg :core2.call/invalid-tx-fn {:fn-doc fn-doc})))

          (let [fn-form (:form fn-body)]
            (try
              (sci/eval-form sci-ctx fn-form)

              (catch Throwable t
                (throw (err/runtime-err :core2.call/error-compiling-tx-fn {:fn-form fn-form} t))))))))))

(defn- tx-fn-q [allocator prepare-ra-cache scan-src tx-opts q & args]
  ;; bear in mind Datalog doesn't yet look at `app-time-as-of-now?`, essentially just assumes its true.
  (let [q (into tx-opts q)]
    (with-open [res (d/open-datalog-query allocator prepare-ra-cache q scan-src args)]
      (vec (iterator-seq res)))))

(defn- tx-fn-sql
  ([allocator ^ConcurrentHashMap prepare-ra-cache scan-src tx-opts query]
   (tx-fn-sql allocator prepare-ra-cache scan-src tx-opts query {}))

  ([allocator ^ConcurrentHashMap prepare-ra-cache scan-src tx-opts query query-opts]
   (try
     (let [query-opts (into tx-opts query-opts)
           pq (sql/prepare-sql query prepare-ra-cache query-opts)]
       (with-open [res (sql/open-sql-query allocator pq scan-src query-opts)]
         (vec (iterator-seq res))))
     (catch Throwable e
       (log/error e)
       (throw e)))))

(def ^:private !last-tx-fn-error (atom nil))

(defn reset-tx-fn-error! []
  (first (reset-vals! !last-tx-fn-error nil)))


(defn- ->call-indexer ^core2.indexer.OpIndexer [allocator, ^DenseUnionVector tx-ops-vec, scan-src,
                                                prepare-ra-cache, {:keys [tx-key] :as tx-opts}]
  (let [call-vec (.getStruct tx-ops-vec 4)
        ^DenseUnionVector fn-id-vec (.getChild call-vec "fn-id" DenseUnionVector)
        ^ListVector args-vec (.getChild call-vec "args" ListVector)

        ;; TODO confirm/expand API that we expose to tx-fns
        sci-ctx (sci/init {:bindings {'q (partial tx-fn-q allocator prepare-ra-cache scan-src tx-opts)
                                      'sql-q (partial tx-fn-sql allocator prepare-ra-cache scan-src tx-opts)
                                      'sleep (fn [n] (Thread/sleep n))
                                      '*current-tx* tx-key}})]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (try
          (let [call-offset (.getOffset tx-ops-vec tx-op-idx)
                fn-id (t/get-object fn-id-vec call-offset)
                tx-fn (find-fn allocator (sci/fork sci-ctx) scan-src prepare-ra-cache tx-opts fn-id)

                args (t/get-object args-vec call-offset)

                res (try
                      (sci/binding [sci/out *out*
                                    sci/in *in*]
                        (apply tx-fn args))

                      (catch InterruptedException ie (throw ie))
                      (catch Throwable t
                        (log/warn t "unhandled error evaluating tx fn")
                        (throw (err/runtime-err :core2.call/error-evaluating-tx-fn
                                                {:fn-id fn-id, :args args}
                                                t))))]
            (when (false? res)
              (throw abort-exn))

            ;; if the user returns `nil` or `true`, we just continue with the rest of the transaction
            (when-not (or (nil? res) (true? res))
              (let [tx-ops-vec (txp/open-tx-ops-vec allocator)]
                (try
                  (txp/write-tx-ops! allocator (.asDenseUnion (vw/vec->writer tx-ops-vec)) res)
                  tx-ops-vec

                  (catch Throwable t
                    (.close tx-ops-vec)
                    (throw t))))))

          (catch Throwable t
            (reset! !last-tx-fn-error t)
            (throw t)))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface SqlOpIndexer
  (^void indexOp [^core2.vector.IIndirectRelation inRelation, queryOpts]))

(defn- ->sql-insert-indexer
  ^core2.indexer.SqlOpIndexer
  [^IInternalIdManager iid-mgr, ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^IDocumentIndexer doc-idxer
   {:keys [^Instant current-time]}]

  (let [current-time-µs (util/instant->micros current-time)]
    (reify SqlOpIndexer
      (indexOp [_ in-rel {:keys [table]}]
        (let [row-count (.rowCount in-rel)
              content-row-copiers (vec
                                   (for [^IIndirectVector in-col in-rel
                                         :when (not (temporal/temporal-column? (.getName in-col)))]
                                     (content-row-copier doc-idxer table in-col)))
              id-col (.vectorForName in-rel "id")
              app-time-start-rdr (some-> (.vectorForName in-rel "application_time_start") (.monoReader t/temporal-col-type))
              app-time-end-rdr (some-> (.vectorForName in-rel "application_time_end") (.monoReader t/temporal-col-type))]
          (dotimes [idx row-count]
            (let [row-id (.nextRowId doc-idxer)]
              (doseq [^ContentRowCopier content-row-copier content-row-copiers]
                (.copyContentRow content-row-copier row-id idx))

              (let [eid (t/get-object (.getVector id-col) (.getIndex id-col idx))
                    new-entity? (not (.isKnownId iid-mgr table eid))
                    iid (.getOrCreateInternalId iid-mgr table eid row-id)
                    start-app-time (if app-time-start-rdr
                                     (.readLong app-time-start-rdr idx)
                                     current-time-µs)
                    end-app-time (if app-time-end-rdr
                                   (.readLong app-time-end-rdr idx)
                                   util/end-of-time-μs)]
                (when (> start-app-time end-app-time)
                  (throw (err/runtime-err :core2.indexer/invalid-app-times
                                          {:app-time-start (util/micros->instant start-app-time)
                                           :app-time-end (util/micros->instant end-app-time)})))

                (.logPut log-op-idxer iid row-id start-app-time end-app-time)
                (.indexPut temporal-idxer iid row-id start-app-time end-app-time new-entity?)))))))))

(defn- ->sql-update-indexer
  ^core2.indexer.SqlOpIndexer
  [^IMetadataManager metadata-mgr, ^IBufferPool buffer-pool
   ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^IDocumentIndexer doc-idxer]

  (reify SqlOpIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [row-count (.rowCount in-rel)
            content-row-copiers (->> (for [^IIndirectVector in-col in-rel
                                       :let [col-name (.getName in-col)]
                                       :when (not (temporal/temporal-column? col-name))]
                                   [col-name (content-row-copier doc-idxer table in-col)])
                                 (into {}))
            iid-rdr (.monoReader (.vectorForName in-rel "_iid") :i64)
            row-id-rdr (.monoReader (.vectorForName in-rel "_row-id") :i64)
            app-time-start-rdr (.monoReader (.vectorForName in-rel "application_time_start") t/temporal-col-type)
            app-time-end-rdr (.monoReader (.vectorForName in-rel "application_time_end") t/temporal-col-type)]
        (dotimes [idx row-count]
          (let [old-row-id (.readLong row-id-rdr idx)
                new-row-id (.nextRowId doc-idxer)
                iid (.readLong iid-rdr idx)
                start-app-time (.readLong app-time-start-rdr idx)
                end-app-time (.readLong app-time-end-rdr idx)]
            (doseq [^ContentRowCopier content-row-copier (vals content-row-copiers)]
              (.copyContentRow content-row-copier new-row-id idx))

            (letfn [(copy-row [^BigIntVector root-row-id-vec, ^ValueVector content-vec]
                      (let [content-row-copier (content-row-copier doc-idxer table (iv/->direct-vec content-vec))]
                        ;; TODO these are sorted, so we could binary search instead
                        (dotimes [idx (.getValueCount root-row-id-vec)]
                          (when (= old-row-id (.get root-row-id-vec idx))
                            (.copyContentRow content-row-copier new-row-id idx)))))]

              (doseq [[^String col-name, ^LiveColumn live-col] (.liveColumnsWith doc-idxer table old-row-id)
                      :when (not (contains? content-row-copiers col-name))]
                (copy-row (.row-id-vec live-col) (.content-vec live-col))
                (copy-row (.transient-row-id-vec live-col) (.transient-content-vec live-col)))

              (when-let [{:keys [^long chunk-idx cols]} (meta/row-id->cols metadata-mgr (name table) old-row-id)]
                (doseq [{:keys [^String col-name ^long block-idx]} cols
                        :when (not (contains? content-row-copiers col-name))]
                  @(-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx table col-name))
                       (util/then-apply
                         (fn [buf]
                           (with-open [chunks (util/->chunks buf {:block-idxs (doto (RoaringBitmap.)
                                                                                (.add block-idx))
                                                                  :close-buffer? true})]
                             (-> chunks
                                 (.forEachRemaining (reify Consumer
                                                      (accept [_ block-root]
                                                        (let [^VectorSchemaRoot block-root block-root]
                                                          (copy-row (.getVector block-root "_row-id")
                                                                    (.getVector block-root col-name))))))))))))))

            (.logPut log-op-idxer iid new-row-id start-app-time end-app-time)
            (.indexPut temporal-idxer iid new-row-id start-app-time end-app-time false)))))))

(defn- ->sql-delete-indexer ^core2.indexer.SqlOpIndexer [^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^IDocumentIndexer doc-idxer]
  (reify SqlOpIndexer
    (indexOp [_ in-rel _query-opts]
      (let [row-count (.rowCount in-rel)
            iid-rdr (.monoReader (.vectorForName in-rel "_iid") :i64)
            app-time-start-rdr (.monoReader (.vectorForName in-rel "application_time_start") t/temporal-col-type)
            app-time-end-rdr (.monoReader (.vectorForName in-rel "application_time_end") t/temporal-col-type)]
        (dotimes [idx row-count]
          (let [row-id (.nextRowId doc-idxer)
                iid (.readLong iid-rdr idx)
                start-app-time (.readLong app-time-start-rdr idx)
                end-app-time (.readLong app-time-end-rdr idx)]
            (.logDelete log-op-idxer iid start-app-time end-app-time)
            (.indexDelete temporal-idxer iid row-id start-app-time end-app-time false)))))))

(defn- ->sql-erase-indexer ^core2.indexer.SqlOpIndexer [^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer]
  (reify SqlOpIndexer
    (indexOp [_ in-rel _query-opts]
      (let [row-count (.rowCount in-rel)
            iid-rdr (.monoReader (.vectorForName in-rel "_iid") :i64)]
        (dotimes [idx row-count]
          (let [iid (.readLong iid-rdr idx)]
            (.logEvict log-op-idxer iid)
            (.indexEvict temporal-idxer iid)))))))

(defn- ->sql-indexer ^core2.indexer.OpIndexer [^BufferAllocator allocator, ^IMetadataManager metadata-mgr, ^IBufferPool buffer-pool, ^IInternalIdManager iid-mgr
                                               ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^IDocumentIndexer doc-idxer
                                               ^DenseUnionVector tx-ops-vec, ^ScanSource scan-src, ^ConcurrentHashMap prepared-ra-cache
                                               {:keys [app-time-as-of-now?] :as tx-opts}]
  (let [sql-vec (.getStruct tx-ops-vec 0)
        ^VarCharVector query-vec (.getChild sql-vec "query" VarCharVector)
        ^VarBinaryVector params-vec (.getChild sql-vec "params" VarBinaryVector)
        insert-idxer (->sql-insert-indexer iid-mgr log-op-idxer temporal-idxer doc-idxer tx-opts)
        update-idxer (->sql-update-indexer metadata-mgr buffer-pool log-op-idxer temporal-idxer doc-idxer)
        delete-idxer (->sql-delete-indexer log-op-idxer temporal-idxer doc-idxer)
        erase-idxer (->sql-erase-indexer log-op-idxer temporal-idxer)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [sql-offset (.getOffset tx-ops-vec tx-op-idx)]
          (letfn [(index-op [^SqlOpIndexer op-idxer query-opts inner-query]
                    (let [^core2.operator.PreparedQuery pq (.computeIfAbsent prepared-ra-cache
                                                                             inner-query
                                                                             (reify Function
                                                                               (apply [_ _]
                                                                                 (op/prepare-ra inner-query))))]
                      (letfn [(index-op* [^IIndirectRelation params]
                                (with-open [res (-> (.bind pq (into (select-keys tx-opts [:current-time :default-tz])
                                                                    {:srcs {'$ scan-src}, :params params}))
                                                    (.openCursor))]

                                  (.forEachRemaining res
                                                     (reify Consumer
                                                       (accept [_ in-rel]
                                                         (.indexOp op-idxer in-rel query-opts))))))]
                        (if (.isNull params-vec sql-offset)
                          (index-op* nil)

                          (with-open [is (ByteArrayInputStream. (.get params-vec sql-offset))
                                      asr (ArrowStreamReader. is allocator)]
                            (let [root (.getVectorSchemaRoot asr)]
                              (while (.loadNextBatch asr)
                                (let [rel (iv/<-root root)

                                      ^IIndirectRelation
                                      rel (iv/->indirect-rel (->> rel
                                                                  (map-indexed (fn [idx ^IIndirectVector col]
                                                                                 (.withName col (str "?_" idx)))))
                                                             (.rowCount rel))

                                      selection (int-array 1)]
                                  (dotimes [idx (.rowCount rel)]
                                    (aset selection 0 idx)
                                    (index-op* (-> rel (iv/select selection))))))))))))]

            (let [query-str (t/get-object query-vec sql-offset)]

              ;; TODO handle error
              (zmatch (sql/compile-query query-str {:app-time-as-of-now? app-time-as-of-now?})
                [:insert query-opts inner-query]
                (index-op insert-idxer query-opts inner-query)

                [:update query-opts inner-query]
                (index-op update-idxer query-opts inner-query)

                [:delete query-opts inner-query]
                (index-op delete-idxer query-opts inner-query)

                [:erase query-opts inner-query]
                (index-op erase-idxer query-opts inner-query)

                (throw (UnsupportedOperationException. "sql query"))))))

        nil))))

(defn- live-cols->live-roots [live-cols]
  (->> live-cols
       (into {} (map (fn [[table-name ^Map live-cols]]
                       (MapEntry/create table-name
                                        (->> live-cols
                                             (into {} (map (fn [[col-name ^LiveColumn live-col]]
                                                             (MapEntry/create col-name (.liveRoot live-col))))))))))))

(defn- live-cols->tx-live-roots [live-cols]
  (->> live-cols
       (into {} (map (fn [[table-name ^Map live-cols]]
                       (MapEntry/create table-name
                                        (->> live-cols
                                             (into {} (map (fn [[col-name ^LiveColumn live-col]]
                                                             (MapEntry/create col-name (.txLiveRoot live-col))))))))))))

(deftype Indexer [^BufferAllocator allocator
                  ^ObjectStore object-store
                  ^IMetadataManager metadata-mgr
                  ^IBufferPool buffer-pool
                  ^ITemporalManager temporal-mgr
                  ^IInternalIdManager iid-mgr
                  ^IWatermarkManager wm-mgr
                  ^ILogIndexer log-indexer

                  ^long max-rows-per-chunk
                  ^long max-rows-per-block

                  ^Map live-columns
                  ^ConcurrentHashMap prepare-ra-cache
                  ^:volatile-mutable ^long chunk-idx
                  ^:volatile-mutable ^TransactionInstant latest-completed-tx
                  ^:volatile-mutable ^long chunk-row-count

                  ^:volatile-mutable ^ISharedWatermark shared-wm
                  ^StampedLock wm-lock]

  TransactionIndexer
  (indexTx [this {:keys [sys-time] :as tx-key} tx-root]
    (let [^DenseUnionVector tx-ops-vec (-> ^ListVector (.getVector tx-root "tx-ops")
                                           (.getDataVector))

          log-op-idxer (.startTx log-indexer tx-key)
          temporal-idxer (.startTx temporal-mgr tx-key)
          doc-idxer (DocumentIndexer. allocator live-columns (+ chunk-idx chunk-row-count) 0)

          scan-src (reify ScanSource
                     (metadataManager [_] metadata-mgr)
                     (bufferPool [_] buffer-pool)
                     (txBasis [_] tx-key)
                     (openWatermark [_]
                       (wm/->Watermark nil (live-cols->live-roots live-columns) (live-cols->tx-live-roots live-columns)
                                       temporal-idxer chunk-idx max-rows-per-block false)))

          tx-opts {:current-time sys-time
                   :app-time-as-of-now? (== 1 (-> ^BitVector (.getVector tx-root "application-time-as-of-now?")
                                                  (.get 0)))
                   :default-tz (ZoneId/of (str (-> (.getVector tx-root "default-tz")
                                                   (.getObject 0))))
                   :tx-key tx-key}]

      (letfn [(index-tx-ops [^DenseUnionVector tx-ops-vec]
                (let [!put-idxer (delay (->put-indexer iid-mgr log-op-idxer temporal-idxer doc-idxer tx-ops-vec sys-time))
                      !delete-idxer (delay (->delete-indexer iid-mgr log-op-idxer temporal-idxer doc-idxer tx-ops-vec sys-time))
                      !evict-idxer (delay (->evict-indexer iid-mgr log-op-idxer temporal-idxer doc-idxer tx-ops-vec))
                      !call-idxer (delay (->call-indexer allocator tx-ops-vec scan-src prepare-ra-cache tx-opts))
                      !sql-idxer (delay (->sql-indexer allocator metadata-mgr buffer-pool iid-mgr
                                                       log-op-idxer temporal-idxer doc-idxer
                                                       tx-ops-vec scan-src prepare-ra-cache tx-opts))]
                  (dotimes [tx-op-idx (.getValueCount tx-ops-vec)]
                    (when-let [more-tx-ops (case (.getTypeId tx-ops-vec tx-op-idx)
                                             0 (.indexOp ^OpIndexer @!sql-idxer tx-op-idx)
                                             1 (.indexOp ^OpIndexer @!put-idxer tx-op-idx)
                                             2 (.indexOp ^OpIndexer @!delete-idxer tx-op-idx)
                                             3 (.indexOp ^OpIndexer @!evict-idxer tx-op-idx)
                                             4 (.indexOp ^OpIndexer @!call-idxer tx-op-idx)
                                             5 (throw abort-exn))]
                      (try
                        (index-tx-ops more-tx-ops)
                        (finally
                          (util/try-close more-tx-ops)))))))]
        (let [e (try
                  (index-tx-ops tx-ops-vec)
                  (catch core2.RuntimeException e e)
                  (catch core2.IllegalArgumentException e e)
                  (catch InterruptedException e (throw e))
                  (catch Throwable t
                    (log/error t "error in indexer - FIXME memory leak here?")
                    (throw t)))
              wm-lock-stamp (.writeLock wm-lock)]
          (try
            (if e
              (do
                (when (not= e abort-exn)
                  (log/debug e "aborted tx"))
                (.abort temporal-idxer)
                (.abort log-op-idxer)
                (.abort doc-idxer))

              (do
                (let [tx-row-count (.commit doc-idxer)]
                  (set! (.chunk-row-count this) (+ chunk-row-count tx-row-count)))

                (.commit log-op-idxer)

                (let [evicted-row-ids (.commit temporal-idxer)]
                  #_{:clj-kondo/ignore [:missing-body-in-when]}
                  (when-not (.isEmpty evicted-row-ids)
                    ;; TODO create work item
                    ))))

            (set! (.-latest-completed-tx this) tx-key)

            (finally
              (.unlock wm-lock wm-lock-stamp)))))

      (when (>= chunk-row-count max-rows-per-chunk)
        (finish-chunk! this))

      tx-key))

  (openWatermark [this tx-key]
    (letfn [(maybe-existing-wm []
              (when-let [^ISharedWatermark wm (.shared-wm this)]
                (let [wm-tx-key (.txBasis wm)]
                  (when (or (nil? tx-key)
                            (and wm-tx-key
                                 (<= (.tx-id tx-key) (.tx-id wm-tx-key))))
                    (.retain wm)))))]
      (or (let [wm-lock-stamp (.readLock wm-lock)]
            (try
              (maybe-existing-wm)
              (finally
                (.unlock wm-lock wm-lock-stamp))))

          (let [wm-lock-stamp (.writeLock wm-lock)]
            (try
              (or (maybe-existing-wm)
                  (let [^ISharedWatermark old-wm (.shared-wm this)]
                    (try
                      (let [shared-wm (.wrapWatermark wm-mgr
                                                      (wm/->Watermark tx-key
                                                                      (snapshot-live-cols live-columns)
                                                                      (Collections/emptySortedMap)
                                                                      (.getTemporalWatermark temporal-mgr)
                                                                      chunk-idx max-rows-per-block true))]
                        (set! (.shared-wm this) shared-wm)

                        (.retain shared-wm))
                      (finally
                        (some-> old-wm .release)))))

              (finally
                (.unlock wm-lock wm-lock-stamp)))))))

  (latestCompletedTx [_] latest-completed-tx)

  IndexerPrivate
  (writeColumn [_this live-column]
    (let [^VectorSchemaRoot live-root (.liveRoot live-column)]
      (with-open [write-root (VectorSchemaRoot/create (.getSchema live-root) allocator)]
        (let [loader (VectorLoader. write-root)
              row-counts (blocks/row-id-aligned-blocks live-root chunk-idx max-rows-per-block)]
          (with-open [^ICursor slices (blocks/->slices live-root row-counts)]
            (util/build-arrow-ipc-byte-buffer write-root :file
              (fn [write-batch!]
                (.forEachRemaining slices
                                   (reify Consumer
                                     (accept [_ sliced-root]
                                       (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                                         (.load loader arb)
                                         (write-batch!))))))))))))

  (closeCols [_this]
    (doseq [^LiveColumn live-column (->> (vals live-columns) (mapcat vals))]
      (util/try-close live-column))

    (.clear live-columns)
    (.clear log-indexer))

  Finish
  (finish-chunk! [this]
    (when-not (.isEmpty live-columns)
      (log/debugf "finishing chunk '%x', tx '%s'" chunk-idx (pr-str latest-completed-tx))

      @(CompletableFuture/allOf (->> (cons
                                      (.finishChunk log-indexer chunk-idx)
                                      (for [[^String table-name, ^Map table-live-cols] live-columns
                                            [^String col-name, ^LiveColumn live-column] table-live-cols]
                                        (.putObject object-store (meta/->chunk-obj-key chunk-idx table-name col-name) (.writeColumn this live-column))))
                                     (into-array CompletableFuture)))
      (.registerNewChunk temporal-mgr chunk-idx)
      (.registerNewChunk metadata-mgr (live-cols->live-roots live-columns) chunk-idx)

      (let [wm-lock-stamp (.writeLock wm-lock)]
        (try
          (set! (.-chunk-idx this) (+ chunk-idx chunk-row-count))
          (set! (.-chunk-row-count this) 0)

          (when-let [^ISharedWatermark shared-wm (.shared-wm this)]
            (set! (.shared-wm this) nil)
            (.release shared-wm))

          (.closeCols this)
          (log/debug "finished chunk.")
          (finally
            (.unlock wm-lock wm-lock-stamp))))))

  Closeable
  (close [this]
    (.closeCols this)
    (.close log-indexer)
    (some-> shared-wm .release)))

(defmethod ig/prep-key :core2/indexer [_ opts]
  (merge {:allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :metadata-mgr (ig/ref ::meta/metadata-manager)
          :temporal-mgr (ig/ref ::temporal/temporal-manager)
          :watermark-mgr (ig/ref ::wm/watermark-manager)
          :internal-id-mgr (ig/ref :core2.indexer/internal-id-manager)
          :buffer-pool (ig/ref ::bp/buffer-pool)
          :log-indexer (ig/ref :core2.indexer/log-indexer)
          :row-counts (ig/ref :core2/row-counts)
          :prepare-ra-cache (ig/ref :core2/prepare-ra-cache)}
         opts))

(defmethod ig/init-key :core2/indexer
  [_ {:keys [allocator object-store metadata-mgr buffer-pool ^ITemporalManager temporal-mgr, ^IWatermarkManager watermark-mgr, internal-id-mgr log-indexer prepare-ra-cache]
      {:keys [max-rows-per-chunk max-rows-per-block]} :row-counts
      :as deps}]

  (let [{:keys [latest-row-id latest-tx]} (log-idx/latest-tx deps)
        chunk-idx (if latest-row-id
                    (inc (long latest-row-id))
                    0)]

    (Indexer. allocator object-store metadata-mgr buffer-pool temporal-mgr internal-id-mgr watermark-mgr
              log-indexer

              max-rows-per-chunk max-rows-per-block
              (ConcurrentSkipListMap.) ; live-columns
              prepare-ra-cache
              chunk-idx latest-tx 0

              nil ; watermark
              (StampedLock.))))

(defmethod ig/halt-key! :core2/indexer [_ ^AutoCloseable indexer]
  (.close indexer))
