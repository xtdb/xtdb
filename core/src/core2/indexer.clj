(ns core2.indexer
  (:require [clojure.tools.logging :as log]
            [core2.align :as align]
            [core2.api :as c2]
            [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            [core2.buffer-pool :as bp]
            [core2.datalog :as d]
            [core2.error :as err]
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
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           core2.operator.scan.ScanSource
           (core2.temporal ITemporalManager ITemporalTxIndexer)
           (core2.vector IIndirectRelation IIndirectVector IVectorWriter)
           (core2.watermark IWatermarkManager)
           (java.io ByteArrayInputStream Closeable)
           java.lang.AutoCloseable
           java.nio.ByteBuffer
           (java.time Instant ZoneId)
           (java.util Collections HashMap Map TreeMap)
           (java.util.concurrent CompletableFuture ConcurrentHashMap ConcurrentSkipListMap)
           (java.util.function Consumer Function)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector TimeStampMicroTZVector TimeStampVector ValueVector VarBinaryVector VarCharVector VectorLoader VectorSchemaRoot VectorUnloader)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
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
(definterface IInternalIdManager
  (^long getOrCreateInternalId [^String table, ^Object id, ^long row-id])
  (^boolean isKnownId [^String table, ^Object id]))

(defn- normalize-id [id]
  (cond-> id
    (bytes? id) (ByteBuffer/wrap)))

(defmethod ig/prep-key ::internal-id-manager [_ opts]
  (merge {:metadata-mgr (ig/ref ::meta/metadata-manager)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)
          :allocator (ig/ref :core2/allocator)}
         opts))

(deftype InternalIdManager [^Map id->internal-id]
  IInternalIdManager
  (getOrCreateInternalId [_ table id row-id]
    (.computeIfAbsent id->internal-id
                      [table (normalize-id id)]
                      (reify Function
                        (apply [_ _]
                          ;; big endian for index distribution
                          (Long/reverseBytes row-id)))))

  (isKnownId [_ table id]
    (.containsKey id->internal-id [table (normalize-id id)]))

  Closeable
  (close [_]
    (.clear id->internal-id)))

(defmethod ig/init-key ::internal-id-manager [_ {:keys [allocator ^IBufferPool buffer-pool, ^IMetadataManager metadata-mgr]}]
  (let [iid-mgr (InternalIdManager. (ConcurrentHashMap.))
        known-chunks (.knownChunks metadata-mgr)]
    (doseq [chunk-idx known-chunks]
      ;; once we have split storage per-table this should get a lot simpler/faster
      ;; HACK I could probably be a better async citizen here, but it's at startup,
      ;; and hopefully the above should make this easier anyway
      (with-open [id-chunks (-> @(.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx "id"))
                                (util/->chunks {:close-buffer? true}))
                  table-chunks (-> @(.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx "_table"))
                                   (util/->chunks {:close-buffer? true}))]
        (let [roots (HashMap.)]
          (while (and (.tryAdvance id-chunks
                                   (reify Consumer
                                     (accept [_ id-root] (.put roots :id id-root))))
                      (.tryAdvance table-chunks
                                   (reify Consumer
                                     (accept [_ table-root] (.put roots :table table-root)))))
            (let [row-ids (->> (for [^VectorSchemaRoot root (.values roots)]
                                 (align/->row-id-bitmap (.getVector root 0)))

                               ^Roaring64Bitmap
                               (reduce (fn [^Roaring64Bitmap l, ^Roaring64Bitmap r]
                                         (doto l (.and r))))
                               (.toArray))]
              (with-open [row-id-col (-> (vw/open-vec allocator "_row-id" :i64 row-ids)
                                         (iv/->direct-vec))]
                (let [aligned-roots (align/align-vectors (.values roots) (iv/->indirect-rel [row-id-col]))
                      id-col (.vectorForName aligned-roots "id")
                      id-vec (.getVector id-col)
                      table-col (.vectorForName aligned-roots "_table")
                      table-vec (.getVector table-col)
                      row-id-col (.vectorForName aligned-roots "_row-id")
                      ^BigIntVector row-id-vec (.getVector row-id-col)]
                  (dotimes [idx (.rowCount aligned-roots)]
                    (.getOrCreateInternalId iid-mgr
                                            (t/get-object table-vec (.getIndex id-col idx))
                                            (t/get-object id-vec (.getIndex id-col idx))
                                            (.get row-id-vec (.getIndex row-id-col idx)))))))))))
    iid-mgr))

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
  (^core2.indexer.ILiveColumn getLiveColumn [^String fieldName])
  (^java.util.Map liveColumnsWith [^long rowId])
  (^long nextRowId [])
  (^long commit [] "returns: count of rows in this tx")
  (^void abort []))

(deftype DocumentIndexer [^BufferAllocator allocator, ^Map live-columns
                          ^long start-row-id, ^:unsynchronized-mutable ^long tx-row-count]
  IDocumentIndexer
  (getLiveColumn [_ field-name]
    (.computeIfAbsent live-columns field-name
                      (util/->jfn
                        (fn [field-name]
                          (->live-column allocator field-name)))))

  (liveColumnsWith [_ row-id]
    (->> live-columns
         (into {} (filter (comp (fn [^LiveColumn live-col]
                                  (.containsRowId live-col row-id))
                                val)))))

  (nextRowId [this]
    (let [tx-row-count tx-row-count]
      (set! (.tx-row-count this) (inc tx-row-count))
      (+ start-row-id tx-row-count)))

  (commit [_]
    (doseq [^ILiveColumn live-col (vals live-columns)]
      (.commit live-col))

    tx-row-count)

  (abort [_]
    (doseq [^ILiveColumn live-col (vals live-columns)]
      (.abort live-col))))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface TransactionIndexer
  (^core2.watermark.Watermark getWatermark [])
  (^core2.api.TransactionInstant indexTx [^core2.api.TransactionInstant tx
                                          ^org.apache.arrow.vector.VectorSchemaRoot txRoot])
  (^core2.api.TransactionInstant latestCompletedTx []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IndexerPrivate
  (^java.nio.ByteBuffer writeColumn [^core2.indexer.LiveColumn live-column])
  (^void closeCols [])
  (^void finishChunk []))

(defn- snapshot-live-cols [^Map live-columns]
  (Collections/unmodifiableSortedMap
   (reduce-kv (fn [^Map acc k ^LiveColumn col]
                (doto acc (.put k (util/slice-root (.liveRoot col)))))
              (TreeMap.)
              live-columns)))

(def ^:private log-ops-col-type
  '[:union #{:null
             [:list
              [:struct {iid :i64
                        row-id [:union #{:null :i64}]
                        application-time-start [:union #{:null [:timestamp-tz :micro "UTC"]}]
                        application-time-end [:union #{:null [:timestamp-tz :micro "UTC"]}]
                        evict? :bool}]]}])

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILogOpIndexer
  (^void logPut [^long iid, ^long rowId, ^long app-timeStart, ^long app-timeEnd])
  (^void logDelete [^long iid, ^long app-timeStart, ^long app-timeEnd])
  (^void logEvict [^long iid])
  (^void commit [])
  (^void abort []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILogIndexer
  (^core2.indexer.ILogOpIndexer startTx [^core2.api.TransactionInstant txKey])
  (^java.nio.ByteBuffer writeLog [])
  (^void clear [])
  (^void close []))

(defn- ->log-indexer [^BufferAllocator allocator, ^long max-rows-per-block]
  (let [log-writer (vw/->rel-writer allocator)
        transient-log-writer (vw/->rel-writer allocator)

        ;; we're ignoring the writers for tx-id and sys-time, because they're simple primitive vecs and we're only writing to idx 0
        ^BigIntVector tx-id-vec (-> (.writerForName transient-log-writer "tx-id" :i64)
                                    (.getVector))

        ^TimeStampMicroTZVector sys-time-vec (-> (.writerForName transient-log-writer "system-time" [:timestamp-tz :micro "UTC"])
                                                 (.getVector))

        ops-writer (.asList (.writerForName transient-log-writer "ops" log-ops-col-type))
        ^ListVector ops-vec (.getVector ops-writer)
        ops-data-writer (.asStruct (.getDataWriter ops-writer))

        row-id-writer (.writerForName ops-data-writer "row-id")
        ^BigIntVector row-id-vec (.getVector row-id-writer)
        iid-writer (.writerForName ops-data-writer "iid")
        ^BigIntVector iid-vec (.getVector iid-writer)

        app-time-start-writer (.writerForName ops-data-writer "application-time-start")
        ^TimeStampMicroTZVector app-time-start-vec (.getVector app-time-start-writer)
        app-time-end-writer (.writerForName ops-data-writer "application-time-end")
        ^TimeStampMicroTZVector app-time-end-vec (.getVector app-time-end-writer)

        evict-writer (.writerForName ops-data-writer "evict?")
        ^BitVector evict-vec (.getVector evict-writer)]

    (reify ILogIndexer
      (startTx [_ tx-key]
        (.startValue ops-writer)
        (doto tx-id-vec
          (.setSafe 0 (.tx-id tx-key))
          (.setValueCount 1))
        (doto sys-time-vec
          (.setSafe 0 (util/instant->micros (.sys-time tx-key)))
          (.setValueCount 1))

        (reify ILogOpIndexer
          (logPut [_ iid row-id app-time-start app-time-end]
            (let [op-idx (.startValue ops-data-writer)]
              (.setSafe row-id-vec op-idx row-id)
              (.setSafe iid-vec op-idx iid)
              (.setSafe app-time-start-vec op-idx app-time-start)
              (.setSafe app-time-end-vec op-idx app-time-end)
              (.setSafe evict-vec op-idx 0)

              (.endValue ops-data-writer)))

          (logDelete [_ iid app-time-start app-time-end]
            (let [op-idx (.startValue ops-data-writer)]
              (.setSafe iid-vec op-idx iid)
              (.setNull row-id-vec op-idx)
              (.setSafe app-time-start-vec op-idx app-time-start)
              (.setSafe app-time-end-vec op-idx app-time-end)
              (.setSafe evict-vec op-idx 0)

              (.endValue ops-data-writer)))

          (logEvict [_ iid]
            (let [op-idx (.startValue ops-data-writer)]
              (.setSafe iid-vec op-idx iid)
              (.setNull row-id-vec op-idx)
              (.setNull app-time-start-vec op-idx)
              (.setNull app-time-end-vec op-idx)
              (.setSafe evict-vec op-idx 1))

            (.endValue ops-data-writer))

          (commit [_]
            (.endValue ops-writer)
            (.setValueCount ops-vec 1)
            (vw/append-rel log-writer (vw/rel-writer->reader transient-log-writer))

            (.clear transient-log-writer))

          (abort [_]
            (.clear ops-vec)
            (.setNull ops-vec 0)
            (.setValueCount ops-vec 1)
            (vw/append-rel log-writer (vw/rel-writer->reader transient-log-writer))

            (.clear transient-log-writer))))

      (writeLog [_]
        (let [log-root (let [^Iterable vecs (for [^IVectorWriter w (seq log-writer)]
                                              (.getVector w))]
                         (VectorSchemaRoot. vecs))]
          (with-open [write-root (VectorSchemaRoot/create (.getSchema log-root) allocator)]
            (let [loader (VectorLoader. write-root)
                  row-counts (blocks/list-count-blocks (.getVector log-root "ops") max-rows-per-block)]
              (with-open [^ICursor slices (blocks/->slices log-root row-counts)]
                (util/build-arrow-ipc-byte-buffer write-root :file
                  (fn [write-batch!]
                    (.forEachRemaining slices
                                       (reify Consumer
                                         (accept [_ sliced-root]
                                           (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                                             (.load loader arb)
                                             (write-batch!))))))))))))

      (clear [_]
        (.clear log-writer))

      Closeable
      (close [_]
        (.close transient-log-writer)
        (.close log-writer)))))

(defn- with-latest-log-chunk [{:keys [^ObjectStore object-store ^IBufferPool buffer-pool]} f]
  (when-let [latest-log-k (last (.listObjects object-store "log-"))]
    @(-> (.getBuffer buffer-pool latest-log-k)
         (util/then-apply
           (fn [^ArrowBuf log-buffer]
             (assert log-buffer)

             (when log-buffer
               (f log-buffer)))))))

(defn latest-tx [deps]
  (with-latest-log-chunk deps
    (fn [log-buf]
      (util/with-last-block log-buf
        (fn [^VectorSchemaRoot log-root]
          (let [tx-count (.getRowCount log-root)
                ^BigIntVector tx-id-vec (.getVector log-root "tx-id")
                ^TimeStampMicroTZVector sys-time-vec (.getVector log-root "system-time")
                ^BigIntVector row-id-vec (-> ^ListVector (.getVector log-root "ops")
                                             ^StructVector (.getDataVector)
                                             (.getChild "row-id"))]
            {:latest-tx (c2/->TransactionInstant (.get tx-id-vec (dec tx-count))
                                                 (util/micros->instant (.get sys-time-vec (dec tx-count))))
             :latest-row-id (.get row-id-vec (dec (.getValueCount row-id-vec)))}))))))

(def ^:private abort-exn (err/runtime-err :abort-exn))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface OpIndexer
  (^org.apache.arrow.vector.complex.DenseUnionVector indexOp [^long tx-op-idx]
   "returns a tx-ops-vec of more operations (mostly for `:call`)"))

(definterface DocRowCopier
  (^void copyDocRow [^long rowId, ^int srcIdx]))

(defn- doc-row-copier ^core2.indexer.DocRowCopier [^IDocumentIndexer doc-idxer, ^IIndirectVector col-rdr]
  (let [col-name (.getName col-rdr)
        ^LiveColumn live-col (.getLiveColumn doc-idxer col-name)
        ^IVectorWriter vec-writer (.contentWriter live-col)
        row-copier (.rowCopier col-rdr vec-writer)]
    (reify DocRowCopier
      (copyDocRow [_ row-id src-idx]
        (when (.isPresent col-rdr src-idx)
          (.writeRowId live-col row-id)
          (.startValue vec-writer)
          (.copyRow row-copier src-idx)
          (.endValue vec-writer))))))

(defn- put-doc-copier ^core2.indexer.DocRowCopier [^TransactionIndexer indexer, ^DenseUnionVector tx-ops-vec]
  (let [doc-rdr (-> (.getStruct tx-ops-vec 1)
                    (.getChild "document")
                    (iv/->direct-vec)
                    (.structReader))
        doc-copiers (vec
                     (for [^String col-name (.structKeys doc-rdr)]
                       (doc-row-copier indexer (.readerForKey doc-rdr col-name))))]
    (reify DocRowCopier
      (copyDocRow [_ row-id src-idx]
        (doseq [^DocRowCopier doc-copier doc-copiers]
          (.copyDocRow doc-copier row-id src-idx))))))

(defn- ->put-indexer ^core2.indexer.OpIndexer [^IInternalIdManager iid-mgr ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^IDocumentIndexer doc-idxer
                                               ^DenseUnionVector tx-ops-vec, ^Instant current-time]
  (let [put-vec (.getStruct tx-ops-vec 1)
        ^DenseUnionVector doc-duv (.getChild put-vec "document" DenseUnionVector)
        ^TimeStampVector app-time-start-vec (.getChild put-vec "application_time_start")
        ^TimeStampVector app-time-end-vec (.getChild put-vec "application_time_end")
        doc-copier (put-doc-copier doc-idxer tx-ops-vec)
        current-time-µs (util/instant->micros current-time)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [row-id (.nextRowId doc-idxer)
              put-offset (.getOffset tx-ops-vec tx-op-idx)
              leg-type-id (.getTypeId doc-duv put-offset)
              leg-offset (.getOffset doc-duv put-offset)
              id-vec (-> ^StructVector (.getVectorByType doc-duv leg-type-id)
                         (.getChild "id"))
              ^VarCharVector table-vec (-> ^StructVector (.getVectorByType doc-duv leg-type-id)
                                           (.getChild "_table"))
              table (or (some-> table-vec (t/get-object leg-offset)) "xt_docs")
              eid (t/get-object id-vec leg-offset)
              new-entity? (not (.isKnownId iid-mgr table eid))
              iid (.getOrCreateInternalId iid-mgr table eid row-id)
              start-app-time (if (.isNull app-time-start-vec put-offset)
                               current-time-µs
                               (.get app-time-start-vec put-offset))
              end-app-time (if (.isNull app-time-end-vec put-offset)
                             util/end-of-time-μs
                             (.get app-time-end-vec put-offset))]
          (.copyDocRow doc-copier row-id put-offset)
          (.logPut log-op-idxer iid row-id start-app-time end-app-time)
          (.indexPut temporal-idxer iid row-id start-app-time end-app-time new-entity?))

        nil))))

(defn- ->delete-indexer
  ^core2.indexer.OpIndexer
  [^IInternalIdManager iid-mgr, ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^IDocumentIndexer doc-idxer
   ^DenseUnionVector tx-ops-vec, ^Instant current-time]
  (let [delete-vec (.getStruct tx-ops-vec 2)
        ^VarCharVector table-vec (.getChild delete-vec "_table" VarCharVector)
        ^DenseUnionVector id-vec (.getChild delete-vec "id" DenseUnionVector)
        ^TimeStampVector app-time-start-vec (.getChild delete-vec "application_time_start")
        ^TimeStampVector app-time-end-vec (.getChild delete-vec "application_time_end")
        current-time-µs (util/instant->micros current-time)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [row-id (.nextRowId doc-idxer)
              delete-offset (.getOffset tx-ops-vec tx-op-idx)
              table (or (t/get-object table-vec delete-offset) "xt_docs")
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

(defn- find-fn [allocator sci-ctx scan-src tx-opts fn-id]
  ;; HACK: assume xt_docs here...
  ;; TODO confirm fn-body doc key

  (let [pq (op/prepare-ra '[:scan xt_docs [{id (= id ?id)} fn]])]
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

(defn- tx-fn-q [allocator scan-src tx-opts q & args]
  ;; bear in mind Datalog doesn't yet look at `app-time-as-of-now?`, essentially just assumes its true.
  (let [q (into tx-opts q)]
    (with-open [res (d/open-datalog-query allocator q scan-src args)]
      (vec (iterator-seq res)))))

(defn- tx-fn-sql
  ([allocator scan-src tx-opts query]
   (tx-fn-sql allocator scan-src tx-opts query {}))

  ([allocator scan-src tx-opts query query-opts]
   (try
     (let [query-opts (into tx-opts query-opts)
           pq (sql/prepare-sql query query-opts)]
       (with-open [res (sql/open-sql-query allocator pq scan-src query-opts)]
         (vec (iterator-seq res))))
     (catch Throwable e
       (log/error e)
       (throw e)))))

(def ^:private !last-tx-fn-error (atom nil))

(defn reset-tx-fn-error! []
  (first (reset-vals! !last-tx-fn-error nil)))

(defn- ->call-indexer ^core2.indexer.OpIndexer [allocator, ^DenseUnionVector tx-ops-vec, scan-src, {:keys [tx-key] :as tx-opts}]
  (let [call-vec (.getStruct tx-ops-vec 4)
        ^DenseUnionVector fn-id-vec (.getChild call-vec "fn-id" DenseUnionVector)
        ^ListVector args-vec (.getChild call-vec "args" ListVector)

        ;; TODO confirm/expand API that we expose to tx-fns
        sci-ctx (sci/init {:bindings {'q (partial tx-fn-q allocator scan-src tx-opts)
                                      'sql-q (partial tx-fn-sql allocator scan-src tx-opts)
                                      '*current-tx* tx-key}})]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (try
          (let [call-offset (.getOffset tx-ops-vec tx-op-idx)
                fn-id (t/get-object fn-id-vec call-offset)
                tx-fn (find-fn allocator (sci/fork sci-ctx) scan-src tx-opts fn-id)

                args (t/get-object args-vec call-offset)

                res (try
                      (sci/binding [sci/out *out*
                                    sci/in *in*]
                        (apply tx-fn args))

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

(defn- table-row-writer [^IDocumentIndexer doc-idxer, ^String table-name]
  (let [^LiveColumn live-col (.getLiveColumn doc-idxer "_table")
        ^IVectorWriter vec-writer (.contentWriter live-col)
        ^IVectorWriter vec-str-writer (.writerForType (.asDenseUnion vec-writer) :utf8)]
    (reify DocRowCopier
      (copyDocRow [_ row-id _src-idx]
        (.writeRowId live-col row-id)

        (.startValue vec-writer)
        (.startValue vec-str-writer)
        (t/write-value! table-name vec-str-writer)
        (.endValue vec-str-writer)
        (.endValue vec-writer)))))

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
              doc-row-copiers (vec
                               (cons (table-row-writer doc-idxer table)
                                     (for [^IIndirectVector in-col in-rel
                                           :when (not (temporal/temporal-column? (.getName in-col)))]
                                       (doc-row-copier doc-idxer in-col))))
              id-col (.vectorForName in-rel "id")
              app-time-start-rdr (some-> (.vectorForName in-rel "application_time_start") (.monoReader))
              app-time-end-rdr (some-> (.vectorForName in-rel "application_time_end") (.monoReader))]
          (dotimes [idx row-count]
            (let [row-id (.nextRowId doc-idxer)]
              (doseq [^DocRowCopier doc-row-copier doc-row-copiers]
                (.copyDocRow doc-row-copier row-id idx))

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
    (indexOp [_ in-rel _query-opts]
      (let [row-count (.rowCount in-rel)
            doc-row-copiers (->> (for [^IIndirectVector in-col in-rel
                                       :let [col-name (.getName in-col)]
                                       :when (not (temporal/temporal-column? col-name))]
                                   [col-name (doc-row-copier doc-idxer in-col)])
                                 (into {}))
            iid-rdr (.monoReader (.vectorForName in-rel "_iid"))
            row-id-rdr (.monoReader (.vectorForName in-rel "_row-id"))
            app-time-start-rdr (.monoReader (.vectorForName in-rel "application_time_start"))
            app-time-end-rdr (.monoReader (.vectorForName in-rel "application_time_end"))]
        (dotimes [idx row-count]
          (let [old-row-id (.readLong row-id-rdr idx)
                new-row-id (.nextRowId doc-idxer)
                iid (.readLong iid-rdr idx)
                start-app-time (.readLong app-time-start-rdr idx)
                end-app-time (.readLong app-time-end-rdr idx)]
            (doseq [^DocRowCopier doc-row-copier (vals doc-row-copiers)]
              (.copyDocRow doc-row-copier new-row-id idx))

            (letfn [(copy-row [^BigIntVector root-row-id-vec, ^ValueVector content-vec]
                      (let [doc-row-copier (doc-row-copier doc-idxer (iv/->direct-vec content-vec))]
                        ;; TODO these are sorted, so we could binary search instead
                        (dotimes [idx (.getValueCount root-row-id-vec)]
                          (when (= old-row-id (.get root-row-id-vec idx))
                            (.copyDocRow doc-row-copier new-row-id idx)))))]

              (doseq [[^String col-name, ^LiveColumn live-col] (.liveColumnsWith doc-idxer old-row-id)
                      :when (not (contains? doc-row-copiers col-name))]
                (copy-row (.row-id-vec live-col) (.content-vec live-col))
                (copy-row (.transient-row-id-vec live-col) (.transient-content-vec live-col)))

              (when-let [{:keys [^long chunk-idx cols]} (meta/row-id->cols metadata-mgr old-row-id)]
                (doseq [{:keys [^String col-name ^long block-idx]} cols
                        :when (not (contains? doc-row-copiers col-name))]
                  @(-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx col-name))
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
            iid-rdr (.monoReader (.vectorForName in-rel "_iid"))
            app-time-start-rdr (.monoReader (.vectorForName in-rel "application_time_start"))
            app-time-end-rdr (.monoReader (.vectorForName in-rel "application_time_end"))]
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
            iid-rdr (.monoReader (.vectorForName in-rel "_iid"))]
        (dotimes [idx row-count]
          (let [iid (.readLong iid-rdr idx)]
            (.logEvict log-op-idxer iid)
            (.indexEvict temporal-idxer iid)))))))

(defn- ->sql-indexer ^core2.indexer.OpIndexer [^BufferAllocator allocator, ^IMetadataManager metadata-mgr, ^IBufferPool buffer-pool, ^IInternalIdManager iid-mgr
                                               ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^IDocumentIndexer doc-idxer
                                               ^DenseUnionVector tx-ops-vec, ^ScanSource scan-src
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
                    (let [pq (op/prepare-ra inner-query)]
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
       (into {} (map (fn [[col-name ^LiveColumn live-col]]
                       (MapEntry/create col-name (.liveRoot live-col)))))))

(defn- live-cols->tx-live-roots [live-cols]
  (->> live-cols
       (into {} (map (fn [[col-name ^LiveColumn live-col]]
                       (MapEntry/create col-name (.txLiveRoot live-col)))))))

(deftype Indexer [^BufferAllocator allocator
                  ^ObjectStore object-store
                  ^IMetadataManager metadata-mgr
                  ^IBufferPool buffer-pool
                  ^ITemporalManager temporal-mgr
                  ^IInternalIdManager iid-mgr
                  ^IWatermarkManager watermark-mgr
                  ^long max-rows-per-chunk
                  ^long max-rows-per-block
                  ^Map live-columns
                  ^ILogIndexer log-indexer
                  ^:volatile-mutable ^long chunk-idx
                  ^:volatile-mutable ^TransactionInstant latest-completed-tx
                  ^:volatile-mutable ^long chunk-row-count]

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
                                       temporal-idxer chunk-idx max-rows-per-block)))

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
                      !call-idxer (delay (->call-indexer allocator tx-ops-vec scan-src tx-opts))
                      !sql-idxer (delay (->sql-indexer allocator metadata-mgr buffer-pool iid-mgr
                                                      log-op-idxer temporal-idxer doc-idxer
                                                      tx-ops-vec scan-src tx-opts))]
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
        (if-let [e (try
                     (index-tx-ops tx-ops-vec)
                     (catch core2.RuntimeException e e)
                     (catch core2.IllegalArgumentException e e)
                     (catch Throwable t
                       (log/error t "error in indexer - FIXME memory leak here?")
                       (throw t)))]
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
                )))))

      (.setWatermark watermark-mgr chunk-idx tx-key (snapshot-live-cols live-columns) nil (.getTemporalWatermark temporal-mgr))
      (set! (.-latest-completed-tx this) tx-key)

      (when (>= chunk-row-count max-rows-per-chunk)
        (.finishChunk this))

      tx-key))

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
    (doseq [^LiveColumn live-column (vals live-columns)]
      (util/try-close live-column))

    (.clear live-columns)
    (.clear log-indexer))

  (finishChunk [this]
    (when-not (.isEmpty live-columns)
      (log/debugf "finishing chunk '%x', tx '%s'" chunk-idx (pr-str latest-completed-tx))

      (try
        @(CompletableFuture/allOf (->> (cons
                                        (.putObject object-store (format "log-%016x.arrow" chunk-idx) (.writeLog log-indexer))
                                        (for [[^String col-name, ^LiveColumn live-column] live-columns]
                                          (.putObject object-store (meta/->chunk-obj-key chunk-idx col-name) (.writeColumn this live-column))))
                                       (into-array CompletableFuture)))
        (.registerNewChunk temporal-mgr chunk-idx)
        (.registerNewChunk metadata-mgr (live-cols->live-roots live-columns) chunk-idx)

        (set! (.-chunk-idx this) (+ chunk-idx chunk-row-count))
        (set! (.-chunk-row-count this) 0)

        (.setWatermark watermark-mgr chunk-idx latest-completed-tx nil nil (.getTemporalWatermark temporal-mgr))
        (log/debug "finished chunk.")
        (finally
          (.closeCols this)))))

  Closeable
  (close [this]
    (.closeCols this)
    (.close log-indexer)))

(defmethod ig/prep-key ::indexer [_ opts]
  (merge {:allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :metadata-mgr (ig/ref ::meta/metadata-manager)
          :temporal-mgr (ig/ref ::temporal/temporal-manager)
          :watermark-mgr (ig/ref :core2.watermark/watermark-manager)
          :internal-id-mgr (ig/ref ::internal-id-manager)
          :buffer-pool (ig/ref ::bp/buffer-pool)
          :row-counts (ig/ref :core2/row-counts)}
         opts))

(defmethod ig/init-key ::indexer
  [_ {:keys [allocator object-store metadata-mgr buffer-pool ^ITemporalManager temporal-mgr, ^IWatermarkManager watermark-mgr, internal-id-mgr]
      {:keys [max-rows-per-chunk max-rows-per-block]} :row-counts
      :as deps}]

  (let [{:keys [latest-row-id latest-tx]} (latest-tx deps)
        chunk-idx (if latest-row-id
                    (inc (long latest-row-id))
                    0)]
    (.setWatermark watermark-mgr chunk-idx latest-tx nil nil (.getTemporalWatermark temporal-mgr))

    (Indexer. allocator object-store metadata-mgr buffer-pool temporal-mgr internal-id-mgr watermark-mgr
              max-rows-per-chunk max-rows-per-block
              (ConcurrentSkipListMap.)
              (->log-indexer allocator max-rows-per-block)
              chunk-idx latest-tx 0)))

(defmethod ig/halt-key! ::indexer [_ ^AutoCloseable indexer]
  (.close indexer))
