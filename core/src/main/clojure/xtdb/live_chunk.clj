(ns xtdb.live-chunk
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.blocks :as blocks]
            [xtdb.bloom :as bloom]
            xtdb.indexer.internal-id-manager
            [xtdb.metadata :as meta]
            xtdb.object-store
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [clojure.lang MapEntry]
           java.lang.AutoCloseable
           (java.util ArrayList HashMap List Map)
           (java.util.concurrent CompletableFuture)
           java.util.concurrent.atomic.AtomicInteger
           (java.util.function Consumer)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector ValueVector VectorLoader VectorSchemaRoot VectorUnloader)
           org.roaringbitmap.longlong.Roaring64Bitmap
           org.roaringbitmap.RoaringBitmap
           (xtdb ICursor SliceCursor)
           (xtdb.metadata IMetadataManager IMetadataPredicate ITableMetadata ITableMetadataWriter)
           xtdb.object_store.ObjectStore
           (xtdb.vector IVectorReader IRelationWriter IVectorWriter)))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IRowCounter
  (^int blockIdx [])
  (^long blockRowCount [])
  (^long chunkRowCount [])
  (^ints blockRowCounts [])

  (^void addRows [^int rowCount])
  (^int nextBlock [])
  (^void nextChunk []))

(deftype RowCounter [^:volatile-mutable ^int block-idx
                     ^:volatile-mutable ^long chunk-row-count
                     ^List block-row-counts
                     ^:volatile-mutable ^long block-row-count]
  IRowCounter
  (blockIdx [_] block-idx)
  (blockRowCount [_] block-row-count)
  (chunkRowCount [_] chunk-row-count)

  (blockRowCounts [_]
    (int-array (cond-> (vec block-row-counts)
                 (pos? block-row-count) (conj block-row-count))))

  (addRows [this row-count]
    (set! (.block-row-count this) (+ block-row-count row-count)))

  (nextBlock [this]
    (set! (.block-idx this) (inc block-idx))
    (.add block-row-counts block-row-count)
    (set! (.chunk-row-count this) (+ chunk-row-count block-row-count))
    (set! (.block-row-count this) 0))

  (nextChunk [this]
    (.clear block-row-counts)
    (set! (.chunk-row-count this) 0)
    (set! (.block-idx this) 0)))

(defn- ->row-counter ^xtdb.live_chunk.IRowCounter [^long block-idx]
  (let [block-row-counts (ArrayList. ^List (repeat block-idx 0))]
    (RowCounter. block-idx 0 block-row-counts 0)))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveTableTx
  (^xtdb.vector.IRelationWriter writer [])

  (^void writeRowId [^long rowId])
  (^boolean containsRowId [^long rowId])

  (^xtdb.live_chunk.ILiveTableWatermark openWatermark [^boolean retain])

  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveTableWatermark
  (^java.util.Map columnTypes [])
  (^xtdb.ICursor #_<RR> liveBlocks [^java.util.Set #_<String> colNames,
                                    ^xtdb.metadata.IMetadataPredicate metadataPred])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveTable
  (^xtdb.live_chunk.ILiveTableWatermark openWatermark [^boolean retain])
  (^xtdb.live_chunk.ILiveTableTx startTx [])
  (^void finishBlock [])
  (^java.util.concurrent.CompletableFuture finishChunk [])

  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveChunkWatermark
  (^xtdb.live_chunk.ILiveTableWatermark liveTable [^String table])
  (^java.util.Map allColumnTypes [])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveChunkTx
  (^xtdb.live_chunk.ILiveTableTx liveTable [^String table])
  (^xtdb.live_chunk.ILiveChunkWatermark openWatermark [])

  (^long nextRowId [])
  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveChunk
  (^xtdb.live_chunk.ILiveTable liveTable [^String table])
  (^xtdb.live_chunk.ILiveChunkWatermark openWatermark [])
  (^xtdb.live_chunk.ILiveChunkTx startTx [])

  (^long chunkIdx [])
  (^boolean isChunkFull [])
  (^long rowsPerBlock [])
  (^long blockRowCount [])

  (^void finishBlock [])
  (^void nextBlock [])
  (^java.util.concurrent.CompletableFuture finishChunk [^xtdb.api.protocols.TransactionInstant latestCompletedTx])
  (^void nextChunk [])
  (^void close []))

(defn block-full? [^ILiveChunk live-chunk]
  (<= (.rowsPerBlock live-chunk) (.blockRowCount live-chunk)))

(defn- ->excluded-block-idxs ^org.roaringbitmap.RoaringBitmap [^ITableMetadata table-metadata, ^IMetadataPredicate metadata-pred]
  (let [exclude-block-idxs (RoaringBitmap.)]
    (when-let [pred (some-> metadata-pred (.build table-metadata))]
      (dotimes [block-idx (.blockCount table-metadata)]
        (when-not (.test pred block-idx)
          (.add exclude-block-idxs block-idx))))

    exclude-block-idxs))

(defn- without-block-idxs [^ICursor inner, ^RoaringBitmap exclude-block-idxs]
  (let [!block-idx (AtomicInteger. 0)]
    (reify ICursor
      (tryAdvance [_ c]
        (let [!advanced? (volatile! false)]
          (while (and (not @!advanced?)
                      (.tryAdvance inner
                                   (reify Consumer
                                     (accept [_ el]
                                       (let [block-idx (.getAndIncrement !block-idx)]
                                         (when-not (.contains exclude-block-idxs block-idx)
                                           (.accept c el)
                                           (vreset! !advanced? true))))))))
          @!advanced?))

      (close [_] (.close inner)))))

(defn- open-wm-rel ^xtdb.vector.RelationReader [^IRelationWriter rel, ^BigIntVector row-id-vec, retain?]
  (let [out-cols (ArrayList.)]
    (try
      (.syncRowCount rel)
      (doseq [^ValueVector v (cons row-id-vec
                                   (->> (vals rel)
                                        (map #(.getVector ^IVectorWriter %))))]
        (.add out-cols (vr/vec->reader (cond-> v
                                         retain? (util/slice-vec)))))

      (vr/rel-reader out-cols)

      (catch Throwable t
        (when retain? (util/close out-cols))
        (throw t)))))

(deftype LiveTableTx [^BufferAllocator allocator, ^ObjectStore object-store
                      ^String table-name, ^ITableMetadataWriter table-metadata-writer
                      ^IRelationWriter static-rel, ^BigIntVector static-row-id-vec, ^Roaring64Bitmap static-row-id-bitmap
                      ^IRelationWriter transient-rel, ^BigIntVector transient-row-id-vec, ^Roaring64Bitmap transient-row-id-bitmap
                      ^IRowCounter row-counter]
  ILiveTableTx
  (writer [_] transient-rel)

  (writeRowId [_ row-id]
    (.addLong transient-row-id-bitmap row-id)

    (let [dest-idx (.getValueCount transient-row-id-vec)]
      (.setSafe transient-row-id-vec dest-idx row-id)
      (.setValueCount transient-row-id-vec (inc dest-idx))))

  (containsRowId [_ row-id]
    (or (.contains static-row-id-bitmap row-id)
        (.contains transient-row-id-bitmap row-id)))

  (openWatermark [_ retain?]
    (let [col-types (->> transient-rel
                         (into {} (map (fn [[col-name ^IVectorWriter col]]
                                         (let [v (.getVector col)]
                                           (MapEntry/create col-name (types/field->col-type (.getField v))))))))
          row-counts (.blockRowCounts row-counter)
          static-wm-rel (open-wm-rel static-rel static-row-id-vec retain?)
          transient-wm-rel (open-wm-rel transient-rel transient-row-id-vec false)]

      (reify ILiveTableWatermark
        (columnTypes [_] col-types)

        (liveBlocks [_ col-names metadata-pred]
          (let [excluded-block-idxs (->excluded-block-idxs (.tableMetadata table-metadata-writer) metadata-pred)]
            (util/->concat-cursor (-> (vr/rel-reader (->> static-wm-rel
                                                          (filter (comp col-names #(.getName ^IVectorReader %)))))
                                      (vr/with-absent-cols allocator col-names)
                                      (SliceCursor. row-counts)
                                      (without-block-idxs excluded-block-idxs))
                                  (-> (vr/rel-reader (->> transient-wm-rel
                                                              (filter (comp col-names #(.getName ^IVectorReader %)))))
                                      (vr/with-absent-cols allocator col-names)
                                      (SliceCursor. (int-array [(.rowCount transient-wm-rel)]))))))

        AutoCloseable
        (close [_] (when retain? (.close static-wm-rel))))))

  (commit [_]
    (.addRows row-counter (.getValueCount transient-row-id-vec))
    (doto static-row-id-bitmap (.or transient-row-id-bitmap))

    (doto (vw/->writer static-row-id-vec)
      (vw/append-vec (vr/vec->reader transient-row-id-vec))
      (.syncValueCount))

    (let [copier (.rowCopier static-rel (vw/rel-wtr->rdr transient-rel))]
      (dotimes [idx (.getValueCount transient-row-id-vec)]
        (.copyRow copier idx)))

    (.clear transient-rel))

  AutoCloseable
  (close [_]
    (.close transient-rel)
    (util/try-close transient-row-id-vec)))

(deftype LiveTable [^BufferAllocator allocator, ^ObjectStore object-store, ^IMetadataManager metadata-mgr
                    ^String table-name,
                    ^IRelationWriter static-rel, ^BigIntVector row-id-vec, ^Roaring64Bitmap row-id-bitmap
                    ^ITableMetadataWriter table-metadata-writer
                    ^long chunk-idx, ^IRowCounter row-counter]
  ILiveTable
  (startTx [_]
    (LiveTableTx. allocator object-store table-name table-metadata-writer
                  static-rel row-id-vec row-id-bitmap
                  (vw/->rel-writer allocator) (BigIntVector. (types/col-type->field "_row_id" :i64) allocator) (Roaring64Bitmap.)
                  row-counter))

  (openWatermark [_ retain?]
    (let [col-types (->> (vals static-rel)
                         (into {} (map (fn [^IVectorWriter col]
                                         (let [v (.getVector col)]
                                           (MapEntry/create (.getName v) (types/field->col-type (.getField v))))))))
          row-counts (.blockRowCounts row-counter)
          wm-rel (open-wm-rel static-rel row-id-vec retain?)]

      (reify ILiveTableWatermark
        (columnTypes [_] col-types)

        (liveBlocks [_ col-names metadata-pred]
          (let [excluded-block-idxs (->excluded-block-idxs (.tableMetadata table-metadata-writer) metadata-pred)]
            (-> (vr/rel-reader (->> wm-rel
                                      (filter (comp col-names #(.getName ^IVectorReader %)))))
                (vr/with-absent-cols allocator col-names)
                (SliceCursor. row-counts)
                (without-block-idxs excluded-block-idxs))))

        AutoCloseable
        (close [_] (when retain? (.close wm-rel))))))

  (finishBlock [_]
    (let [block-meta-wtr (.writeBlockMetadata table-metadata-writer (.blockIdx row-counter))]
      (doseq [^IVectorReader live-vec (cons (vr/vec->reader row-id-vec) (seq (vw/rel-wtr->rdr static-rel)))]
        (.writeMetadata block-meta-wtr (.select live-vec (.chunkRowCount row-counter) (.blockRowCount row-counter))))
      (.endBlock block-meta-wtr))

    (.nextBlock row-counter))

  (finishChunk [_]
    (let [row-counts (.blockRowCounts row-counter)
          block-meta-wtr (.writeBlockMetadata table-metadata-writer -1)

          !fut (-> (CompletableFuture/allOf
                    (->> (cons row-id-vec (map #(.getVector ^IVectorWriter %) (vals static-rel)))
                         (map (fn [^ValueVector live-vec]
                                (let [live-root (VectorSchemaRoot/of (into-array [live-vec]))]
                                  (.writeMetadata block-meta-wtr (vr/vec->reader live-vec))

                                  (.putObject object-store (meta/->chunk-obj-key chunk-idx table-name (.getName live-vec))
                                              (with-open [write-root (VectorSchemaRoot/create (.getSchema live-root) allocator)]
                                                (let [loader (VectorLoader. write-root)]
                                                  (with-open [^ICursor slices (blocks/->slices live-root row-counts)]
                                                    (let [buf (util/build-arrow-ipc-byte-buffer write-root :file
                                                                                                (fn [write-batch!]
                                                                                                  (.forEachRemaining slices
                                                                                                                     (reify Consumer
                                                                                                                       (accept [_ sliced-root]
                                                                                                                         (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                                                                                                                           (.load loader arb)
                                                                                                                           (write-batch!)))))))]
                                                      (.nextChunk row-counter)
                                                      buf))))))))
                         (into-array CompletableFuture)))

                   (util/then-compose
                     (fn [_]
                       (.endBlock block-meta-wtr)
                       (.finishChunk table-metadata-writer))))

          chunk-metadata (meta/live-rel->chunk-metadata table-name (vw/rel-wtr->rdr static-rel))]
      (-> !fut
          (util/then-apply (fn [_] chunk-metadata)))))

  AutoCloseable
  (close [_]
    (util/try-close static-rel)
    (util/try-close row-id-vec)
    (util/try-close table-metadata-writer)))

(deftype LiveChunkTx [^BufferAllocator allocator
                      ^ObjectStore object-store
                      ^IMetadataManager metadata-mgr
                      ^Map live-tables, ^Map live-table-txs
                      ^long chunk-idx, ^long tx-start-row, ^IRowCounter row-counter
                      ^:volatile-mutable ^long tx-row-count]
  ILiveChunkTx
  (liveTable [_ table-name]
    (letfn [(->live-table [table]
              (LiveTable. allocator object-store metadata-mgr table
                          (vw/->rel-writer allocator) (BigIntVector. (types/col-type->field "_row_id" :i64) allocator) (Roaring64Bitmap.)
                          (.openTableMetadataWriter metadata-mgr table chunk-idx)
                          chunk-idx (->row-counter (.blockIdx row-counter))))

            (->live-table-tx [table-name]
              (-> ^ILiveTable (.computeIfAbsent live-tables table-name
                                                (util/->jfn ->live-table))
                  (.startTx)))]

      (.computeIfAbsent live-table-txs table-name
        (util/->jfn ->live-table-tx))))

  (openWatermark [_]
    (util/with-close-on-catch [wms (HashMap.)]
      (doseq [[table-name ^ILiveTableTx live-table] live-table-txs]
        (.put wms table-name (.openWatermark live-table false)))

      (doseq [[table-name ^ILiveTable live-table] live-tables]
        (.computeIfAbsent wms table-name
                          (util/->jfn (fn [_] (.openWatermark live-table false)))))

      (reify ILiveChunkWatermark
        (liveTable [_ table-name] (.get wms table-name))

        (allColumnTypes [_] (update-vals wms #(.columnTypes ^ILiveTableWatermark %)))

        AutoCloseable
        (close [_] (run! util/try-close (.values wms))))))

  (nextRowId [this]
    (let [tx-row-count (.tx-row-count this)]
      (set! (.tx-row-count this) (inc tx-row-count))
      (+ tx-start-row tx-row-count)))

  (commit [_]
    (doseq [^ILiveTableTx live-table (.values live-table-txs)]
      (.commit live-table))
    (.addRows row-counter tx-row-count))

  AutoCloseable
  (close [_]
    (run! util/try-close (.values live-table-txs))
    (.clear live-table-txs)))

(deftype LiveChunk [^BufferAllocator allocator
                    ^ObjectStore object-store
                    ^IMetadataManager metadata-mgr
                    ^long rows-per-block, ^long rows-per-chunk
                    ^Map live-tables
                    ^:volatile-mutable ^long chunk-idx
                    ^IRowCounter row-counter]
  ILiveChunk
  (liveTable [_ table] (.get live-tables table))

  (startTx [_]
    (LiveChunkTx. allocator object-store metadata-mgr
                  live-tables (HashMap.)
                  chunk-idx (+ chunk-idx (.chunkRowCount row-counter) (.blockRowCount row-counter))
                  row-counter 0))

  (openWatermark [_]
    (util/with-close-on-catch [wms (HashMap.)]
      (doseq [[table-name ^ILiveTable live-table] live-tables]
        (.put wms table-name (.openWatermark live-table true)))

      (reify ILiveChunkWatermark
        (liveTable [_ table-name] (.get wms table-name))

        (allColumnTypes [_] (update-vals wms #(.columnTypes ^ILiveTableWatermark %)))

        AutoCloseable
        (close [_] (util/close wms)))))

  (chunkIdx [_] chunk-idx)
  (rowsPerBlock [_] rows-per-block)
  (blockRowCount [_] (.blockRowCount row-counter))
  (isChunkFull [_] (>= (.chunkRowCount row-counter) rows-per-chunk))

  (finishBlock [_]
    (doseq [^ILiveTable live-table (.values live-tables)]
      (.finishBlock live-table)))

  (nextBlock [_] (.nextBlock row-counter))

  (finishChunk [_ latest-completed-tx]
    (let [futs (for [^ILiveTable live-table (.values live-tables)]
                 (.finishChunk live-table))]

      (-> (CompletableFuture/allOf (into-array CompletableFuture futs))
          (util/then-apply (fn [_]
                             (.finishChunk metadata-mgr chunk-idx
                                           {:latest-completed-tx latest-completed-tx
                                            :latest-row-id (dec (+ chunk-idx (.chunkRowCount row-counter)))
                                            :tables (-> (into {} (keep deref) futs)
                                                        (util/rethrowing-cause))}))))))

  (nextChunk [this]
    (run! util/try-close (.values live-tables))
    (.clear live-tables)

    (set! (.chunk-idx this)
          (+ (.chunk-idx this) (.chunkRowCount row-counter)))

    (.nextChunk row-counter))

  AutoCloseable
  (close [_]
    (run! util/try-close (.values live-tables))
    (.clear live-tables)))

(defmethod ig/prep-key :xtdb/live-chunk [_ opts]
  (merge {:rows-per-block 1024
          :rows-per-chunk 102400
          :allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)
          :metadata-mgr (ig/ref ::meta/metadata-manager)}
         opts))

(defmethod ig/init-key :xtdb/live-chunk [_ {:keys [allocator object-store metadata-mgr ^long rows-per-block ^long rows-per-chunk]}]
  (let [chunk-idx (if-let [{:keys [^long latest-row-id]} (meta/latest-chunk-metadata metadata-mgr)]
                    (inc latest-row-id)
                    0)
        bloom-false-positive-probability (bloom/bloom-false-positive-probability? rows-per-chunk)]

    (when (> bloom-false-positive-probability 0.05)
      (log/warn "Bloom should be sized for large chunks:" rows-per-chunk
                "false positive probability:" bloom-false-positive-probability
                "bits:" bloom/bloom-bits
                "can be set via system property xtdb.bloom.bits"))

    (LiveChunk. allocator object-store metadata-mgr
                rows-per-block rows-per-chunk
                (HashMap.)
                chunk-idx (->row-counter 0))))

(defmethod ig/halt-key! :xtdb/live-chunk [_ live-chunk]
  (util/try-close live-chunk))
