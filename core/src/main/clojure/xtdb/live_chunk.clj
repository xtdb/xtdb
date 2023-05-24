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
            [xtdb.vector.indirect :as iv]
            [xtdb.vector.writer :as vw])
  (:import [clojure.lang MapEntry]
           java.lang.AutoCloseable
           (java.util ArrayList HashMap List Map)
           (java.util.concurrent CompletableFuture)
           java.util.concurrent.atomic.AtomicInteger
           (java.util.function Consumer IntPredicate)
           java.util.stream.IntStream
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector ValueVector VectorLoader VectorSchemaRoot VectorUnloader)
           org.roaringbitmap.longlong.Roaring64Bitmap
           org.roaringbitmap.RoaringBitmap
           xtdb.ICursor
           (xtdb.metadata IMetadataManager IMetadataPredicate ITableMetadata ITableMetadataWriter)
           xtdb.object_store.ObjectStore
           (xtdb.vector IIndirectVector IRelationWriter IVectorWriter)))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IRowCounter
  (^int blockIdx [])
  (^long blockRowCount [])
  (^long chunkRowCount [])
  (^java.util.List blockRowCounts [])

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
    (cond-> (vec block-row-counts)
      (pos? block-row-count) (conj block-row-count)))

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

  (^xtdb.vector.IIndirectRelation liveRow [^long rowId])
  (^void writeRowId [^long rowId])
  (^boolean containsRowId [^long rowId])

  (^xtdb.live_chunk.ILiveTableWatermark openWatermark [^boolean retain])

  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveTableWatermark
  (^java.util.Map columnTypes [])
  (^xtdb.ICursor #_<IIR> liveBlocks [^java.util.Set #_<String> colNames,
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
  (^boolean isBlockFull [])

  (^void finishBlock [])
  (^void nextBlock [])
  (^java.util.concurrent.CompletableFuture finishChunk [^xtdb.api.protocols.TransactionInstant latestCompletedTx])
  (^void nextChunk [])
  (^void close []))

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

(defn- retain-vec [^ValueVector v]
  (-> (.getTransferPair v (.getAllocator v))
      (doto (.splitAndTransfer 0 (.getValueCount v)))
      (.getTo)))

(defn- open-wm-rel ^xtdb.vector.IIndirectRelation [^IRelationWriter rel, ^BigIntVector row-id-vec, retain?]
  (let [out-cols (ArrayList.)]
    (try
      (doseq [^IIndirectVector v (cons (iv/->direct-vec row-id-vec)
                                       (mapv vw/vec-wtr->rdr rel))]
        (.add out-cols (iv/->direct-vec (cond-> (.getVector v)
                                          retain? (retain-vec)))))

      (iv/->indirect-rel out-cols)

      (catch Throwable t
        (when retain? (run! util/try-close out-cols))
        (throw t)))))

(deftype LiveTableTx [^BufferAllocator allocator, ^ObjectStore object-store
                      ^String table-name, ^ITableMetadataWriter table-metadata-writer
                      ^IRelationWriter static-rel, ^BigIntVector static-row-id-vec, ^Roaring64Bitmap static-row-id-bitmap
                      ^IRelationWriter transient-rel, ^BigIntVector transient-row-id-vec, ^Roaring64Bitmap transient-row-id-bitmap
                      ^IRowCounter row-counter]
  ILiveTableTx
  (writer [_] transient-rel)

  (liveRow [_ row-id]
    (letfn [(live-row* [^IRelationWriter rel, ^BigIntVector row-id-vec, ^Roaring64Bitmap row-id-bitmap]
              (when (.contains row-id-bitmap row-id)
                (let [idx (-> (IntStream/range 0 (.getValueCount row-id-vec))
                              (.filter (reify IntPredicate
                                         (test [_ idx]
                                           (= row-id (.get row-id-vec idx)))))
                              (.findFirst)
                              (.getAsInt))]
                  (iv/select (vw/rel-wtr->rdr rel) (int-array [idx])))))]

      (or (live-row* static-rel static-row-id-vec static-row-id-bitmap)
          (live-row* transient-rel transient-row-id-vec transient-row-id-bitmap))))

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
                         (into {} (map (fn [^IVectorWriter col]
                                         (let [v (.getVector col)]
                                           (MapEntry/create (.getName v) (types/field->col-type (.getField v))))))))
          row-counts (.blockRowCounts row-counter)
          static-wm-rel (open-wm-rel static-rel static-row-id-vec retain?)
          transient-wm-rel (open-wm-rel transient-rel transient-row-id-vec false)]

      (reify ILiveTableWatermark
        (columnTypes [_] col-types)

        (liveBlocks [_ col-names metadata-pred]
          (let [excluded-block-idxs (->excluded-block-idxs (.tableMetadata table-metadata-writer) metadata-pred)]
            (util/->concat-cursor (-> (iv/->indirect-rel (->> static-wm-rel
                                                              (filter (comp col-names #(.getName ^IIndirectVector %)))))
                                      (iv/with-absent-cols allocator col-names)
                                      (iv/->slice-cursor row-counts)
                                      (without-block-idxs excluded-block-idxs))
                                  (-> (iv/->indirect-rel (->> transient-wm-rel
                                                              (filter (comp col-names #(.getName ^IIndirectVector %)))))
                                      (iv/with-absent-cols allocator col-names)
                                      (iv/->slice-cursor [(.rowCount transient-wm-rel)])))))

        AutoCloseable
        (close [_] (when retain? (.close static-wm-rel))))))

  (commit [_]
    (.addRows row-counter (.getValueCount transient-row-id-vec))
    (doto static-row-id-bitmap (.or transient-row-id-bitmap))

    (doto (vw/->writer static-row-id-vec)
      (vw/append-vec (iv/->direct-vec transient-row-id-vec))
      (.getVector))

    (let [copier (.rowCopier static-rel (vw/rel-wtr->rdr transient-rel))]
      (dotimes [idx (.getValueCount transient-row-id-vec)]
        (.copyRow copier idx)))

    (.clear transient-rel))

  AutoCloseable
  (close [_]
    (.close transient-rel)
    (util/try-close transient-row-id-vec)))

(defn- ->col-metadata-writer ^xtdb.metadata.IColumnMetadataWriter [^ITableMetadataWriter table-metadata-writer, ^Map col-metadata-writers, ^String col-name]
  (.computeIfAbsent col-metadata-writers col-name
                    (util/->jfn
                      (fn [col-name]
                        (.columnMetadataWriter table-metadata-writer col-name)))))

(deftype LiveTable [^BufferAllocator allocator, ^ObjectStore object-store, ^IMetadataManager metadata-mgr
                    ^String table-name,
                    ^IRelationWriter static-rel, ^BigIntVector row-id-vec, ^Roaring64Bitmap row-id-bitmap
                    ^ITableMetadataWriter table-metadata-writer, ^Map col-metadata-writers
                    ^long chunk-idx, ^IRowCounter row-counter]
  ILiveTable
  (startTx [_]
    (LiveTableTx. allocator object-store table-name table-metadata-writer
                  static-rel row-id-vec row-id-bitmap
                  (vw/->rel-writer allocator) (BigIntVector. (types/col-type->field "_row_id" :i64) allocator) (Roaring64Bitmap.)
                  row-counter))

  (openWatermark [_ retain?]
    (let [col-types (->> static-rel
                         (into {} (map (fn [^IVectorWriter col]
                                         (let [v (.getVector col)]
                                           (MapEntry/create (.getName v) (types/field->col-type (.getField v))))))))
          row-counts (.blockRowCounts row-counter)
          wm-rel (open-wm-rel static-rel row-id-vec retain?)]

      (reify ILiveTableWatermark
        (columnTypes [_] col-types)

        (liveBlocks [_ col-names metadata-pred]
          (let [excluded-block-idxs (->excluded-block-idxs (.tableMetadata table-metadata-writer) metadata-pred)]
            (-> (iv/->indirect-rel (->> wm-rel
                                        (filter (comp col-names #(.getName ^IIndirectVector %)))))
                (iv/with-absent-cols allocator col-names)
                (iv/->slice-cursor row-counts)
                (without-block-idxs excluded-block-idxs))))

        AutoCloseable
        (close [_] (when retain? (.close wm-rel))))))

  (finishBlock [_]
    (doseq [^IIndirectVector live-vec (cons (iv/->direct-vec row-id-vec) (map vw/vec-wtr->rdr static-rel))]
      (doto (->col-metadata-writer table-metadata-writer col-metadata-writers (.getName live-vec))
        (.writeMetadata (iv/slice-col live-vec
                                      (.chunkRowCount row-counter)
                                      (.blockRowCount row-counter))
                        (.blockIdx row-counter))))

    (.nextBlock row-counter))

  (finishChunk [_]
    (let [row-counts (.blockRowCounts row-counter)

          !fut (-> (CompletableFuture/allOf
                    (->> (cons row-id-vec (map #(.getVector ^IVectorWriter %) static-rel))
                         (map (fn [^ValueVector live-vec]
                                (let [live-root (VectorSchemaRoot/of (into-array [live-vec]))]
                                  (doto (->col-metadata-writer table-metadata-writer col-metadata-writers (.getName live-vec))
                                    (.writeMetadata (iv/->direct-vec live-vec) -1))

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
                          (.openTableMetadataWriter metadata-mgr table chunk-idx) (HashMap.)
                          chunk-idx (->row-counter (.blockIdx row-counter))))

            (->live-table-tx [table-name]
              (-> ^ILiveTable (.computeIfAbsent live-tables table-name
                                                (util/->jfn ->live-table))
                  (.startTx)))]

      (.computeIfAbsent live-table-txs table-name
                        (util/->jfn ->live-table-tx))))

  (openWatermark [_]
    (let [wms (HashMap.)]
      (try
        (doseq [[table-name ^ILiveTableTx live-table] live-table-txs]
          (.put wms table-name (.openWatermark live-table false)))

        (doseq [[table-name ^ILiveTable live-table] live-tables]
          (.computeIfAbsent wms table-name
                            (util/->jfn (fn [_] (.openWatermark live-table false)))))

        (reify ILiveChunkWatermark
          (liveTable [_ table-name] (.get wms table-name))

          AutoCloseable
          (close [_] (run! util/try-close (.values wms))))

        (catch Throwable t
          (run! util/try-close (.values wms))
          (throw t)))))

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
    (let [wms (HashMap.)]
      (try
        (doseq [[table-name ^ILiveTable live-table] live-tables]
          (.put wms table-name (.openWatermark live-table true)))

        (reify ILiveChunkWatermark
          (liveTable [_ table-name] (.get wms table-name))

          AutoCloseable
          (close [_] (run! util/try-close (.values wms))))

        (catch Throwable t
          (run! util/try-close (.values wms))
          (throw t)))))

  (chunkIdx [_] chunk-idx)
  (isBlockFull [_] (>= (.blockRowCount row-counter) rows-per-block))
  (isChunkFull [_] (>= (.chunkRowCount row-counter) rows-per-chunk))

  (finishBlock [_]
    (doseq [^ILiveTable live-table (vals live-tables)]
      (.finishBlock live-table)))

  (nextBlock [_] (.nextBlock row-counter))

  (finishChunk [_ latest-completed-tx]
    (let [futs (for [^ILiveTable live-table (vals live-tables)]
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
