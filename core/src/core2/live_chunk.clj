(ns core2.live-chunk
  (:require [clojure.tools.logging :as log]
            [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            core2.indexer.internal-id-manager
            [core2.metadata :as meta]
            core2.object-store
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import [clojure.lang MapEntry]
           core2.ICursor
           (core2.metadata IColumnMetadataWriter IMetadataManager ITableMetadataWriter)
           core2.object_store.ObjectStore
           java.lang.AutoCloseable
           (java.util ArrayList HashMap List Map)
           (java.util.concurrent CompletableFuture)
           java.util.concurrent.atomic.AtomicLong
           java.util.function.Consumer
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector ValueVector VectorLoader VectorSchemaRoot VectorUnloader)
           org.roaringbitmap.longlong.Roaring64Bitmap))

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

(defn- ->row-counter ^core2.live_chunk.IRowCounter [^long block-idx]
  (let [block-row-counts (ArrayList. ^List (repeat block-idx 0))]
    (RowCounter. block-idx 0 block-row-counts 0)))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveColumnWatermark
  (columnType [])
  (^core2.ICursor #_<IIR> liveBlocks [])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveColumnTx
  (^core2.live_chunk.ILiveColumnWatermark openWatermark [^boolean retain])
  (^void writeRowId [^long rowId])
  (^boolean containsRowId [^long rowId])
  (^org.apache.arrow.vector.VectorSchemaRoot liveRoot [])
  (^org.apache.arrow.vector.VectorSchemaRoot txLiveRoot [])
  (^core2.vector.IVectorWriter contentWriter [])
  (^void commit [])
  (^void abort [])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveColumn
  (^core2.live_chunk.ILiveColumnTx startTx [])
  (^core2.live_chunk.ILiveColumnWatermark openWatermark [^boolean retain])
  (^boolean containsRowId [^long rowId])
  (^org.apache.arrow.vector.VectorSchemaRoot liveRoot [])
  (^void finishBlock [])
  (^java.util.concurrent.CompletableFuture finishChunk [])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveTableTx
  (^core2.live_chunk.ILiveColumnTx liveColumn [^String colName])
  (^Iterable #_#_<Map$Entry<String, VSR>> liveRootsWith [^long rowId])
  (^core2.live_chunk.ILiveTableWatermark openWatermark [^boolean retain])

  (^void commit [])
  (^void abort [])

  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveTableWatermark
  (^core2.live_chunk.ILiveColumnWatermark liveColumn [^String colName])
  (^core2.ICursor #_<IIR> liveBlocks [^Iterable colNames])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveTable
  (^core2.live_chunk.ILiveTableWatermark openWatermark [^boolean retain])
  (^core2.live_chunk.ILiveTableTx startTx [])
  (^void finishBlock [])
  (^java.util.concurrent.CompletableFuture finishChunk [])

  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveChunkWatermark
  (^core2.live_chunk.ILiveTableWatermark liveTable [^String table])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveChunkTx
  (^core2.live_chunk.ILiveTableTx liveTable [^String table])

  (^core2.live_chunk.ILiveChunkWatermark openWatermark [])
  ;; (live-cols->live-roots live-columns) (live-cols->tx-live-roots live-columns)

  (^long nextRowId [])
  (^void commit [])
  (^void abort [])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveChunk
  (^core2.live_chunk.ILiveTable liveTable [^String table])
  (^core2.live_chunk.ILiveChunkWatermark openWatermark [])

  (^core2.live_chunk.ILiveChunkTx startTx [])

  (^long chunkIdx [])
  (^boolean isChunkFull [])
  (^boolean isBlockFull [])

  (^void finishBlock [])
  (^void nextBlock [])
  (^java.util.concurrent.CompletableFuture finishChunk [^core2.api.TransactionInstant latest-completed-tx])
  (^void nextChunk [])
  (^void close []))

(deftype LiveColumnTx [^BigIntVector row-id-vec, ^ValueVector content-vec, ^Roaring64Bitmap row-id-bitmap
                       ^BigIntVector transient-row-id-vec, ^ValueVector transient-content-vec, ^Roaring64Bitmap transient-row-id-bitmap
                       ^IRowCounter row-counter]
  ILiveColumnTx
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

  (openWatermark [this retain?]
    (let [col-type (types/merge-col-types (types/field->col-type (.getField content-vec))
                                          (types/field->col-type (.getField transient-content-vec)))

          live-root (cond-> (.liveRoot this)
                      retain? util/slice-root)
          row-counts (.blockRowCounts row-counter)
          tx-live-root (.txLiveRoot this)]

      (reify ILiveColumnWatermark
        (columnType [_] col-type)

        (liveBlocks [_]
          (util/->concat-cursor (iv/->slice-cursor (iv/<-root tx-live-root) [(.getRowCount tx-live-root)])
                                (iv/->slice-cursor (iv/<-root live-root) row-counts)))

        AutoCloseable
        (close [_] (when retain? (.close live-root))))))

  (commit [_]
    (doto (vw/vec->writer content-vec)
      (vw/append-vec (iv/->direct-vec transient-content-vec)))

    (doto (vw/vec->writer row-id-vec)
      (vw/append-vec (iv/->direct-vec transient-row-id-vec)))

    (.addRows row-counter (.getValueCount transient-row-id-vec))

    (doto row-id-bitmap (.or transient-row-id-bitmap))

    (.clear transient-row-id-vec)
    (.clear transient-content-vec)
    (.clear transient-row-id-bitmap))

  (abort [_]
    (.clear transient-row-id-vec)
    (.clear transient-content-vec)
    (.clear transient-row-id-bitmap))

  AutoCloseable
  (close [_]
    (.close transient-row-id-vec)
    (.close transient-content-vec)))

(deftype LiveColumn [^BufferAllocator allocator, ^ObjectStore object-store
                     ^String table-name, ^String col-name, ^IColumnMetadataWriter col-metadata-writer
                     ^BigIntVector row-id-vec, ^ValueVector content-vec, ^Roaring64Bitmap row-id-bitmap
                     ^long chunk-idx, ^IRowCounter row-counter]
  ILiveColumn
  (startTx [_]
    (LiveColumnTx. row-id-vec content-vec row-id-bitmap
                   (.createVector types/row-id-field allocator)
                   (.createVector (types/->field col-name types/dense-union-type false) allocator)
                   (Roaring64Bitmap.) row-counter))

  (containsRowId [_ row-id] (.contains row-id-bitmap row-id))

  (liveRoot [_]
    (let [^Iterable vs [row-id-vec content-vec]]
      (VectorSchemaRoot. vs)))

  (openWatermark [_ retain?]
    (let [col-type (types/field->col-type (.getField content-vec))
          ^Iterable vs [row-id-vec content-vec]
          row-counts (.blockRowCounts row-counter)
          live-root (cond-> (VectorSchemaRoot. vs)
                      retain? util/slice-root)]

      (reify ILiveColumnWatermark
        (columnType [_] col-type)

        (liveBlocks [_]
          (iv/->slice-cursor (iv/<-root live-root) row-counts))

        AutoCloseable
        (close [_] (when retain? (.close live-root))))))

  (finishBlock [this]
    (.writeBlockMetadata col-metadata-writer
                         (iv/slice-rel (iv/<-root (.liveRoot this))
                                       (.chunkRowCount row-counter)
                                       (.blockRowCount row-counter))
                         (.blockIdx row-counter))
    (.nextBlock row-counter))

  (finishChunk [this]
    (let [live-root (.liveRoot this)]
      (.writeChunkMetadata col-metadata-writer (iv/<-root live-root))

      (.putObject object-store (meta/->chunk-obj-key chunk-idx table-name col-name)
                  (with-open [write-root (VectorSchemaRoot/create (.getSchema live-root) allocator)]
                    (let [loader (VectorLoader. write-root)
                          row-counts (.blockRowCounts row-counter)]
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
                          buf)))))))

  AutoCloseable
  (close [_]
    (util/try-close content-vec)
    (util/try-close row-id-vec)))

(defn- table-wm-live-blocks ^core2.ICursor [^Map wms, ^Iterable col-names]
  (let [slice-cursors (->> col-names
                           (into {} (keep (fn [^String col-name]
                                            (when-let [^ILiveColumnWatermark col-wm (.get wms col-name)]
                                              (MapEntry/create col-name (.liveBlocks col-wm)))))))]
    (when (= (set (keys slice-cursors)) (set col-names))
      (util/combine-col-cursors slice-cursors))))

(deftype LiveTableTx [^BufferAllocator allocator, ^ObjectStore object-store
                      ^String table-name, ^ITableMetadataWriter table-metadata-writer
                      ^Map live-columns, ^Map live-column-txs
                      ^long chunk-idx, ^long block-idx]
  ILiveTableTx
  (liveColumn [_ col-name]
    (letfn [(->live-col [col-name]
              (LiveColumn. allocator object-store table-name col-name (.columnMetadataWriter table-metadata-writer col-name)
                           (-> types/row-id-field (.createVector allocator))
                           (-> (types/->field col-name types/dense-union-type false) (.createVector allocator))
                           (Roaring64Bitmap.) chunk-idx (->row-counter block-idx)))

            (->live-col-tx [col-name]
              (-> ^ILiveColumn
                  (.computeIfAbsent live-columns col-name (util/->jfn ->live-col))
                  (.startTx)))]

      (.computeIfAbsent live-column-txs col-name
                        (util/->jfn ->live-col-tx))))

  (liveRootsWith [_ row-id]
    (concat (->> live-column-txs
                 (keep (fn [[col-name, ^ILiveColumnTx live-col]]
                         (when (.containsRowId live-col row-id)
                           (MapEntry/create col-name (.txLiveRoot live-col))))))
            (->> live-columns
                 (keep (fn [[col-name, ^ILiveColumn live-col]]
                         (when (.containsRowId live-col row-id)
                           (MapEntry/create col-name (.liveRoot live-col))))))))

  (openWatermark [_ retain?]
    (let [wms (HashMap.)]
      (try
        (doseq [[col-name ^ILiveColumnTx live-col] live-column-txs]
          (.put wms col-name (.openWatermark live-col retain?)))

        (doseq [[col-name ^ILiveColumn live-col] live-columns]
          (.computeIfAbsent wms col-name
                            (util/->jfn (fn [_] (.openWatermark live-col retain?)))))

        (reify ILiveTableWatermark
          (liveColumn [_ col-name] (.get wms col-name))
          (liveBlocks [_ col-names] (table-wm-live-blocks wms col-names))

          AutoCloseable
          (close [_] (run! util/try-close (.values wms))))

        (catch Throwable t
          (run! util/try-close (.values wms))
          (throw t)))))

  (commit [_]
    (doseq [^ILiveColumnTx live-col (.values live-column-txs)]
      (.commit live-col)))

  (abort [_]
    (doseq [^ILiveColumnTx live-col (.values live-column-txs)]
      (.abort live-col)))

  AutoCloseable
  (close [_]
    (run! util/try-close (.values live-column-txs))
    (.clear live-column-txs)))

(deftype LiveTable [^BufferAllocator allocator, ^ObjectStore object-store, ^IMetadataManager metadata-mgr
                    ^String table-name
                    ^Map live-columns
                    ^ITableMetadataWriter table-metadata-writer
                    ^long chunk-idx, ^IRowCounter chunk-row-counter]
  ILiveTable
  (startTx [_]
    (LiveTableTx. allocator object-store table-name table-metadata-writer live-columns (HashMap.) chunk-idx (.blockIdx chunk-row-counter)))

  (openWatermark [_ retain?]
    (let [wms (HashMap.)]
      (try
        (doseq [[col-name ^ILiveColumn live-col] live-columns]
          (.put wms col-name (.openWatermark live-col retain?)))

        (reify ILiveTableWatermark
          (liveColumn [_ col-name] (.get wms col-name))
          (liveBlocks [_ col-names] (table-wm-live-blocks wms col-names))

          AutoCloseable
          (close [_] (run! util/try-close (.values wms))))

        (catch Throwable t
          (run! util/try-close (.values wms))
          (throw t)))))

  (finishBlock [_]
    (doseq [^ILiveColumn live-col (.values live-columns)]
      (.finishBlock live-col)))

  (finishChunk [_]
    (let [!fut (-> (CompletableFuture/allOf
                    (->> (for [^ILiveColumn live-column (vals live-columns)]
                           (.finishChunk live-column))
                         (into-array CompletableFuture)))
                   (util/then-compose
                     (fn [_]
                       (.finishChunk table-metadata-writer))))

          live-roots (-> live-columns
                         (update-vals (fn [^ILiveColumn live-col]
                                        (.liveRoot live-col))))

          chunk-metadata (meta/live-roots->chunk-metadata table-name live-roots)]
      (-> !fut
          (util/then-apply (fn [_] chunk-metadata)))))

  AutoCloseable
  (close [_]
    (run! util/try-close (.values live-columns))
    (util/try-close table-metadata-writer)
    (.clear live-columns)))

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
                          (HashMap.) (.openTableMetadataWriter metadata-mgr table chunk-idx)
                          chunk-idx row-counter))

            (->live-table-tx [table-name]
              (-> ^ILiveTable
                  (.computeIfAbsent live-tables table-name
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

  (abort [_]
    (doseq [^ILiveTableTx live-table (.values live-table-txs)]
      (.abort live-table)))

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
                                            :tables (into {} (keep deref) futs)}))))))

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

(defmethod ig/prep-key :core2/live-chunk [_ opts]
  (merge {:rows-per-block 1024
          :rows-per-chunk 102400
          :allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :metadata-mgr (ig/ref ::meta/metadata-manager)}
         opts))

(defmethod ig/init-key :core2/live-chunk [_ {:keys [allocator object-store metadata-mgr ^long rows-per-block ^long rows-per-chunk]}]
  (let [chunk-idx (if-let [{:keys [^long latest-row-id]} (meta/latest-chunk-metadata metadata-mgr)]
                    (inc latest-row-id)
                    0)
        bloom-false-positive-probability (bloom/bloom-false-positive-probability? rows-per-chunk)]

    (when (> bloom-false-positive-probability 0.05)
      (log/warn "Bloom should be sized for large chunks:" rows-per-chunk
                "false positive probability:" bloom-false-positive-probability
                "bits:" bloom/bloom-bits
                "can be set via system property core2.bloom.bits"))

    (LiveChunk. allocator object-store metadata-mgr
                rows-per-block rows-per-chunk
                (HashMap.)
                chunk-idx (->row-counter 0))))

(defmethod ig/halt-key! :core2/live-chunk [_ live-chunk]
  (util/try-close live-chunk))
