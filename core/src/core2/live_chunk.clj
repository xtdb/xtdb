(ns core2.live-chunk
  (:require [clojure.tools.logging :as log]
            [core2.blocks :as blocks]
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
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           java.lang.AutoCloseable
           (java.util HashMap Map)
           (java.util.concurrent CompletableFuture)
           java.util.function.Consumer
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector ValueVector VectorLoader VectorSchemaRoot VectorUnloader)
           org.roaringbitmap.longlong.Roaring64Bitmap))

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
  (^java.util.concurrent.CompletableFuture finishChunk [^long chunk-idx])
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
  (^java.util.concurrent.CompletableFuture #_<chunk-metadata> finishChunk [^long chunk-idx])

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
  (^long commit [] "returns: count of rows in this tx")
  (^void abort [])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILiveChunk
  (^core2.live_chunk.ILiveTable liveTable [^String table])
  (^core2.live_chunk.ILiveChunkWatermark openWatermark [^long chunk-idx])

  (^core2.live_chunk.ILiveChunkTx startTx [^long chunkIdx, ^long startRowId])

  (^java.util.concurrent.CompletableFuture #_<Iterable<chunk-metadata>> finishChunk [^long chunk-idx])
  (^void clear [])
  (^void close []))

(deftype LiveColumnTx [^BigIntVector row-id-vec, ^ValueVector content-vec, ^Roaring64Bitmap row-id-bitmap
                       ^BigIntVector transient-row-id-vec, ^ValueVector transient-content-vec, ^Roaring64Bitmap transient-row-id-bitmap
                       ^long max-rows-per-block, ^long chunk-idx]
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
          tx-live-root (.txLiveRoot this)]

      (reify ILiveColumnWatermark
        (columnType [_] col-type)

        (liveBlocks [_]
          (util/->concat-cursor (iv/->slice-cursor (iv/<-root tx-live-root) [(.getRowCount tx-live-root)])
                                (iv/->slice-cursor (iv/<-root live-root) (blocks/row-id-aligned-blocks live-root chunk-idx max-rows-per-block))))

        AutoCloseable
        (close [_] (when retain? (.close live-root))))))

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

  AutoCloseable
  (close [_]
    (.close transient-row-id-vec)
    (.close transient-content-vec)))

(deftype LiveColumn [^BufferAllocator allocator, ^ObjectStore object-store
                     ^String table-name, ^String col-name
                     ^BigIntVector row-id-vec, ^ValueVector content-vec, ^Roaring64Bitmap row-id-bitmap
                     ^long max-rows-per-block, ^long chunk-idx]
  ILiveColumn
  (startTx [_]
    (LiveColumnTx. row-id-vec content-vec row-id-bitmap
                   (.createVector types/row-id-field allocator)
                   (.createVector (types/->field col-name types/dense-union-type false) allocator)
                   (Roaring64Bitmap.)
                   max-rows-per-block chunk-idx))

  (containsRowId [_ row-id] (.contains row-id-bitmap row-id))

  (liveRoot [_]
    (let [^Iterable vs [row-id-vec content-vec]]
      (VectorSchemaRoot. vs)))

  (openWatermark [_ retain?]
    (let [col-type (types/field->col-type (.getField content-vec))
          ^Iterable vs [row-id-vec content-vec]
          live-root (cond-> (VectorSchemaRoot. vs)
                      retain? util/slice-root)]

      (reify ILiveColumnWatermark
        (columnType [_] col-type)

        (liveBlocks [_]
          (iv/->slice-cursor (iv/<-root live-root)
                             (blocks/row-id-aligned-blocks live-root chunk-idx max-rows-per-block)))

        AutoCloseable
        (close [_] (when retain? (.close live-root))))))

  (finishChunk [_ chunk-idx]
    (.putObject object-store (meta/->chunk-obj-key chunk-idx table-name col-name)
                (let [^Iterable vs [row-id-vec content-vec]
                      ^VectorSchemaRoot live-root (VectorSchemaRoot. vs)]
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
                                                     (write-batch!)))))))))))))

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
                      ^String table-name,
                      ^Map live-columns, ^Map live-column-txs
                      ^long max-rows-per-block, ^long chunk-idx]
  ILiveTableTx
  (liveColumn [_ col-name]
    (letfn [(->live-col [col-name]
              (LiveColumn. allocator object-store table-name col-name
                           (-> types/row-id-field (.createVector allocator))
                           (-> (types/->field col-name types/dense-union-type false) (.createVector allocator))
                           (Roaring64Bitmap.) max-rows-per-block chunk-idx))

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
                    ^long max-rows-per-block, ^long chunk-idx]
  ILiveTable
  (startTx [_]
    (LiveTableTx. allocator object-store table-name live-columns (HashMap.) max-rows-per-block chunk-idx))

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

  (finishChunk [_ chunk-idx]
    (let [live-roots (-> live-columns
                         (update-vals (fn [^ILiveColumn live-col]
                                        (.liveRoot live-col))))
          !fut (CompletableFuture/allOf
                (->> (cons (.finishTableChunk metadata-mgr chunk-idx table-name live-roots)
                           (for [^ILiveColumn live-column (vals live-columns)]
                             (.finishChunk live-column chunk-idx)))
                     (into-array CompletableFuture)))
          chunk-metadata (meta/live-roots->chunk-metadata table-name live-roots)]
      (-> !fut
          (util/then-apply (fn [_] chunk-metadata)))))

  AutoCloseable
  (close [_]
    (run! util/try-close (.values live-columns))
    (.clear live-columns)))

(deftype LiveChunkTx [^BufferAllocator allocator, ^ObjectStore object-store, ^IMetadataManager metadata-mgr
                      ^Map live-tables, ^Map live-table-txs,
                      ^long max-rows-per-block
                      ^long chunk-idx, ^long start-row-id
                      ^:unsynchronized-mutable ^long tx-row-count]
  ILiveChunkTx
  (liveTable [_ table-name]
    (letfn [(->live-table [table]
              (LiveTable. allocator object-store metadata-mgr
                          table (HashMap.)
                          max-rows-per-block chunk-idx))

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
    (let [tx-row-count tx-row-count]
      (set! (.tx-row-count this) (inc tx-row-count))
      (+ start-row-id tx-row-count)))

  (commit [_]
    (doseq [^ILiveTableTx live-table (.values live-table-txs)]
      (.commit live-table))
    tx-row-count)

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
                    ^Map live-tables
                    ^long max-rows-per-block]
  ILiveChunk
  (liveTable [_ table] (.get live-tables table))

  (startTx [_ chunk-idx start-row-id]
    (LiveChunkTx. allocator object-store metadata-mgr live-tables (HashMap.) max-rows-per-block chunk-idx start-row-id 0))

  (openWatermark [_ _chunk-idx]
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

  (finishChunk [_ chunk-idx]
    (let [futs (for [^ILiveTable live-table (vals live-tables)]
                 (.finishChunk live-table chunk-idx))]

      (-> (CompletableFuture/allOf (into-array CompletableFuture futs))
          (util/then-apply (fn [_]
                             {:tables (into {} (keep deref) futs)})))))

  (clear [_]
    (run! util/try-close (.values live-tables))
    (.clear live-tables))

  AutoCloseable
  (close [this] (.clear this)))

(defmethod ig/prep-key :core2/live-chunk [_ opts]
  (merge {:allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :metadata-mgr (ig/ref ::meta/metadata-manager)
          :row-counts (ig/ref :core2/row-counts)}
         opts))

(defmethod ig/init-key :core2/live-chunk [_ {:keys [allocator object-store metadata-mgr]
                                             {:keys [max-rows-per-block]} :row-counts}]
  (LiveChunk. allocator object-store metadata-mgr
              (HashMap.) max-rows-per-block))

(defmethod ig/halt-key! :core2/live-chunk [_ live-chunk]
  (util/try-close live-chunk))
