(ns core2.scan
  (:require [core2.align :as align]
            [core2.metadata :as meta]
            [core2.util :as util]
            core2.tx
            [core2.types :as t]
            [core2.select :as sel])
  (:import core2.ICursor
           core2.buffer_pool.BufferPool
           core2.metadata.IMetadataManager
           core2.select.IVectorPredicate
           java.io.Closeable
           [java.util LinkedList List Queue Map]
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector VectorLoader VectorSchemaRoot]
           org.apache.arrow.vector.ipc.message.ArrowRecordBatch
           org.roaringbitmap.longlong.Roaring64Bitmap))

(definterface IChunk
  (^org.roaringbitmap.longlong.Roaring64Bitmap rowIdBitmap [])
  (^boolean loadNextBlock []))

(deftype Chunk [^IVectorPredicate vec-pred
                ^ArrowBuf buf,
                ^Queue blocks
                ^VectorSchemaRoot root
                ^VectorLoader loader
                ^:unsynchronized-mutable ^ArrowRecordBatch current-record-batch]
  IChunk
  (loadNextBlock [_]
    (when current-record-batch
      (.close current-record-batch))

    (if-let [block (.poll blocks)]
      (let [record-batch (util/->arrow-record-batch-view block buf)]
        (.load loader record-batch)
        true)
      false))

  (rowIdBitmap [_]
    (-> (when vec-pred
          (sel/select (.getVector root 1) vec-pred))
        (align/->row-id-bitmap (.getVector root t/row-id-field))))

  Closeable
  (close [_]
    (when current-record-batch
      (.close current-record-batch))
    (.close root)
    (.close buf)))

(defn- ->chunk [^BufferAllocator allocator ^IVectorPredicate vec-pred ^ArrowBuf buf]
  (let [footer (util/read-arrow-footer buf)
        root (VectorSchemaRoot/create (.getSchema footer) allocator)]
    (Chunk. vec-pred
            buf
            (LinkedList. (.getRecordBatches footer))
            root
            (VectorLoader. root)
            nil)))

(defn align-roots [chunks ^VectorSchemaRoot out-root]
  (let [row-id-bitmaps (for [^Chunk chunk chunks]
                         (.rowIdBitmap chunk))
        row-id-bitmap (reduce #(.and ^Roaring64Bitmap %1 %2)
                              (first row-id-bitmaps)
                              (rest row-id-bitmaps))]
    (align/align-vectors (map #(.root ^Chunk %) chunks) row-id-bitmap out-root)
    out-root))

(deftype ScanCursor [^BufferAllocator allocator
                     ^BufferPool buffer-pool
                     ^Queue #_<Long> chunk-idxs
                     ^List col-names
                     ^Map col-preds
                     ^:unsynchronized-mutable ^VectorSchemaRoot root
                     ^:unsynchronized-mutable ^List #_<Chunk> chunks]
  ICursor
  (tryAdvance [this c]
    (letfn [(next-block [chunks ^VectorSchemaRoot root]
              (if (every? true? (map #(.loadNextBlock ^Chunk %) chunks))
                (do
                  (align-roots chunks root)
                  (if (pos? (.getRowCount root))
                    (do
                      (.accept c root)
                      true)
                    (recur chunks root)))
                (do
                  (doseq [^Chunk chunk chunks]
                    (.close chunk))
                  (set! (.chunks this) nil)

                  (.close root)
                  (set! (.root this) nil)

                  false)))]

      (or (when chunks
            (next-block chunks root))

          (when-let [chunk-idx (.poll chunk-idxs)]
            (let [chunks (->> (for [col-name col-names]
                                (-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx col-name))
                                    (util/then-apply
                                      (fn [buf]
                                        (->chunk allocator (get col-preds col-name) buf)))))
                              vec
                              (mapv deref))]
              (set! (.chunks this) chunks)
              (set! (.root this)
                    (VectorSchemaRoot/create (align/roots->aligned-schema (map #(.root ^Chunk %) chunks)) allocator))
              (next-block chunks root)))

          false)))

  (close [_]
    (doseq [^Chunk chunk chunks]
      (.close chunk))
    (when root
      (.close root))))

(defn- ->scan-cursor [^BufferAllocator allocator
                      ^BufferPool buffer-pool
                      ^List col-names
                      ^Map col-preds
                      ^Queue chunk-idxs]
  (ScanCursor. allocator buffer-pool
               chunk-idxs col-names col-preds
               nil nil))

(definterface IScanFactory
  (^core2.ICursor scanBlocks [^java.util.List #_<String> colNames,
                              metadataPred
                              ^java.util.Map #_#_<String, IVectorPredicate> colPreds]))

(deftype ScanFactory [^BufferAllocator allocator
                      ^IMetadataManager metadata-mgr
                      ^BufferPool buffer-pool]
  IScanFactory
  (scanBlocks [_ col-names metadata-pred col-preds]
    (let [chunk-idxs (LinkedList. (meta/matching-chunks metadata-mgr metadata-pred))]
      (->scan-cursor allocator buffer-pool col-names col-preds chunk-idxs))))
