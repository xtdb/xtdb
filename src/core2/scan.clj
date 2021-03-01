(ns core2.scan
  (:require core2.buffer-pool
            [core2.metadata :as meta]
            [core2.types :as t]
            [core2.util :as util]
            [core2.align :as align])
  (:import core2.buffer_pool.BufferPool
           core2.metadata.IMetadataManager
           java.io.Closeable
           [java.util LinkedList List Queue]
           java.util.function.IntConsumer
           java.util.stream.IntStream
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector.ipc.message ArrowRecordBatch]
           [org.apache.arrow.vector BigIntVector FieldVector VectorLoader VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           org.apache.arrow.vector.types.pojo.Schema
           org.roaringbitmap.longlong.Roaring64Bitmap
           org.roaringbitmap.RoaringBitmap))

;; TODO make a Java interface of this
(gen-interface
 :name core2.ICursor
 :extends [java.lang.AutoCloseable]
 :methods [[tryAdvance [java.util.function.Consumer] boolean]])

(import core2.ICursor)

(definterface IChunk
  (^boolean loadNextBlock []))

(deftype Chunk [^ArrowBuf buf,
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

  Closeable
  (close [_]
    (when current-record-batch
      (.close current-record-batch))
    (.close root)
    (.close buf)))

(defn- ->chunk [^BufferAllocator allocator ^ArrowBuf buf]
  (let [footer (util/read-arrow-footer buf)
        root (VectorSchemaRoot/create (.getSchema footer) allocator)]
    (Chunk. buf
            (LinkedList. (.getRecordBatches footer))
            root
            (VectorLoader. root)
            nil)))

(defn align-roots [in-roots ^VectorSchemaRoot out-root]
  (let [row-id-bitmaps (for [^VectorSchemaRoot in-root in-roots]
                         (align/->row-id-bitmap (.getVector in-root 0)))
        row-id-bitmap (reduce #(.and ^Roaring64Bitmap %1 %2)
                              (first row-id-bitmaps)
                              (rest row-id-bitmaps))]
    (align/align-vectors in-roots row-id-bitmap out-root)
    out-root))

(deftype ScanCursor [^BufferAllocator allocator
                     ^BufferPool buffer-pool
                     ^Queue #_<Long> chunk-idxs
                     ^List col-names
                     ^:unsynchronized-mutable ^VectorSchemaRoot root
                     ^:unsynchronized-mutable ^List #_<Chunk> chunks]
  ICursor
  (tryAdvance [this c]
    (letfn [(next-block [chunks ^VectorSchemaRoot root]
              (if (every? true? (map #(.loadNextBlock ^Chunk %) chunks))
                (do
                  (align-roots (map #(.root ^Chunk %) chunks) root)
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
                                (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx col-name)))
                              vec
                              (mapv (comp #(->chunk allocator %) deref)))]
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
                      ^Queue chunk-idxs]
  (ScanCursor. allocator buffer-pool
               chunk-idxs col-names
               nil nil))

(definterface IScanFactory
  (^core2.ICursor scanBlocks [^java.util.List #_<String> colNames]))

(deftype ScanFactory [^BufferAllocator allocator
                      ^IMetadataManager metadata-mgr
                      ^BufferPool buffer-pool]
  IScanFactory
  (scanBlocks [_ col-names]
    (->scan-cursor allocator buffer-pool col-names (LinkedList. (.knownChunks metadata-mgr)))))
