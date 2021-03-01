(ns core2.scan
  (:require [core2.align :as align]
            [core2.indexer :as idx]
            [core2.metadata :as meta]
            [core2.select :as sel]
            core2.tx
            [core2.types :as t]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.buffer_pool.BufferPool
           core2.ICursor
           core2.metadata.IMetadataManager
           core2.tx.Watermark
           core2.util.IChunkCursor
           [java.util LinkedHashMap LinkedList List Map Queue]
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.VectorSchemaRoot
           org.roaringbitmap.longlong.Roaring64Bitmap))

(defn- next-roots [col-names chunks]
  (when (= (count col-names) (count chunks))
    (let [in-roots (LinkedHashMap.)]
      (when (every? true? (for [col-name col-names
                                :let [^ICursor chunk (get chunks col-name)]
                                :when chunk]
                            (.tryAdvance chunk
                                         (reify Consumer
                                           (accept [_ root]
                                             (.put in-roots col-name root))))))
        in-roots))))

(defn- ->row-id-bitmap [^VectorSchemaRoot root vec-pred]
  (-> (when vec-pred
        (sel/select (.getVector root 1) vec-pred))
      (align/->row-id-bitmap (.getVector root t/row-id-field))))

(defn- align-roots [^List col-names ^Map col-preds ^Map in-roots ^VectorSchemaRoot out-root]
  (let [row-id-bitmaps (for [col-name col-names]
                         (->row-id-bitmap (get in-roots col-name) (get col-preds col-name)))
        row-id-bitmap (reduce #(.and ^Roaring64Bitmap %1 %2)
                              (first row-id-bitmaps)
                              (rest row-id-bitmaps))]
    (align/align-vectors (vals in-roots) row-id-bitmap out-root)
    out-root))

(deftype ScanCursor [^BufferAllocator allocator
                     ^BufferPool buffer-pool
                     ^Watermark watermark
                     ^Queue #_<Long> chunk-idxs
                     ^List col-names
                     ^Map col-preds
                     ^:unsynchronized-mutable ^VectorSchemaRoot root
                     ^:unsynchronized-mutable ^Map #_#_<String, IChunkCursor> chunks
                     ^:unsynchronized-mutable ^boolean live-chunk-done?]
  ICursor
  (tryAdvance [this c]
    (letfn [(next-block [chunks ^VectorSchemaRoot root]
              (loop []
                (if-let [in-roots (next-roots col-names chunks)]
                  (do
                    (align-roots col-names col-preds in-roots root)
                    (if (pos? (.getRowCount root))
                      (do
                        (.accept c root)
                        true)
                      (recur)))

                  (do
                    (doseq [^ICursor chunk (vals chunks)]
                      (.close chunk))
                    (set! (.chunks this) nil)

                    (.close root)
                    (set! (.root this) nil)

                    false))))

            (next-chunk []
              (loop []
                (when-let [chunk-idx (.poll chunk-idxs)]
                  (let [chunks (->> (for [col-name col-names]
                                      (-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx col-name))
                                          (util/then-apply
                                            (fn [buf]
                                              (MapEntry/create col-name (util/->chunks buf allocator))))))
                                    vec
                                    (into {} (map deref)))
                        root (VectorSchemaRoot/create (align/align-schemas (for [^IChunkCursor chunk (vals chunks)]
                                                                             (.getSchema chunk)))
                                                      allocator)]
                    (set! (.chunks this) chunks)
                    (set! (.root this) root)

                    (or (next-block chunks root)
                        (recur))))))

            (live-chunk []
              (let [chunks (idx/->live-slices watermark col-names)
                    root (VectorSchemaRoot/create (align/align-schemas (for [^IChunkCursor chunk (vals chunks)]
                                                                         (.getSchema chunk)))
                                                  allocator)]
                (set! (.chunks this) chunks)
                (set! (.root this) root)

                (next-block chunks root)))]

      (or (when-not live-chunk-done?
            (set! (.live-chunk-done? this) true)
            (live-chunk))

          (when chunks
            (next-block chunks root))

          (next-chunk)

          false)))

  (close [_]
    (doseq [^ICursor chunk (vals chunks)]
      (.close chunk))
    (when root
      (.close root))))

(defn- ->scan-cursor [^BufferAllocator allocator
                      ^BufferPool buffer-pool
                      ^Watermark watermark
                      ^List col-names
                      ^Map col-preds
                      ^Queue chunk-idxs]
  (ScanCursor. allocator buffer-pool watermark
               chunk-idxs col-names col-preds
               nil nil false))

(definterface IScanFactory
  (^core2.ICursor scanBlocks [^core2.tx.Watermark watermark
                              ^java.util.List #_<String> colNames,
                              metadataPred
                              ^java.util.Map #_#_<String, IVectorPredicate> colPreds]))

(deftype ScanFactory [^BufferAllocator allocator
                      ^IMetadataManager metadata-mgr
                      ^BufferPool buffer-pool]
  IScanFactory
  (scanBlocks [_ watermark col-names metadata-pred col-preds]
    (let [chunk-idxs (LinkedList. (meta/matching-chunks metadata-mgr watermark metadata-pred))]
      (->scan-cursor allocator buffer-pool watermark col-names col-preds chunk-idxs))))
