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
                     ^:unsynchronized-mutable ^VectorSchemaRoot out-root
                     ^:unsynchronized-mutable ^Map #_#_<String, IChunkCursor> chunks
                     ^:unsynchronized-mutable ^boolean live-chunk-done?]

  ICursor
  (tryAdvance [this c]
    (letfn [(next-block [chunks ^VectorSchemaRoot out-root]
              (loop []
                (if-let [in-roots (next-roots col-names chunks)]
                  (do
                    (align-roots col-names col-preds in-roots out-root)
                    (if (pos? (.getRowCount out-root))
                      (do
                        (.accept c out-root)
                        true)
                      (recur)))

                  (do
                    (doseq [^ICursor chunk (vals chunks)]
                      (.close chunk))
                    (set! (.chunks this) nil)

                    (.close out-root)
                    (set! (.out-root this) nil)

                    false))))

            (live-chunk []
              (let [chunks (idx/->live-slices watermark col-names)
                    out-root (VectorSchemaRoot/create (align/align-schemas (for [^IChunkCursor chunk (vals chunks)]
                                                                             (.getSchema chunk)))
                                                      allocator)]
                (set! (.chunks this) chunks)
                (set! (.out-root this) out-root)

                (next-block chunks out-root)))

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
                        out-root (VectorSchemaRoot/create (align/align-schemas (for [^IChunkCursor chunk (vals chunks)]
                                                                                 (.getSchema chunk)))
                                                          allocator)]
                    (set! (.chunks this) chunks)
                    (set! (.out-root this) out-root)

                    (or (next-block chunks out-root)
                        (recur))))))]

      (or (when chunks
            (next-block chunks out-root))

          (next-chunk)

          (when-not live-chunk-done?
            (set! (.live-chunk-done? this) true)
            (live-chunk))

          false)))

  (close [_]
    (doseq [^ICursor chunk (vals chunks)]
      (.close chunk))
    (when out-root
      (.close out-root))))

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

#_
(definterface ISelectFactory
  (^core2.ICursor selectBlocks [^core2.ICursor inCursor
                                ^core2.IVectorRootPredicate pred]))

#_
(definterface IProjectFactory
  (^core2.ICursor projectBlocks [^core2.ICursor inCursor
                                 ^java.util.List #_#_<Pair<String, ColExpr>> colSpecs]))

#_
(definterface IRenameFactory
  (^core2.ICursor projectBlocks [^core2.ICursor inCursor
                                 ^java.util.Map #_#_<String, String> renameSpecs]))

#_
(definterface IEquiJoinFactory
  (^core2.ICursor joinBlocks [^core2.ICursor leftCursor
                              ^String leftColName
                              ^core2.ICursor rightCursor
                              ^String rightColName]))

#_
(definterface ICrossJoinFactory
  (^core2.ICursor joinBlocks [^core2.ICursor leftCursor
                              ^core2.ICursor rightCursor]))

#_
(definterface ISliceFactory
  (^core2.ICursor sliceBlocks [^core2.ICursor inCursor
                               ^Long offset
                               ^Long limit]))

#_
(definterface IOrderByFactory
  (^core2.ICursor orderBlocks [^core2.ICursor inCursor
                               ^java.util.List #_#_<Pair<String, Direction>> orderSpec]))

#_
(definterface IGroupByFactory
  (^core2.ICursor orderBlocks [^core2.ICursor inCursor
                               ^java.util.List #_#_#_<Pair<String, Or<String, AggregateExpr>>> aggregateSpecs]))
