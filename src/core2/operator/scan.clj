(ns core2.operator.scan
  (:require [core2.align :as align]
            [core2.indexer :as idx]
            [core2.metadata :as meta]
            [core2.temporal :as temporal]
            core2.tx
            [core2.types :as t]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.buffer_pool.IBufferPool
           core2.ICursor
           core2.metadata.IMetadataManager
           core2.select.IVectorSelector
           [core2.temporal ITemporalManager TemporalRoots]
           core2.tx.Watermark
           core2.util.IChunkCursor
           [java.util HashMap LinkedList List Map Queue]
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]
           org.roaringbitmap.longlong.Roaring64Bitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn- next-roots [col-names chunks]
  (when (= (count col-names) (count chunks))
    (let [in-roots (HashMap.)]
      (when (every? true? (for [col-name col-names
                                :let [^ICursor chunk (get chunks col-name)]
                                :when chunk]
                            (.tryAdvance chunk
                                         (reify Consumer
                                           (accept [_ root]
                                             (.put in-roots col-name root))))))
        in-roots))))

(defn- roaring64-and
  (^org.roaringbitmap.longlong.Roaring64Bitmap [] (Roaring64Bitmap.))
  (^org.roaringbitmap.longlong.Roaring64Bitmap [^Roaring64Bitmap x] x)
  (^org.roaringbitmap.longlong.Roaring64Bitmap [^Roaring64Bitmap x ^Roaring64Bitmap y]
   (doto x
     (.and y))))

(defn- adjust-temporal-min-range-to-row-id-range ^longs [^longs temporal-min-range ^Roaring64Bitmap row-id-bitmap]
  (let [temporal-min-range (or (temporal/->copy-range temporal-min-range) (temporal/->min-range))]
    (if (.isEmpty row-id-bitmap)
      temporal-min-range
      (let [min-row-id (.select row-id-bitmap 0)]
        (doto temporal-min-range
          (aset temporal/row-id-idx
                (max min-row-id (aget temporal-min-range temporal/row-id-idx))))))))

(defn- adjust-temporal-max-range-to-row-id-range ^longs [^longs temporal-max-range ^Roaring64Bitmap row-id-bitmap]
  (let [temporal-max-range (or (temporal/->copy-range temporal-max-range) (temporal/->max-range))]
    (if (.isEmpty row-id-bitmap)
      temporal-max-range
      (let [max-row-id (.select row-id-bitmap (dec (.getLongCardinality row-id-bitmap)))]
        (doto temporal-max-range
          (aset temporal/row-id-idx
                (min max-row-id (aget temporal-max-range temporal/row-id-idx))))))))

(defn- ->row-id->repeat-count ^java.util.Map [^TemporalRoots temporal-roots ^Roaring64Bitmap row-id-bitmap]
  (when temporal-roots
    (when-let [^VectorSchemaRoot root (first (.values ^Map (.roots temporal-roots)))]
      (let [res (HashMap.)
            ^BigIntVector row-id-vec (.getVector root 0)]
        (dotimes [idx (.getValueCount row-id-vec)]
          (when (.contains row-id-bitmap (.get row-id-vec idx))
            (.put res idx (inc ^long (.getOrDefault res idx 0)))))
        res))))

(defn- align-roots [^ITemporalManager temporal-manager ^Watermark watermark ^List col-names ^Map col-preds ^longs temporal-min-range ^longs temporal-max-range ^Map in-roots ^VectorSchemaRoot out-root]
  (let [row-id-bitmaps (for [^String col-name col-names
                             :when (not (temporal/temporal-column? col-name))
                             :let [^IVectorSelector vec-pred (.get col-preds col-name)
                                   ^VectorSchemaRoot in-root (.get in-roots col-name)]]
                         (align/->row-id-bitmap (when vec-pred
                                                  (.select vec-pred (.getVector in-root col-name)))
                                                (.getVector in-root t/row-id-field)))
        row-id-bitmap (reduce roaring64-and row-id-bitmaps)
        temporal-min-range (adjust-temporal-min-range-to-row-id-range temporal-min-range row-id-bitmap)
        temporal-max-range (adjust-temporal-max-range-to-row-id-range temporal-max-range row-id-bitmap)
        temporal-roots (.createTemporalRoots temporal-manager watermark (filterv temporal/temporal-column? col-names)
                                             temporal-min-range
                                             temporal-max-range
                                             row-id-bitmap)]
    (try
      (let [row-id-bitmap (if temporal-roots
                            (.row-id-bitmap temporal-roots)
                            row-id-bitmap)
            roots (for [col-name col-names]
                    (if (temporal/temporal-column? col-name)
                      (.get ^Map (.roots temporal-roots) col-name)
                      (.get in-roots col-name)))
            temporal-row-id-bitmaps (for [col-name col-names
                                          :when (temporal/temporal-column? col-name)]
                                      (.select ^IVectorSelector (.get col-preds col-name)
                                               (.get ^Map (.roots temporal-roots) col-name)))
            row-id-bitmap (reduce roaring64-and
                                  row-id-bitmap
                                  temporal-row-id-bitmaps)
            row-id->repeat-count (->row-id->repeat-count temporal-roots row-id-bitmap)]
        (align/align-vectors roots row-id-bitmap row-id->repeat-count out-root))
      (finally
        (util/try-close temporal-roots)))
    out-root))

(deftype ScanCursor [^BufferAllocator allocator
                     ^IBufferPool buffer-pool
                     ^ITemporalManager temporal-manager
                     ^Watermark watermark
                     ^Queue #_<Long> chunk-idxs
                     ^List col-names
                     ^Map col-preds
                     ^longs temporal-min-range
                     ^longs temporal-max-range
                     ^:unsynchronized-mutable ^VectorSchemaRoot out-root
                     ^:unsynchronized-mutable ^Map #_#_<String, IChunkCursor> chunks
                     ^:unsynchronized-mutable ^boolean live-chunk-done?]

  ICursor
  (tryAdvance [this c]
    (let [real-col-names (remove temporal/temporal-column? col-names)]
      (letfn [(create-out-root [^Map chunks]
                (when (= (count chunks) (count real-col-names))
                  (VectorSchemaRoot/create (align/align-schemas (for [col-name col-names]
                                                                  (if (temporal/temporal-column? col-name)
                                                                    (temporal/->temporal-root-schema col-name)
                                                                    (.getSchema ^IChunkCursor (.get chunks col-name)))))
                                           allocator)))

              (next-block [chunks ^VectorSchemaRoot out-root]
                (loop []
                  (if-let [in-roots (next-roots real-col-names chunks)]
                    (do
                      (align-roots temporal-manager watermark col-names col-preds temporal-min-range temporal-max-range in-roots out-root)
                      (if (pos? (.getRowCount out-root))
                        (do
                          (.accept c out-root)
                          true)
                        (recur)))

                    (do
                      (doseq [^ICursor chunk (vals chunks)]
                        (.close chunk))
                      (set! (.chunks this) nil)

                      (util/try-close out-root)
                      (set! (.out-root this) nil)

                      false))))

              (live-chunk []
                (let [chunks (idx/->live-slices watermark real-col-names)
                      out-root (create-out-root chunks)]
                  (set! (.chunks this) chunks)
                  (set! (.out-root this) out-root)

                  (next-block chunks out-root)))

              (next-chunk []
                (loop []
                  (when-let [chunk-idx (.poll chunk-idxs)]
                    (let [chunks (->> (for [col-name real-col-names]
                                        (-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx col-name))
                                            (util/then-apply
                                              (fn [buf]
                                                (when buf
                                                  (MapEntry/create col-name (util/->chunks buf allocator)))))))
                                      (remove nil?)
                                      vec
                                      (into {} (map deref)))
                          out-root (create-out-root chunks)]
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

            false))))

  (close [_]
    (doseq [^ICursor chunk (vals chunks)]
      (.close chunk))
    (when out-root
      (.close out-root))))

(defn ->scan-cursor [^BufferAllocator allocator
                     ^IMetadataManager metadata-mgr
                     ^ITemporalManager temporal-manager
                     ^IBufferPool buffer-pool
                     ^Watermark watermark
                     ^List col-names
                     metadata-pred ;; TODO derive this from col-preds
                     ^Map col-preds
                     ^longs temporal-min-range
                     ^longs temporal-max-range]
  (let [chunk-idxs (LinkedList. (or (meta/matching-chunks metadata-mgr watermark metadata-pred) []))]
    (ScanCursor. allocator buffer-pool temporal-manager watermark
                 chunk-idxs col-names col-preds temporal-min-range temporal-max-range
                 nil nil false)))
