(ns core2.operator.scan
  (:require [core2.align :as align]
            [core2.bloom :as bloom]
            [core2.indexer :as idx]
            [core2.metadata :as meta]
            core2.operator.select
            [core2.relation :as rel]
            [core2.temporal :as temporal]
            core2.tx
            [core2.types :as t]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.buffer_pool.IBufferPool
           core2.ICursor
           core2.metadata.IMetadataManager
           core2.operator.select.IColumnSelector
           [core2.temporal ITemporalManager TemporalRoots]
           core2.tx.Watermark
           [java.util HashMap LinkedList List Map Queue]
           [java.util.function BiFunction Consumer]
           [org.apache.arrow.vector BigIntVector VarBinaryVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex ListVector StructVector]
           [org.roaringbitmap IntConsumer RoaringBitmap]
           org.roaringbitmap.buffer.MutableRoaringBitmap
           org.roaringbitmap.longlong.Roaring64Bitmap))

(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic *column->pushdown-bloom* {})

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

(defn- ->atemporal-row-id-bitmap [^List col-names ^Map col-preds ^Map in-roots]
  (->> (for [^String col-name col-names
                     :when (not (temporal/temporal-column? col-name))
                     :let [^IColumnSelector col-pred (.get col-preds col-name)
                           ^VectorSchemaRoot in-root (.get in-roots col-name)]]
                 (align/->row-id-bitmap (when col-pred
                                          (.select col-pred (rel/<-vector (.getVector in-root col-name))))
                                        (.getVector in-root t/row-id-field)))
       (reduce roaring64-and)))

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
          (let [row-id (.get row-id-vec idx)]
            (when (.contains row-id-bitmap row-id)
              (.compute res row-id (reify BiFunction
                                     (apply [_ k v]
                                       (if v
                                         (inc (long v))
                                         1)))))))
        res))))

(defn- ->temporal-roots ^core2.temporal.TemporalRoots [^ITemporalManager temporal-manager ^Watermark watermark ^List col-names ^longs temporal-min-range ^longs temporal-max-range atemporal-row-id-bitmap]
  (let [temporal-min-range (adjust-temporal-min-range-to-row-id-range temporal-min-range atemporal-row-id-bitmap)
        temporal-max-range (adjust-temporal-max-range-to-row-id-range temporal-max-range atemporal-row-id-bitmap)]
    (.createTemporalRoots temporal-manager watermark (filterv temporal/temporal-column? col-names)
                          temporal-min-range
                          temporal-max-range
                          atemporal-row-id-bitmap)))

(defn- ->temporal-row-id-bitmap [col-names ^Map col-preds ^TemporalRoots temporal-roots atemporal-row-id-bitmap]
  (reduce roaring64-and
          (if temporal-roots
            (.row-id-bitmap temporal-roots)
            atemporal-row-id-bitmap)
          (for [^String col-name col-names
                :when (temporal/temporal-column? col-name)
                :let [^IColumnSelector col-pred (.get col-preds col-name)
                      ^VectorSchemaRoot in-root (.get ^Map (.roots temporal-roots) col-name)]]
            (align/->row-id-bitmap (when col-pred
                                     (.select col-pred (rel/<-vector (.getVector in-root col-name))))
                                   (.getVector in-root t/row-id-field)))))

(defn- align-roots ^core2.relation.IReadRelation [^List col-names ^Map in-roots ^TemporalRoots temporal-roots row-id-bitmap]
  (let [roots (for [col-name col-names]
                (if (temporal/temporal-column? col-name)
                  (.get ^Map (.roots temporal-roots) col-name)
                  (.get in-roots col-name)))
        row-id->repeat-count (->row-id->repeat-count temporal-roots row-id-bitmap)]
    (align/align-vectors roots row-id-bitmap row-id->repeat-count)))

(defn- filter-pushdown-bloom-block-idxs [^IMetadataManager metadata-manager ^long chunk-idx ^String col-name ^RoaringBitmap block-idxs]
  (if-let [^MutableRoaringBitmap pushdown-bloom (get *column->pushdown-bloom* col-name)]
    @(meta/with-metadata metadata-manager chunk-idx
       (fn [_chunk-idx ^VectorSchemaRoot metadata-root]
         (let [metadata-idxs (meta/->metadata-idxs metadata-root)
               ^VarBinaryVector bloom-vec (.getVector metadata-root "bloom")]
           (when (MutableRoaringBitmap/intersects pushdown-bloom
                                                  (bloom/bloom->bitmap bloom-vec (.columnIndex metadata-idxs col-name)))
             (let [filtered-block-idxs (RoaringBitmap.)
                   ^ListVector blocks-vec (.getVector metadata-root "blocks")
                   ^StructVector blocks-data-vec (.getDataVector blocks-vec)
                   ^VarBinaryVector blocks-bloom-vec (.getChild blocks-data-vec "bloom")]
               (.forEach block-idxs
                         (reify IntConsumer
                           (accept [_ block-idx]
                             (when-let [bloom-vec-idx (.blockIndex metadata-idxs col-name block-idx)]
                               (when (and (not (.isNull blocks-data-vec bloom-vec-idx))
                                          (MutableRoaringBitmap/intersects pushdown-bloom
                                                                           (bloom/bloom->bitmap blocks-bloom-vec bloom-vec-idx)))
                                 (.add filtered-block-idxs block-idx))))))

               (when-not (.isEmpty filtered-block-idxs)
                 filtered-block-idxs))))))
    block-idxs))

(deftype ScanCursor [^IBufferPool buffer-pool
                     ^ITemporalManager temporal-manager
                     ^IMetadataManager metadata-manager
                     ^Watermark watermark
                     ^Queue #_<ChunkMatch> matching-chunks
                     ^List col-names
                     ^Map col-preds
                     ^longs temporal-min-range
                     ^longs temporal-max-range
                     ^:unsynchronized-mutable ^Map #_#_<String, ICursor> chunks
                     ^:unsynchronized-mutable ^boolean live-chunk-done?]
  ICursor
  (tryAdvance [this c]
    (let [real-col-names (remove temporal/temporal-column? col-names)]
      (letfn [(next-block [chunks]
                (loop []
                  (if-let [in-roots (next-roots real-col-names chunks)]
                    (let [atemporal-row-id-bitmap (->atemporal-row-id-bitmap col-names col-preds in-roots)
                          temporal-roots (->temporal-roots temporal-manager watermark col-names temporal-min-range temporal-max-range atemporal-row-id-bitmap)]
                      (or (try
                            (let [row-id-bitmap (->temporal-row-id-bitmap col-names col-preds temporal-roots atemporal-row-id-bitmap)
                                  read-rel (align-roots col-names in-roots temporal-roots row-id-bitmap)]
                              (if (and read-rel (pos? (.rowCount read-rel)))
                                (do
                                  (.accept c read-rel)
                                  true)
                                false))
                            (finally
                              (util/try-close temporal-roots)))
                          (recur)))

                    (do
                      (doseq [^ICursor chunk (vals chunks)]
                        (.close chunk))
                      (set! (.chunks this) nil)

                      false))))

              (live-chunk []
                (let [chunks (idx/->live-slices watermark real-col-names)]
                  (set! (.chunks this) chunks)
                  (next-block chunks)))

              (next-chunk []
                (loop []
                  (when-let [{:keys [chunk-idx block-idxs]} (.poll matching-chunks)]
                    (or (when-let [block-idxs (reduce (fn [block-idxs col-name]
                                                        (or (->> block-idxs
                                                                 (filter-pushdown-bloom-block-idxs metadata-manager chunk-idx col-name))
                                                            (reduced nil)))
                                                      block-idxs
                                                      real-col-names)]
                          (let [chunks (->> (for [col-name real-col-names]
                                              (-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx col-name))
                                                  (util/then-apply
                                                    (fn [buf]
                                                      (MapEntry/create col-name (util/->chunks buf {:block-idxs block-idxs
                                                                                                    :close-buffer? true}))))))
                                            (remove nil?)
                                            vec
                                            (into {} (map deref)))]
                            (set! (.chunks this) chunks)

                            (next-block chunks)))
                        (recur)))))]

        (or (when chunks
              (next-block chunks))

            (next-chunk)

            (when-not live-chunk-done?
              (set! (.live-chunk-done? this) true)
              (live-chunk))

            false))))

  (close [_]
    (doseq [^ICursor chunk (vals chunks)]
      (util/try-close chunk))))

(defn ->scan-cursor ^core2.operator.scan.ScanCursor [^IMetadataManager metadata-manager
                                                     ^ITemporalManager temporal-manager
                                                     ^IBufferPool buffer-pool
                                                     ^Watermark watermark
                                                     ^List col-names
                                                     metadata-pred ;; TODO derive this from col-preds
                                                     ^Map col-preds
                                                     ^longs temporal-min-range, ^longs temporal-max-range]
  (let [matching-chunks (LinkedList. (or (meta/matching-chunks metadata-manager watermark metadata-pred) []))]
    (ScanCursor. buffer-pool temporal-manager metadata-manager watermark
                 matching-chunks col-names col-preds
                 temporal-min-range temporal-max-range
                 #_chunks nil #_live-chunk-done? false)))
