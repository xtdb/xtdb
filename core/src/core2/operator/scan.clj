(ns core2.operator.scan
  (:require [clojure.core.match :refer [match]]
            [clojure.spec.alpha :as s]
            [core2.align :as align]
            [core2.bloom :as bloom]
            [core2.coalesce :as coalesce]
            [core2.error :as err]
            [core2.expression :as expr]
            [core2.expression.metadata :as expr.meta]
            [core2.expression.temporal :as expr.temp]
            [core2.indexer :as idx]
            [core2.logical-plan :as lp]
            [core2.metadata :as meta]
            core2.snapshot
            [core2.temporal :as temporal]
            [core2.types :as t]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import clojure.lang.MapEntry
           core2.api.TransactionInstant
           core2.buffer_pool.IBufferPool
           core2.ICursor
           core2.indexer.IChunkManager
           core2.metadata.IMetadataManager
           core2.operator.IRelationSelector
           core2.snapshot.Snapshot
           [core2.temporal ITemporalManager TemporalRoots]
           core2.tx.Watermark
           [java.util HashMap LinkedList List Map Queue]
           [java.util.function BiFunction Consumer Function]
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BigIntVector VarBinaryVector VectorSchemaRoot]
           [org.roaringbitmap IntConsumer RoaringBitmap]
           org.roaringbitmap.buffer.MutableRoaringBitmap
           org.roaringbitmap.longlong.Roaring64Bitmap))

;; TODO be good to just specify a single expression here and have the interpreter split it
;; into metadata + col-preds - the former can accept more than just `(and ~@col-preds)
(defmethod lp/ra-expr :scan [_]
  (s/cat :op #{:scan}
         :source (s/? ::lp/source)
         :columns (s/coll-of (s/or :column ::lp/column
                                   :select ::lp/column-expression)
                             :min-count 1)))

(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic *column->pushdown-bloom* {})

(defn ->scan-col-types [scan-exprs srcs]
  (let [mm+wms (HashMap.)]
    (try
      (letfn [(->mm+wm [src-key]
                (.computeIfAbsent mm+wms src-key
                                  (reify Function
                                    (apply [_ _src-key]
                                      (let [^Snapshot src (or (get srcs src-key)
                                                              (throw (err/illegal-arg :unknown-src
                                                                                      {::err/message "Query refers to unknown source"
                                                                                       :db src-key
                                                                                       :src-keys (keys srcs)})))]

                                        {:mm (.metadata-mgr src)
                                         :wm (.getWatermark ^IChunkManager (.indexer src))})))))

              (->col-type [[src-key col-name]]
                (let [{:keys [^IMetadataManager mm, ^Watermark wm]} (->mm+wm src-key)]
                  (if (temporal/temporal-column? col-name)
                    [:timestamp-tz :micro "UTC"]
                    (t/merge-col-types (.columnType mm col-name)
                                       (.columnType wm col-name)))))]

        (->> scan-exprs
             (into {} (comp (mapcat (fn ->scan-col-keys [{:keys [source columns]}]
                                      (let [src-key (or source '$)]
                                        (for [column columns]
                                          [src-key (name (match column
                                                           [:column col] col
                                                           [:select col-map] (key (first col-map))))]))))
                            (distinct)
                            (map (juxt identity ->col-type))))))

      (finally
        (run! util/try-close (map :wm (vals mm+wms)))))))

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

(defn- ->atemporal-row-id-bitmap [^BufferAllocator allocator, ^List col-names, ^Map col-preds, ^Map in-roots, params]
  (->> (for [^String col-name col-names
             :when (not (temporal/temporal-column? col-name))
             :let [^IRelationSelector col-pred (.get col-preds col-name)
                   ^VectorSchemaRoot in-root (.get in-roots col-name)]]
         (align/->row-id-bitmap (when col-pred
                                  (.select col-pred allocator (iv/<-root in-root) params))
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
                                     (apply [_ _k v]
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

(defn- ->temporal-row-id-bitmap [^BufferAllocator allocator, col-names, ^Map col-preds, ^TemporalRoots temporal-roots, atemporal-row-id-bitmap, params]
  (reduce roaring64-and
          (if temporal-roots
            (.row-id-bitmap temporal-roots)
            atemporal-row-id-bitmap)
          (for [^String col-name col-names
                :when (temporal/temporal-column? col-name)
                :let [^IRelationSelector col-pred (.get col-preds col-name)
                      ^VectorSchemaRoot in-root (.get ^Map (.roots temporal-roots) col-name)]]
            (align/->row-id-bitmap (when col-pred
                                     (.select col-pred allocator (iv/<-root in-root) params))
                                   (.getVector in-root t/row-id-field)))))

(defn- align-roots ^core2.vector.IIndirectRelation [^List col-names ^Map in-roots ^TemporalRoots temporal-roots row-id-bitmap]
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
             (let [filtered-block-idxs (RoaringBitmap.)]
               (.forEach block-idxs
                         (reify IntConsumer
                           (accept [_ block-idx]
                             (when-let [bloom-vec-idx (.blockIndex metadata-idxs col-name block-idx)]
                               (when (and (not (.isNull bloom-vec bloom-vec-idx))
                                          (MutableRoaringBitmap/intersects pushdown-bloom
                                                                           (bloom/bloom->bitmap bloom-vec bloom-vec-idx)))
                                 (.add filtered-block-idxs block-idx))))))

               (when-not (.isEmpty filtered-block-idxs)
                 filtered-block-idxs))))))
    block-idxs))

(deftype ScanCursor [^BufferAllocator allocator
                     ^IBufferPool buffer-pool
                     ^ITemporalManager temporal-manager
                     ^IMetadataManager metadata-manager
                     ^Watermark watermark
                     ^Queue #_<ChunkMatch> matching-chunks
                     ^List col-names
                     ^Map col-preds
                     ^longs temporal-min-range
                     ^longs temporal-max-range
                     params
                     ^:unsynchronized-mutable ^Map #_#_<String, ICursor> chunks
                     ^:unsynchronized-mutable ^boolean live-chunk-done?]
  ICursor
  (tryAdvance [this c]
    (let [real-col-names (remove temporal/temporal-column? col-names)]
      (letfn [(next-block [chunks]
                (loop []
                  (if-let [in-roots (next-roots real-col-names chunks)]
                    (let [atemporal-row-id-bitmap (->atemporal-row-id-bitmap allocator col-names col-preds in-roots params)
                          temporal-roots (->temporal-roots temporal-manager watermark col-names temporal-min-range temporal-max-range atemporal-row-id-bitmap)]
                      (or (try
                            (let [row-id-bitmap (->temporal-row-id-bitmap allocator col-names col-preds temporal-roots atemporal-row-id-bitmap params)
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

(defn ->scan-cursor ^core2.operator.scan.ScanCursor [^BufferAllocator allocator
                                                     ^Snapshot snapshot
                                                     ^List col-names
                                                     metadata-pred ;; TODO derive this from col-preds
                                                     ^Map col-preds
                                                     [^longs temporal-min-range, ^longs temporal-max-range]]
  )

(defn- apply-default-valid-time! [[^longs temporal-min-range, ^longs temporal-max-range] default-valid-time col-preds]
  (when-not (or (contains? col-preds "_valid-time-start")
                (contains? col-preds "_valid-time-end"))
    (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                '<= "_valid-time-start" default-valid-time)
    (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                '> "_valid-time-end" default-valid-time)))

(defn- apply-snapshot-tx! [[^longs temporal-min-range, ^longs temporal-max-range], ^Snapshot snapshot, col-preds]
  (when-let [tx-time (.tx-time ^TransactionInstant (.tx snapshot))]
    (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                '<= "_tx-time-start" tx-time)

    (when-not (or (contains? col-preds "_tx-time-start")
                  (contains? col-preds "_tx-time-end"))
      (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                  '> "_tx-time-end" tx-time))))

(defmethod lp/emit-expr :scan [{:keys [source columns]} {:keys [scan-col-types param-types]}]
  (let [src-key (or source '$)

        ordered-col-names (->> columns
                               (map (fn [[col-type arg]]
                                      (str (case col-type
                                             :column arg
                                             :select (key (first arg))))))
                               (distinct))
        selects (->> (for [[col-type arg] columns
                           :when (= col-type :select)]
                       (first arg))
                     (into {}))
        col-preds (->> (for [[col-name select-form] selects]
                         (MapEntry/create (name col-name)
                                          (expr/->expression-relation-selector select-form {:col-names #{col-name}, :param-types param-types})))
                       (into {}))
        metadata-args (vec (concat (for [col-name ordered-col-names
                                         :when (not (contains? col-preds col-name))]
                                     (symbol col-name))
                                   (vals selects)))]
    {:col-types (->> ordered-col-names
                     (into {} (map (juxt identity
                                         (fn [col-name]
                                           (get scan-col-types [src-key col-name]))))))
     :->cursor (fn [{:keys [allocator srcs params default-valid-time]}]
                 (let [^Snapshot snapshot (get srcs src-key)
                       ^IChunkManager indexer (.indexer snapshot)
                       metadata-mgr (.metadata-mgr snapshot)
                       buffer-pool (.buffer-pool snapshot)
                       temporal-mgr (.temporal-mgr snapshot)
                       watermark (.getWatermark indexer)
                       metadata-pred (expr.meta/->metadata-selector (cons 'and metadata-args) (into #{} (map symbol) ordered-col-names) params)
                       [temporal-min-range temporal-max-range] (doto (expr.temp/->temporal-min-max-range selects params)
                                                                 (apply-default-valid-time! default-valid-time col-preds)
                                                                 (apply-snapshot-tx! snapshot col-preds))]
                   (try
                     (let [matching-chunks (LinkedList. (or (meta/matching-chunks metadata-mgr watermark metadata-pred) []))]
                       (-> (ScanCursor. allocator buffer-pool temporal-mgr metadata-mgr watermark
                                        matching-chunks ordered-col-names col-preds
                                        temporal-min-range temporal-max-range params
                                        #_chunks nil #_live-chunk-done? false)
                           (coalesce/->coalescing-cursor allocator)
                           (util/and-also-close watermark)))
                     (catch Throwable t
                       (util/try-close watermark)
                       (throw t)))))}))
