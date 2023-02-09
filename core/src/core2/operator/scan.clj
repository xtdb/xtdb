(ns core2.operator.scan
  (:require [clojure.spec.alpha :as s]
            [core2.align :as align]
            [core2.bloom :as bloom]
            [core2.coalesce :as coalesce]
            [core2.error :as err]
            [core2.expression :as expr]
            [core2.expression.metadata :as expr.meta]
            [core2.expression.temporal :as expr.temp]
            [core2.logical-plan :as lp]
            [core2.metadata :as meta]
            [core2.rewrite :refer [zmatch]]
            [core2.temporal :as temporal]
            [core2.types :as t]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            core2.watermark)
  (:import clojure.lang.MapEntry
           core2.buffer_pool.IBufferPool
           core2.ICursor
           core2.metadata.IMetadataManager
           core2.operator.IRelationSelector
           (core2.vector IIndirectRelation IIndirectVector)
           core2.watermark.IWatermark
           [java.util HashMap Iterator LinkedList List Map Queue]
           [java.util.function Consumer Function]
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
         :table simple-symbol?
         :columns (s/coll-of (s/or :column ::lp/column
                                   :select ::lp/column-expression))))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ScanSource
  (^core2.metadata.IMetadataManager metadataManager [])
  (^core2.buffer_pool.BufferPool bufferPool [])
  (^core2.api.TransactionInstant txBasis [])
  (^core2.watermark.IWatermark openWatermark []))

(def ^:dynamic *column->pushdown-bloom* {})

(defn ->scan-cols [{:keys [source table columns]}]
  (let [src-key (or source '$)]
    (for [column columns]
      [src-key table
       (zmatch column
         [:column col] col
         [:select col-map] (key (first col-map)))])))

(defn ->scan-col-types [srcs scan-cols]
  (let [mm+wms (HashMap.)]
    (try
      (letfn [(->mm+wm [src-key]
                (.computeIfAbsent mm+wms src-key
                                  (reify Function
                                    (apply [_ _src-key]
                                      (let [^ScanSource src (or (get srcs src-key)
                                                                (throw (err/illegal-arg :unknown-src
                                                                                        {::err/message "Query refers to unknown source"
                                                                                         :db src-key
                                                                                         :src-keys (keys srcs)})))]
                                        {:src src
                                         :wm (.openWatermark src)})))))

              (->col-type [[src-key table col-name]]
                (let [{:keys [^ScanSource src, ^IWatermark wm]} (->mm+wm src-key)]
                  (if (temporal/temporal-column? col-name)
                    [:timestamp-tz :micro "UTC"]
                    (t/merge-col-types (.columnType (.metadataManager src) (name table) (name col-name))
                                       (.columnType wm (name table) (name col-name))))))]

        (->> scan-cols
             (into {} (map (juxt identity ->col-type)))))

      (finally
        (run! util/try-close (map :wm (vals mm+wms)))))))

(defn- next-roots [col-names chunks]
  (when (and chunks (= (count col-names) (count chunks)))
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
  (when (seq col-names)
    (->> (for [^String col-name col-names
               :when (not (temporal/temporal-column? col-name))
               :let [^IRelationSelector col-pred (.get col-preds col-name)
                     ^VectorSchemaRoot in-root (.get in-roots (name col-name))]]
           (align/->row-id-bitmap (when col-pred
                                    (.select col-pred allocator (iv/<-root in-root) params))
                                  (.getVector in-root t/row-id-field)))
         (reduce roaring64-and))))

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

(defn- ->temporal-rel ^core2.vector.IIndirectRelation [^IWatermark watermark, ^BufferAllocator allocator, ^List col-names ^longs temporal-min-range ^longs temporal-max-range atemporal-row-id-bitmap]
  (let [temporal-min-range (adjust-temporal-min-range-to-row-id-range temporal-min-range atemporal-row-id-bitmap)
        temporal-max-range (adjust-temporal-max-range-to-row-id-range temporal-max-range atemporal-row-id-bitmap)]
    (.createTemporalRelation watermark allocator
                             (->> (conj col-names "_row-id")
                                  (into [] (comp (distinct) (filter temporal/temporal-column?))))
                             temporal-min-range
                             temporal-max-range
                             atemporal-row-id-bitmap)))

(defn- apply-temporal-preds ^core2.vector.IIndirectRelation [^IIndirectRelation temporal-rel, ^BufferAllocator allocator, ^Map col-preds, params]
  (->> (for [^IIndirectVector col temporal-rel
             :let [col-pred (get col-preds (.getName col))]
             :when col-pred]
         col-pred)
       (reduce (fn [^IIndirectRelation temporal-rel, ^IRelationSelector col-pred]
                 (-> temporal-rel
                     (iv/select (.select col-pred allocator temporal-rel params))))
               temporal-rel)))

(defn- filter-pushdown-bloom-block-idxs [^IMetadataManager metadata-manager chunk-idx ^String table-name ^String col-name ^RoaringBitmap block-idxs]
  (if-let [^MutableRoaringBitmap pushdown-bloom (get *column->pushdown-bloom* (symbol col-name))]
  ;; would prefer this `^long` to be on the param but can only have 4 params in a primitive hinted function in Clojure
    @(meta/with-metadata metadata-manager ^long chunk-idx table-name
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

(defn- remove-col ^core2.vector.IIndirectRelation [^IIndirectRelation rel, ^String col-name]
  (iv/->indirect-rel (remove #(= col-name (.getName ^IIndirectVector %)) rel)
                     (.rowCount rel)))

(deftype ScanCursor [^BufferAllocator allocator
                     ^IBufferPool buffer-pool
                     ^IMetadataManager metadata-manager
                     ^IWatermark watermark
                     ^String table-name
                     ^List content-col-names
                     ^List temporal-col-names
                     ^Map col-preds
                     ^longs temporal-min-range
                     ^longs temporal-max-range
                     ^Queue #_<ChunkMatch> matching-chunks
                     ^Iterator live-slices
                     params
                     ^:unsynchronized-mutable ^Map #_#_<String, ICursor> chunks]
  ICursor
  (tryAdvance [this c]
    (let [keep-row-id-col? (contains? (set temporal-col-names) "_row-id")
          keep-id-col? (contains? (set content-col-names) "id")
          content-col-names (or (not-empty content-col-names) #{"id"})]
      (letfn [(next-block [chunks]
                (loop []
                  (if-let [^Map in-roots (next-roots content-col-names chunks)]
                    (let [atemporal-row-id-bitmap (->atemporal-row-id-bitmap allocator content-col-names col-preds in-roots params)
                          temporal-rel (->temporal-rel watermark allocator temporal-col-names temporal-min-range temporal-max-range atemporal-row-id-bitmap)]
                      (or (try
                            (let [temporal-rel (-> temporal-rel (apply-temporal-preds allocator col-preds params))
                                  read-rel (cond-> (align/align-vectors (.values in-roots) temporal-rel)
                                             (not keep-row-id-col?) (remove-col "_row-id")
                                             (not keep-id-col?) (remove-col "id"))]
                              (if (and read-rel (pos? (.rowCount read-rel)))
                                (do
                                  (.accept c read-rel)
                                  true)
                                false))
                            (finally
                              (util/try-close temporal-rel)))

                          (recur)))

                    (do
                      (doseq [^ICursor chunk (vals chunks)]
                        (.close chunk))
                      (set! (.chunks this) nil)

                      false))))

              (live-chunk []
                (loop []
                  (when (.hasNext live-slices)
                    (let [chunks (.next live-slices)]
                      (set! (.chunks this) chunks)
                      (or (next-block chunks)
                          (recur))))))

              (next-chunk []
                (loop []
                  (when-let [{:keys [chunk-idx block-idxs]} (.poll matching-chunks)]
                    (or (when-let [block-idxs (reduce (fn [block-idxs col-name]
                                                        (or (->> block-idxs
                                                                 (filter-pushdown-bloom-block-idxs metadata-manager chunk-idx table-name col-name))
                                                            (reduced nil)))
                                                      block-idxs
                                                      content-col-names)]
                          (let [chunks (->> (for [col-name content-col-names]
                                              (-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx table-name col-name))
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

            (live-chunk)

            false))))

  (close [_]
    (doseq [^ICursor chunk (vals chunks)]
      (util/try-close chunk))))

(defn- apply-src-tx! [[^longs temporal-min-range, ^longs temporal-max-range], ^ScanSource src, col-preds]
  (when-let [sys-time (some-> (.txBasis src) (.sys-time))]
    (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                :<= "system_time_start" sys-time)

    (when-not (or (contains? col-preds "system_time_start")
                  (contains? col-preds "system_time_end"))
      (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                  :> "system_time_end" sys-time))))

(defmethod lp/emit-expr :scan [{:keys [source table columns]} {:keys [scan-col-types param-types srcs]}]
  (let [src-key (or source '$)

        col-names (->> columns
                       (into [] (comp (map (fn [[col-type arg]]
                                             (case col-type
                                               :column arg
                                               :select (key (first arg)))))
                                      (distinct))))

        {content-col-names false, temporal-col-names true} (->> col-names
                                                                (group-by (comp temporal/temporal-column? name)))

        col-types (->> col-names
                       (into {} (map (juxt identity
                                           (fn [col-name]
                                             (get scan-col-types [src-key table col-name]))))))

        selects (->> (for [[col-type arg] columns
                           :when (= col-type :select)]
                       (first arg))
                     (into {}))

        col-preds (->> (for [[col-name select-form] selects]
                         ;; for temporal preds, we may not need to re-apply these if they can be represented as a temporal range.
                         (MapEntry/create (name col-name)
                                          (expr/->expression-relation-selector select-form {:col-types col-types, :param-types param-types})))
                       (into {}))

        metadata-args (vec (concat (for [col-name content-col-names
                                         :when (not (contains? col-preds (name col-name)))]
                                     col-name)
                                   (for [[col-name select] selects
                                         :when (not (temporal/temporal-column? (name col-name)))]
                                     select)))
        row-count
        (let [^ScanSource src (get srcs src-key)
              metadata-mgr (.metadataManager src)]
          (reduce
            +
            (meta/matching-chunks
              metadata-mgr
              (name table)
              (fn [_chunk-idx ^VectorSchemaRoot metadata-root]
                (let [metadata-idxs (meta/->metadata-idxs metadata-root)
                      id-col-idx (.columnIndex metadata-idxs "id")
                      ^BigIntVector count-vec (.getVector metadata-root "count")]
                  (.get count-vec id-col-idx))))))]

{:col-types (dissoc col-types '_table)
     :stats {:row-count row-count}
     :->cursor (fn [{:keys [allocator srcs params]}]
                 (let [^ScanSource src (get srcs src-key)
                       metadata-mgr (.metadataManager src)
                       buffer-pool (.bufferPool src)
                       watermark (.openWatermark src)]
                   (try
                     (let [metadata-pred (expr.meta/->metadata-selector (cons 'and metadata-args) (set col-names) params)
                           [temporal-min-range temporal-max-range] (doto (expr.temp/->temporal-min-max-range selects params)
                                                                     (apply-src-tx! src col-preds))
                           matching-chunks (LinkedList. (or (meta/matching-chunks metadata-mgr (name table) metadata-pred) []))
                           content-col-names (mapv name content-col-names)
                           temporal-col-names (mapv name temporal-col-names)]
                       (-> (ScanCursor. allocator buffer-pool metadata-mgr watermark
                                        (name table) content-col-names temporal-col-names col-preds
                                        temporal-min-range temporal-max-range
                                        matching-chunks (-> (.liveSlices watermark (name table)
                                                                         (or (not-empty content-col-names) #{"id"}))
                                                            (.iterator))
                                        params
                                        #_chunks nil)
                           (coalesce/->coalescing-cursor allocator)
                           (util/and-also-close watermark)))
                     (catch Throwable t
                       (util/try-close watermark)
                       (throw t)))))}))
