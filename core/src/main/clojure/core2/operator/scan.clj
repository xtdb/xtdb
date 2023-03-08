(ns core2.operator.scan
  (:require [clojure.spec.alpha :as s]
            [core2.align :as align]
            [core2.bloom :as bloom]
            [core2.buffer-pool :as bp]
            [core2.coalesce :as coalesce]
            [core2.expression :as expr]
            [core2.expression.metadata :as expr.meta]
            [core2.expression.temporal :as expr.temp]
            [core2.logical-plan :as lp]
            [core2.metadata :as meta]
            [core2.temporal :as temporal]
            [core2.types :as t]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            core2.watermark
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import clojure.lang.MapEntry
           core2.api.TransactionInstant
           core2.buffer_pool.IBufferPool
           core2.ICursor
           (core2.metadata IMetadataManager ITableMetadata)
           core2.operator.IRelationSelector
           (core2.vector IIndirectRelation IIndirectVector)
           core2.watermark.IWatermark
           [java.util LinkedList List Map Queue]
           [java.util.function Consumer]
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BigIntVector VarBinaryVector]
           [org.roaringbitmap IntConsumer RoaringBitmap]
           org.roaringbitmap.buffer.MutableRoaringBitmap
           org.roaringbitmap.longlong.Roaring64Bitmap))

;; TODO be good to just specify a single expression here and have the interpreter split it
;; into metadata + col-preds - the former can accept more than just `(and ~@col-preds)
(defmethod lp/ra-expr :scan [_]
  (s/cat :op #{:scan}
         :table simple-symbol?
         :columns (s/coll-of (s/or :column ::lp/column
                                   :select ::lp/column-expression))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IScanEmitter
  (scanColTypes [^core2.watermark.IWatermark wm, scan-cols])
  (emitScan [scan-expr scan-col-types param-types]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->scan-cols [{:keys [table columns]}]
  (for [[col-tag col-arg] columns]
    [table (case col-tag
             :column col-arg
             :select (key (first col-arg)))]))

(def ^:dynamic *column->pushdown-bloom* {})

(defn- filter-pushdown-bloom-block-idxs [^IMetadataManager metadata-manager chunk-idx ^String table-name ^String col-name ^RoaringBitmap block-idxs]
  (if-let [^MutableRoaringBitmap pushdown-bloom (get *column->pushdown-bloom* (symbol col-name))]
  ;; would prefer this `^long` to be on the param but can only have 4 params in a primitive hinted function in Clojure
    @(meta/with-metadata metadata-manager ^long chunk-idx table-name
       (util/->jfn
         (fn [^ITableMetadata table-metadata]
           (let [metadata-root (.metadataRoot table-metadata)
                 ^VarBinaryVector bloom-vec (.getVector metadata-root "bloom")]
             (when (MutableRoaringBitmap/intersects pushdown-bloom
                                                    (bloom/bloom->bitmap bloom-vec (.rowIndex table-metadata col-name -1)))
               (let [filtered-block-idxs (RoaringBitmap.)]
                 (.forEach block-idxs
                           (reify IntConsumer
                             (accept [_ block-idx]
                               (when-let [bloom-vec-idx (.rowIndex table-metadata col-name block-idx)]
                                 (when (and (not (.isNull bloom-vec bloom-vec-idx))
                                            (MutableRoaringBitmap/intersects pushdown-bloom
                                                                             (bloom/bloom->bitmap bloom-vec bloom-vec-idx)))
                                   (.add filtered-block-idxs block-idx))))))

                 (when-not (.isEmpty filtered-block-idxs)
                   filtered-block-idxs)))))))
    block-idxs))

(deftype ContentChunkCursor [^IMetadataManager metadata-mgr
                             ^IBufferPool buffer-pool
                             table-name content-col-names
                             ^Queue matching-chunks
                             ^ICursor current-cursor]
  ICursor #_#_<Map<String, IIR>>
  (tryAdvance [this c]
    (loop []
      (or (when current-cursor
            (or (.tryAdvance current-cursor
                             (reify Consumer
                               (accept [_ in-roots]
                                 (.accept c (update-vals in-roots iv/<-root)))))
                (do
                  (util/try-close current-cursor)
                  (set! (.-current-cursor this) nil)
                  false)))

          (if-let [{:keys [chunk-idx block-idxs]} (.poll matching-chunks)]
            (if-let [block-idxs (reduce (fn [block-idxs col-name]
                                          (or (->> block-idxs
                                                   (filter-pushdown-bloom-block-idxs metadata-mgr chunk-idx table-name col-name))
                                              (reduced nil)))
                                        block-idxs
                                        content-col-names)]

              (let [col-cursors (->> (for [col-name content-col-names]
                                       (-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx table-name col-name))
                                           (util/then-apply
                                             (fn [buf]
                                               (MapEntry/create col-name (util/->chunks buf {:block-idxs block-idxs
                                                                                             :close-buffer? true}))))))
                                     (remove nil?)
                                     vec
                                     (into {} (map deref)))]

                (when (= (count col-cursors) (count content-col-names))
                  (set! (.current-cursor this) (util/combine-col-cursors col-cursors)))

                (recur))

              (recur))

            false))))

  (close [_]
    (some-> current-cursor util/try-close)))

(defn- ->content-chunks ^core2.ICursor [^IMetadataManager metadata-mgr
                                        ^IBufferPool buffer-pool
                                        table-name content-col-names
                                        metadata-pred]
  (ContentChunkCursor. metadata-mgr buffer-pool table-name content-col-names
                       (LinkedList. (or (meta/matching-chunks metadata-mgr table-name metadata-pred) []))
                       nil))

(defn- roaring64-and
  (^org.roaringbitmap.longlong.Roaring64Bitmap [] (Roaring64Bitmap.))
  (^org.roaringbitmap.longlong.Roaring64Bitmap [^Roaring64Bitmap x] x)
  (^org.roaringbitmap.longlong.Roaring64Bitmap [^Roaring64Bitmap x ^Roaring64Bitmap y]
   (doto x
     (.and y))))

(defn- ->atemporal-row-id-bitmap [^BufferAllocator allocator, ^List col-names, ^Map col-preds, ^Map in-rels, params]
  (when (seq col-names)
    (->> (for [^String col-name col-names
               :when (not (temporal/temporal-column? col-name))
               :let [^IRelationSelector col-pred (.get col-preds col-name)
                     ^IIndirectRelation in-rel (.get in-rels (name col-name))]]
           (align/->row-id-bitmap (when col-pred
                                    (.select col-pred allocator in-rel params))
                                  (.vectorForName in-rel "_row-id")))
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
    (.createTemporalRelation (.temporalRootsSource watermark)
                             allocator
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

(defn- remove-col ^core2.vector.IIndirectRelation [^IIndirectRelation rel, ^String col-name]
  (iv/->indirect-rel (remove #(= col-name (.getName ^IIndirectVector %)) rel)
                     (.rowCount rel)))

(deftype ScanCursor [^BufferAllocator allocator
                     ^IMetadataManager metadata-manager
                     ^IWatermark watermark
                     ^List content-col-names
                     ^List temporal-col-names
                     ^Map col-preds
                     ^longs temporal-min-range
                     ^longs temporal-max-range
                     ^ICursor #_#_<Map<String, IIR>> blocks
                     params]
  ICursor
  (tryAdvance [_ c]
    (let [keep-row-id-col? (contains? (set temporal-col-names) "_row-id")
          keep-id-col? (contains? (set content-col-names) "id")
          content-col-names (or (not-empty content-col-names) #{"id"})
          !advanced? (volatile! false)]

      (while (and (not @!advanced?)
                  (.tryAdvance blocks
                               (reify Consumer
                                 (accept [_ in-roots]
                                   (let [^Map in-roots in-roots
                                         atemporal-row-id-bitmap (->atemporal-row-id-bitmap allocator content-col-names col-preds in-roots params)
                                         temporal-rel (->temporal-rel watermark allocator temporal-col-names temporal-min-range temporal-max-range atemporal-row-id-bitmap)]
                                     (try
                                       (let [temporal-rel (-> temporal-rel (apply-temporal-preds allocator col-preds params))
                                             read-rel (cond-> (align/align-vectors (.values in-roots) temporal-rel)
                                                        (not keep-row-id-col?) (remove-col "_row-id")
                                                        (not keep-id-col?) (remove-col "id"))]
                                         (when (and read-rel (pos? (.rowCount read-rel)))
                                           (.accept c read-rel)
                                           (vreset! !advanced? true)))
                                       (finally
                                         (util/try-close temporal-rel)))))))))
      (boolean @!advanced?)))

  (close [_]
    (util/try-close blocks)))

(defn- apply-basis-tx! [[^longs temporal-min-range, ^longs temporal-max-range], {^TransactionInstant basis-tx :tx} col-preds]
  (when-let [sys-time (some-> basis-tx (.sys-time))]
    (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                :<= "system_time_start" sys-time)

    (when-not (or (contains? col-preds "system_time_start")
                  (contains? col-preds "system_time_end"))
      (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                  :> "system_time_end" sys-time))))

(defmethod ig/prep-key ::scan-emitter [_ opts]
  (merge opts
         {:metadata-mgr (ig/ref ::meta/metadata-manager)
          :buffer-pool (ig/ref ::bp/buffer-pool)}))

(defmethod ig/init-key ::scan-emitter [_ {:keys [^IMetadataManager metadata-mgr, ^IBufferPool buffer-pool]}]
  (reify IScanEmitter
    (scanColTypes [_ wm scan-cols]
      (letfn [(->col-type [[table col-name]]
                (if (temporal/temporal-column? col-name)
                  [:timestamp-tz :micro "UTC"]
                  (t/merge-col-types (.columnType metadata-mgr (name table) (name col-name))
                                     (some-> (.liveChunk wm)
                                             (.liveTable (name table))
                                             (.liveColumn (name col-name))
                                             (.columnType)))))]

        (->> scan-cols
             (into {} (map (juxt identity ->col-type))))))

    (emitScan [_ {:keys [table columns]} scan-col-types param-types]
      (let [col-names (->> columns
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
                                                 (get scan-col-types [table col-name]))))))

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
            row-count (->> (meta/with-all-metadata metadata-mgr (name table)
                             (util/->jbifn
                              (fn [_chunk-idx ^ITableMetadata table-metadata]
                                (let [id-col-idx (.rowIndex table-metadata "id" -1)
                                      ^BigIntVector count-vec (.getVector (.metadataRoot table-metadata) "count")]
                                  (.get count-vec id-col-idx)))))
                           (reduce +))]

        {:col-types (dissoc col-types '_table)
         :stats {:row-count row-count}
         :->cursor (fn [{:keys [allocator, ^IWatermark watermark, basis, params]}]
                     (let [metadata-pred (expr.meta/->metadata-selector (cons 'and metadata-args) (set col-names) params)
                           [temporal-min-range temporal-max-range] (doto (expr.temp/->temporal-min-max-range selects params)
                                                                     (apply-basis-tx! basis col-preds))
                           content-col-names (mapv name content-col-names)
                           temporal-col-names (mapv name temporal-col-names)]
                       (-> (ScanCursor. allocator metadata-mgr watermark
                                        content-col-names temporal-col-names col-preds
                                        temporal-min-range temporal-max-range
                                        (util/->concat-cursor (->content-chunks metadata-mgr buffer-pool
                                                                                (name table) content-col-names
                                                                                metadata-pred)
                                                              (some-> (.liveChunk watermark)
                                                                      (.liveTable (name table))
                                                                      (.liveBlocks (or (not-empty content-col-names) #{"id"})
                                                                                   metadata-pred)))
                                        params)
                           (coalesce/->coalescing-cursor allocator))))}))))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter scan-col-types, param-types]}]
  (.emitScan scan-emitter scan-expr scan-col-types param-types))
