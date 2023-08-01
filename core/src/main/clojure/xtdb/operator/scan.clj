(ns xtdb.operator.scan
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.bloom :as bloom]
            [xtdb.buffer-pool :as bp]
            [xtdb.coalesce :as coalesce]
            [xtdb.expression :as expr]
            [xtdb.expression.metadata :as expr.meta]
            [xtdb.expression.walk :as expr.walk]
            xtdb.indexer.live-index
            [xtdb.logical-plan :as lp]
            [xtdb.metadata :as meta]
            xtdb.object-store
            [xtdb.temporal :as temporal]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            xtdb.watermark)
  (:import (clojure.lang IPersistentSet MapEntry)
           (java.util ArrayList Arrays HashMap Iterator LinkedList List Map Queue Set)
           (java.util.function BiFunction Consumer IntConsumer)
           java.util.stream.IntStream
           org.apache.arrow.memory.ArrowBuf
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.memory.util ArrowBufPointer]
           (org.apache.arrow.vector BigIntVector NullVector VarBinaryVector VectorLoader VectorSchemaRoot)
           (org.apache.arrow.vector.complex ListVector StructVector)
           (org.roaringbitmap RoaringBitmap)
           org.roaringbitmap.buffer.MutableRoaringBitmap
           (org.roaringbitmap.longlong Roaring64Bitmap)
           xtdb.api.protocols.TransactionInstant
           xtdb.buffer_pool.IBufferPool
           xtdb.ICursor
           xtdb.indexer.live_index.ILiveTableWatermark
           (xtdb.metadata IMetadataManager ITableMetadata)
           xtdb.object_store.ObjectStore
           xtdb.operator.IRelationSelector
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrie LeafMerge LeafMerge$LeafPointer LiveHashTrie$Leaf)
           (xtdb.vector IRelationWriter IRowCopier IVectorReader IVectorWriter RelationReader)
           (xtdb.watermark IWatermark IWatermarkSource Watermark)))

(s/def ::table symbol?)

;; TODO be good to just specify a single expression here and have the interpreter split it
;; into metadata + col-preds - the former can accept more than just `(and ~@col-preds)
(defmethod lp/ra-expr :scan [_]
  (s/cat :op #{:scan}
         :scan-opts (s/keys :req-un [::table]
                            :opt-un [::lp/for-valid-time ::lp/for-system-time ::lp/default-all-valid-time?])
         :columns (s/coll-of (s/or :column ::lp/column
                                   :select ::lp/column-expression))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IScanEmitter
  (tableColNames [^xtdb.watermark.IWatermark wm, ^String table-name])
  (allTableColNames [^xtdb.watermark.IWatermark wm])
  (scanColTypes [^xtdb.watermark.IWatermark wm, scan-cols])
  (emitScan [scan-expr scan-col-types param-types]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->scan-cols [{:keys [columns], {:keys [table]} :scan-opts}]
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
                 ^VarBinaryVector bloom-vec (-> ^ListVector (.getVector metadata-root "columns")
                                                ^StructVector (.getDataVector)
                                                (.getChild "bloom"))]
             (when (MutableRoaringBitmap/intersects pushdown-bloom
                                                    (bloom/bloom->bitmap bloom-vec (.rowIndex table-metadata col-name -1)))
               (let [filtered-block-idxs (RoaringBitmap.)]
                 (.forEach block-idxs
                           (reify org.roaringbitmap.IntConsumer
                             (accept [_ block-idx]
                               (when-let [bloom-vec-idx (.rowIndex table-metadata col-name block-idx)]
                                 (when (and (not (.isNull bloom-vec bloom-vec-idx))
                                            (MutableRoaringBitmap/intersects pushdown-bloom
                                                                             (bloom/bloom->bitmap bloom-vec bloom-vec-idx)))
                                   (.add filtered-block-idxs block-idx))))))

                 (when-not (.isEmpty filtered-block-idxs)
                   filtered-block-idxs)))))))
    block-idxs))

(deftype ContentChunkCursor [^BufferAllocator allocator
                             ^IMetadataManager metadata-mgr
                             ^IBufferPool buffer-pool
                             table-name content-col-names
                             ^Queue matching-chunks
                             ^ICursor current-cursor]
  ICursor #_<RR>
  (tryAdvance [this c]
    (loop []
      (or (when current-cursor
            (or (.tryAdvance current-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (.accept c (-> (vr/<-root in-root)
                                                (vr/with-absent-cols allocator content-col-names))))))
                (do
                  (util/try-close current-cursor)
                  (set! (.-current-cursor this) nil)
                  false)))

          (if-let [{:keys [chunk-idx block-idxs col-names]} (.poll matching-chunks)]
            (if-let [block-idxs (reduce (fn [block-idxs col-name]
                                          (or (->> block-idxs
                                                   (filter-pushdown-bloom-block-idxs metadata-mgr chunk-idx table-name col-name))
                                              (reduced nil)))
                                        block-idxs
                                        content-col-names)]

              (do
                (set! (.current-cursor this)
                      (->> (for [col-name (set/intersection col-names content-col-names)]
                             (-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx table-name col-name))
                                 (util/then-apply
                                   (fn [buf]
                                     (MapEntry/create col-name
                                                      (util/->chunks buf {:block-idxs block-idxs, :close-buffer? true}))))))
                           (remove nil?)
                           vec
                           (into {} (map deref))
                           (util/rethrowing-cause)
                           (util/combine-col-cursors)))

                (recur))

              (recur))

            false))))

  (close [_]
    (some-> current-cursor util/try-close)))

(defn- ->content-chunks ^xtdb.ICursor [^BufferAllocator allocator
                                       ^IMetadataManager metadata-mgr
                                       ^IBufferPool buffer-pool
                                       table-name content-col-names
                                       metadata-pred]
  (ContentChunkCursor. allocator metadata-mgr buffer-pool table-name content-col-names
                       (LinkedList. (or (meta/matching-chunks metadata-mgr table-name metadata-pred) []))
                       nil))

(defn- roaring-and
  (^org.roaringbitmap.RoaringBitmap [] (RoaringBitmap.))
  (^org.roaringbitmap.RoaringBitmap [^RoaringBitmap x] x)
  (^org.roaringbitmap.RoaringBitmap [^RoaringBitmap x ^RoaringBitmap y]
   (doto x
     (.and y))))

(defn- ->atemporal-row-id-bitmap [^BufferAllocator allocator, ^Map col-preds, ^RelationReader in-rel, params]
  (let [row-id-rdr (.readerForName in-rel "_row_id")
        res (Roaring64Bitmap.)]

    (if-let [content-col-preds (seq (remove (comp temporal/temporal-column? util/str->normal-form-str str key) col-preds))]
      (let [^RoaringBitmap
            idx-bitmap (->> (for [^IRelationSelector col-pred (vals content-col-preds)]
                              (RoaringBitmap/bitmapOf (.select col-pred allocator in-rel params)))
                            (reduce roaring-and))]
        (.forEach idx-bitmap
                  (reify org.roaringbitmap.IntConsumer
                    (accept [_ idx]
                      (.addLong res (.getLong row-id-rdr idx))))))

      (dotimes [idx (.valueCount row-id-rdr)]
        (.addLong res (.getLong row-id-rdr idx))))

    res))

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

(defn- select-current-row-ids ^xtdb.vector.RelationReader [^RelationReader content-rel, ^Roaring64Bitmap atemporal-row-id-bitmap, ^IPersistentSet current-row-ids]
  (let [sel (IntStream/builder)
        row-id-rdr (.readerForName content-rel "_row_id")]
    (dotimes [idx (.rowCount content-rel)]
      (let [row-id (.getLong row-id-rdr idx)]
        (when (and (.contains atemporal-row-id-bitmap row-id)
                   (.contains current-row-ids row-id))
          (.add sel idx))))

    (.select content-rel (.toArray (.build sel)))))

(defn- ->temporal-rel ^xtdb.vector.RelationReader [^IWatermark watermark, ^BufferAllocator allocator, ^List col-names ^longs temporal-min-range ^longs temporal-max-range atemporal-row-id-bitmap]
  (let [temporal-min-range (adjust-temporal-min-range-to-row-id-range temporal-min-range atemporal-row-id-bitmap)
        temporal-max-range (adjust-temporal-max-range-to-row-id-range temporal-max-range atemporal-row-id-bitmap)]
    (.createTemporalRelation (.temporalRootsSource watermark)
                             allocator
                             (->> (conj col-names "_row_id")
                                  (into [] (comp (distinct) (filter temporal/temporal-column?))))
                             temporal-min-range
                             temporal-max-range
                             atemporal-row-id-bitmap)))

(defn- apply-temporal-preds ^xtdb.vector.RelationReader [^RelationReader temporal-rel, ^BufferAllocator allocator, ^Map col-preds, params]
  (->> (for [^IVectorReader col temporal-rel
             :let [col-pred (get col-preds (.getName col))]
             :when col-pred]
         col-pred)
       (reduce (fn [^RelationReader temporal-rel, ^IRelationSelector col-pred]
                 (.select temporal-rel (.select col-pred allocator temporal-rel params)))
               temporal-rel)))

(defn- ->row-id->repeat-count ^java.util.Map [^IVectorReader row-id-col]
  (let [res (HashMap.)]
    (dotimes [idx (.valueCount row-id-col)]
      (let [row-id (.getLong row-id-col idx)]
        (.compute res row-id (reify BiFunction
                               (apply [_ _k v]
                                 (if v
                                   (inc (long v))
                                   1))))))
    res))

(defn align-vectors ^xtdb.vector.RelationReader [^RelationReader content-rel, ^RelationReader temporal-rel]
  ;; assumption: temporal-rel is sorted by row-id
  (let [temporal-row-id-col (.readerForName temporal-rel "_row_id")
        content-row-id-rdr (.readerForName content-rel "_row_id")
        row-id->repeat-count (->row-id->repeat-count temporal-row-id-col)
        sel (IntStream/builder)]
    (assert temporal-row-id-col)

    (dotimes [idx (.valueCount content-row-id-rdr)]
      (let [row-id (.getLong content-row-id-rdr idx)]
        (when-let [ns (.get row-id->repeat-count row-id)]
          (dotimes [_ ns]
            (.add sel idx)))))

    (vr/rel-reader (concat temporal-rel
                           (.select content-rel (.toArray (.build sel)))))))

(defn- remove-col ^xtdb.vector.RelationReader [^RelationReader rel, ^String col-name]
  (vr/rel-reader (remove #(= col-name (.getName ^IVectorReader %)) rel)
                 (.rowCount rel)))

(defn- unnormalize-column-names ^xtdb.vector.RelationReader [^RelationReader rel col-names]
  (vr/rel-reader
   (map (fn [col-name]
          (-> (.readerForName ^RelationReader rel (util/str->normal-form-str col-name))
              (.withName col-name)))
        col-names)))

(deftype ScanCursor [^BufferAllocator allocator
                     ^IMetadataManager metadata-manager
                     ^IWatermark watermark
                     ^Set content-col-names
                     ^Set temporal-col-names
                     ^Map col-preds
                     ^longs temporal-min-range
                     ^longs temporal-max-range
                     ^IPersistentSet current-row-ids
                     ^ICursor #_<RR> blocks
                     params]
  ICursor
  (tryAdvance [_ c]
    (let [keep-row-id-col? (contains? temporal-col-names "_row_id")
          !advanced? (volatile! false)
          normalized-temporal-col-names (into #{} (map util/str->normal-form-str) temporal-col-names)]

      (while (and (not @!advanced?)
                  (.tryAdvance blocks
                               (reify Consumer
                                 (accept [_ content-rel]
                                   (let [content-rel (unnormalize-column-names content-rel content-col-names)
                                         atemporal-row-id-bitmap (->atemporal-row-id-bitmap allocator col-preds content-rel params)]
                                     (letfn [(accept-rel [^RelationReader read-rel]
                                               (when (and read-rel (pos? (.rowCount read-rel)))
                                                 (let [read-rel (cond-> read-rel
                                                                  (not keep-row-id-col?) (remove-col "_row_id"))]
                                                   (.accept c read-rel)
                                                   (vreset! !advanced? true))))]
                                       (if current-row-ids
                                         (accept-rel (-> content-rel
                                                         (select-current-row-ids atemporal-row-id-bitmap current-row-ids)))

                                         (let [temporal-rel (->temporal-rel watermark allocator normalized-temporal-col-names
                                                                            temporal-min-range temporal-max-range atemporal-row-id-bitmap)]
                                           (try
                                             (let [temporal-rel (-> temporal-rel
                                                                    (unnormalize-column-names (conj temporal-col-names "_row_id"))
                                                                    (apply-temporal-preds allocator col-preds params))]
                                               (accept-rel (align-vectors content-rel temporal-rel)))
                                             (finally
                                               (util/try-close temporal-rel))))))))))))
      (boolean @!advanced?)))

  (close [_]
    (util/try-close blocks)))

(defn ->temporal-min-max-range [^RelationReader params, {^TransactionInstant basis-tx :tx}, {:keys [for-valid-time for-system-time]}, selects]
  (let [min-range (temporal/->min-range)
        max-range (temporal/->max-range)]
    (letfn [(apply-bound [f col-name ^long time-μs]
              (let [range-idx (temporal/->temporal-column-idx (util/str->normal-form-str (str col-name)))]
                (case f
                  :< (aset max-range range-idx
                           (min (dec time-μs) (aget max-range range-idx)))
                  :<= (aset max-range range-idx
                            (min time-μs (aget max-range range-idx)))
                  :> (aset min-range range-idx
                           (max (inc time-μs) (aget min-range range-idx)))
                  :>= (aset min-range range-idx
                            (max time-μs (aget min-range range-idx)))
                  nil)))

            (->time-μs [[tag arg]]
              (case tag
                :literal (-> arg
                             (util/sql-temporal->micros (.getZone expr/*clock*)))
                :param (-> (-> (.readerForName params (name arg))
                               (.getObject 0))
                           (util/sql-temporal->micros (.getZone expr/*clock*)))
                :now (-> (.instant expr/*clock*)
                         (util/instant->micros))))]

      (when-let [system-time (some-> basis-tx (.system-time) util/instant->micros)]
        (apply-bound :<= "xt$system_from" system-time)

        (when-not for-system-time
          (apply-bound :> "xt$system_to" system-time)))

      (letfn [(apply-constraint [constraint start-col end-col]
                (when-let [[tag & args] constraint]
                  (case tag
                    :at (let [[at] args
                              at-μs (->time-μs at)]
                          (apply-bound :<= start-col at-μs)
                          (apply-bound :> end-col at-μs))

                    ;; overlaps [time-from time-to]
                    :in (let [[from to] args]
                          (apply-bound :> end-col (->time-μs (or from [:now])))
                          (when to
                            (apply-bound :< start-col (->time-μs to))))

                    :between (let [[from to] args]
                               (apply-bound :> end-col (->time-μs (or from [:now])))
                               (when to
                                 (apply-bound :<= start-col (->time-μs to))))

                    :all-time nil)))]

        (apply-constraint for-valid-time "xt$valid_from" "xt$valid_to")
        (apply-constraint for-system-time "xt$system_from" "xt$system_to"))

      (let [col-types (into {} (map (juxt first #(get temporal/temporal-col-types (util/str->normal-form-str (str (first %)))))) selects)
            param-types (expr/->param-types params)]
        (doseq [[col-name select-form] selects
                :when (temporal/temporal-column? (util/str->normal-form-str (str col-name)))]
          (->> (-> (expr/form->expr select-form {:param-types param-types, :col-types col-types})
                   (expr/prepare-expr)
                   (expr.meta/meta-expr {:col-types col-types}))
               (expr.walk/prewalk-expr
                (fn [{:keys [op] :as expr}]
                  (case op
                    :call (when (not= :or (:f expr))
                            expr)

                    :metadata-vp-call
                    (let [{:keys [f param-expr]} expr]
                      (when-let [v (if-let [[_ literal] (find param-expr :literal)]
                                     (when literal (->time-μs [:literal literal]))
                                     (->time-μs [:param (get param-expr :param)]))]
                        (apply-bound f col-name v)))

                    expr)))))
        [min-range max-range]))

    [min-range max-range]))

(defn- scan-op-at-now [scan-op]
  (= :now (first (second scan-op))))

(defn- at-now? [{:keys [for-valid-time for-system-time]}]
  (and (or (nil? for-valid-time)
           (scan-op-at-now for-valid-time))
       (or (nil? for-system-time)
           (scan-op-at-now for-system-time))))

(defn- scan-op-point? [scan-op]
  (= :at (first scan-op)))

(defn- at-point-point? [{:keys [for-valid-time for-system-time]}]
  (and (or (nil? for-valid-time)
           (scan-op-point? for-valid-time))
       (or (nil? for-system-time)
           (scan-op-point? for-system-time))))

(defn use-current-row-id-cache? [^IWatermark watermark scan-opts basis temporal-col-names]
  (and
   (.txBasis watermark)
   (= (:tx basis)
      (.txBasis watermark))
   (at-now? scan-opts)
   (>= (util/instant->micros (:current-time basis))
       (util/instant->micros (:system-time (:tx basis))))
   (empty? (remove #(= % "xt$id") temporal-col-names))))

(defn get-current-row-ids [^IWatermark watermark basis]
  (.getCurrentRowIds
   ^xtdb.temporal.ITemporalRelationSource
   (.temporalRootsSource watermark)
   (util/instant->micros (:current-time basis))))

(defn tables-with-cols [basis ^IWatermarkSource wm-src ^IScanEmitter scan-emitter]
  (let [{:keys [tx, after-tx]} basis
        wm-tx (or tx after-tx)]
    (with-open [^Watermark wm (.openWatermark wm-src wm-tx)]
      (.allTableColNames scan-emitter wm))))

(defn ->temporal-range [^longs temporal-min-range, ^longs temporal-max-range]
  (let [res (long-array 4)]
    (aset res 0 (aget temporal-max-range temporal/app-time-start-idx))
    (aset res 1 (aget temporal-min-range temporal/app-time-end-idx))
    (aset res 2 (aget temporal-max-range temporal/system-time-start-idx))
    (aset res 3 (aget temporal-min-range temporal/system-time-end-idx))
    res))

(defn temporal-range->temporal-timestamp [^longs temporal-range]
  (let [res (long-array 2)]
    (aset res 0 (aget temporal-range 0))
    (aset res 1 (aget temporal-range 2))
    res))

(defn- point-point-row-picker
  ^java.util.function.IntConsumer [^IRelationWriter out-rel, ^RelationReader leaf-rel
                                   col-names, ^longs temporal-timestamps]

  (let [leaf-row-count (.rowCount leaf-rel)
        iid-rdr (.readerForName leaf-rel "xt$iid")
        sys-from-rdr (.readerForName leaf-rel "xt$system_from")
        op-rdr (.readerForName leaf-rel "op")
        put-rdr (.legReader op-rdr :put)
        doc-rdr (.structKeyReader put-rdr "xt$doc")
        put-valid-from-rdr (.structKeyReader put-rdr "xt$valid_from")
        put-valid-to-rdr (.structKeyReader put-rdr "xt$valid_to")

        delete-rdr (.legReader op-rdr :delete)
        delete-valid-from-rdr (.structKeyReader delete-rdr "xt$valid_from")
        delete-valid-to-rdr (.structKeyReader delete-rdr "xt$valid_to")

        row-copiers (vec
                     (for [col-name col-names
                           :let [normalized-name (util/str->normal-form-str col-name)
                                 ^IVectorReader rdr (case normalized-name
                                                      "_iid" (.readerForName leaf-rel "xt$legacy_iid")
                                                      "xt$system_from" sys-from-rdr
                                                      "xt$system_to" (vr/vec->reader 
                                                                      (doto (NullVector. "xt$system_to")
                                                                        (.setValueCount leaf-row-count)))
                                                      "xt$valid_from" nil
                                                      "xt$valid_to" nil
                                                      (.structKeyReader doc-rdr normalized-name))]
                           :when rdr]
                       (.rowCopier rdr
                                   (case normalized-name
                                     "_iid" (.writerForName out-rel col-name :i64)
                                     "xt$system_from" (.writerForName out-rel col-name types/temporal-col-type)
                                     "xt$system_to" (.writerForName out-rel col-name
                                                                    [:union #{:null types/temporal-col-type}])
                                     (.writerForName out-rel col-name)))))

        valid-from-wtrs (vec
                         (for [col-name col-names
                               :when (= "xt$valid_from" (util/str->normal-form-str col-name))]
                           (.writerForName out-rel col-name types/temporal-col-type)))

        valid-to-wtrs (vec
                       (for [col-name col-names
                             :when (= "xt$valid_to" (util/str->normal-form-str col-name))]
                         (.writerForName out-rel col-name types/temporal-col-type)))

        valid-time (aget temporal-timestamps 0)
        system-time (aget temporal-timestamps 1)

        skip-iid-ptr (ArrowBufPointer.)
        prev-iid-ptr (ArrowBufPointer.)
        current-iid-ptr (ArrowBufPointer.)

        current-bounds (long-array 2)]

    (letfn [(duplicate-ptr [^ArrowBufPointer dst, ^ArrowBufPointer src]
              (.set dst (.getBuf src) (.getOffset src) (.getLength src)))

            (constrain-bounds [^long valid-from, ^long valid-to]
              (when (and (< valid-to valid-time)
                         (< (aget current-bounds 0) valid-to))
                (aset current-bounds 0 valid-to))

              (when (and (> valid-from valid-time)
                         (< valid-from (aget current-bounds 1)))
                (aset current-bounds 1 valid-from)))]

      (reify IntConsumer
        (accept [_ idx]
          (when-not (= skip-iid-ptr (.getPointer iid-rdr idx current-iid-ptr))
            (when-not (= prev-iid-ptr current-iid-ptr)
              (aset current-bounds 0 Long/MIN_VALUE)
              (aset current-bounds 1 Long/MAX_VALUE)
              (duplicate-ptr prev-iid-ptr current-iid-ptr))

            (when (<= (.getLong sys-from-rdr idx) system-time)
              (case (.getLeg op-rdr idx)
                :put
                (let [valid-from (.getLong put-valid-from-rdr idx)
                      valid-to (.getLong put-valid-to-rdr idx)]
                  (if (and (<= valid-from valid-time) (< valid-time valid-to))
                    (do
                      (duplicate-ptr skip-iid-ptr current-iid-ptr)
                      (.startRow out-rel)
                      (doseq [^IRowCopier copier row-copiers]
                        (.copyRow copier idx))
                      (doseq [^IVectorWriter valid-from-wtr valid-from-wtrs]
                        (.writeLong valid-from-wtr (Long/max valid-from (aget current-bounds 0))))
                      (doseq [^IVectorWriter valid-to-wtr valid-to-wtrs]
                        (.writeLong valid-to-wtr (Long/min valid-to (aget current-bounds 1))))
                      (.endRow out-rel))

                    (constrain-bounds valid-from valid-to)))

                :delete
                (let [valid-from (.getLong delete-valid-from-rdr idx)
                      valid-to (.getLong delete-valid-to-rdr idx)]
                  (if (and (<= valid-from valid-time) (< valid-time valid-to))
                    (duplicate-ptr skip-iid-ptr current-iid-ptr)
                    (constrain-bounds valid-from valid-to)))

                :evict
                (duplicate-ptr skip-iid-ptr current-iid-ptr)))))))))

(deftype PointPointCursor [^BufferAllocator allocator, arrow-leaves
                           ^Iterator merge-tasks, ^ints leaf-idxs, ^ints current-arrow-page-idxs
                           col-names, ^Map col-preds, ^longs temporal-timestamps,
                           params]
  ICursor
  (tryAdvance [_ c]
    (if (.hasNext merge-tasks)
      (let [{task-leaves :leaves, :keys [path]} (.next merge-tasks)]
        (with-open [out-rel (vw/->rel-writer allocator)]
          (letfn [(rel->leaf-ptr [leaf-ordinal ^RelationReader log-rdr]
                    (let [row-count (.rowCount log-rdr)
                          iid-rdr (.readerForName log-rdr "xt$iid")
                          picker (point-point-row-picker out-rel log-rdr col-names temporal-timestamps)

                          is-valid-buf (ArrowBufPointer.)]

                      (reify LeafMerge$LeafPointer
                        (getPointer [_ buf]
                          (.getPointer iid-rdr (aget leaf-idxs leaf-ordinal) buf))

                        (getLeafOrdinal [_] leaf-ordinal)

                        (pick [_]
                          (let [leaf-idx (aget leaf-idxs leaf-ordinal)]
                            (.accept picker leaf-idx)
                            (aset leaf-idxs leaf-ordinal (inc leaf-idx))
                            true))

                        (isValid [this]
                          (and (< (aget leaf-idxs leaf-ordinal) row-count)
                               (zero? (HashTrie/compareToPath (.getPointer this is-valid-buf) path)))))))

                  (->leaf-ptr [leaf-ordinal [leaf-tag leaf-arg]]
                    (case leaf-tag
                      :arrow (let [{:keys [leaf-buf ^VectorLoader loader, ^VectorSchemaRoot leaf-root arrow-blocks ^long page-idx]} leaf-arg]
                               (when-not (= page-idx (aget current-arrow-page-idxs leaf-ordinal))
                                 (aset current-arrow-page-idxs leaf-ordinal page-idx)

                                 (with-open [rb (util/->arrow-record-batch-view (nth arrow-blocks page-idx) leaf-buf)]
                                   (.load loader rb)
                                   (aset leaf-idxs leaf-ordinal 0)))

                               (rel->leaf-ptr leaf-ordinal (vr/<-root leaf-root)))

                      :live (let [{:keys [^LiveHashTrie$Leaf leaf, ^ILiveTableWatermark live-table-wm]} leaf-arg]
                              (rel->leaf-ptr leaf-ordinal (.select (.liveRelation live-table-wm) (.data leaf))))))]

            (let [leaf-ptrs (into [] (map-indexed ->leaf-ptr) task-leaves)]
              (LeafMerge/merge leaf-ptrs)

              (.accept c (-> (vw/rel-wtr->rdr out-rel)
                             (vr/with-absent-cols allocator col-names)

                             (as-> rel (reduce (fn [^RelationReader rel, ^IRelationSelector col-pred]
                                                 (.select rel (.select col-pred allocator rel params)))
                                               rel
                                               (vals col-preds))))))))
        true)

      false))

  (close [_]
    (util/close (mapcat (juxt :leaf-buf :leaf-root) arrow-leaves))))

(defn- read-tries [^ObjectStore obj-store, ^IBufferPool buffer-pool, ^String table-name, ^ILiveTableWatermark live-table-wm]
  (let [{trie-files :trie, leaf-files :leaf} (->> (.listObjects obj-store (format "tables/%s/chunks" table-name))
                                                  (keep (fn [file-name]
                                                          (when-let [[_ file-type chunk-idx-str] (re-find #"/(leaf|trie)-c(.+?)\.arrow$" file-name)]
                                                            {:file-name file-name
                                                             :file-type (case file-type "leaf" :leaf, "trie" :trie)
                                                             :chunk-idx chunk-idx-str})))
                                                  (group-by :file-type))
        leaf-files (into {} (map (juxt :chunk-idx identity)) leaf-files)]

    (util/with-close-on-catch [leaf-bufs (ArrayList.)]
      ;; TODO get hold of these a page at a time if it's a small query,
      ;; rather than assuming we'll always have/use the whole file.
      (let [arrow-leaves (->> trie-files
                              (mapv (fn [{:keys [chunk-idx]}]
                                      (let [{:keys [file-name]} (get leaf-files chunk-idx)]
                                        (assert file-name (format "can't find leaf file for chunk '%s'" chunk-idx))
                                        (let [leaf-buf @(.getBuffer buffer-pool file-name)
                                              {:keys [^VectorSchemaRoot root loader arrow-blocks]} (util/read-arrow-buf leaf-buf)]
                                          (.add leaf-bufs leaf-buf)

                                          {:leaf-buf leaf-buf, :leaf-root root, :arrow-blocks arrow-blocks, :loader loader})))))]

        (util/with-open [trie-roots (ArrayList. (count trie-files))]
          (doseq [{:keys [file-name]} trie-files]
            (with-open [^ArrowBuf buf @(.getBuffer buffer-pool file-name)]
              (let [{:keys [^VectorLoader loader root arrow-blocks]} (util/read-arrow-buf buf)]
                (with-open [record-batch (util/->arrow-record-batch-view (first arrow-blocks) buf)]
                  (.load loader record-batch)
                  (.add trie-roots root)))))

          {:arrow-leaves arrow-leaves

           :merge-tasks (vec (for [{:keys [path leaves]} (trie/trie-merge-tasks (cond-> (mapv #(ArrowHashTrie/from %) trie-roots)
                                                                                  live-table-wm (conj (.liveTrie live-table-wm))))]
                               {:path path
                                :leaves (mapv (fn [{:keys [trie-idx leaf]}]
                                                (condp = (class leaf)
                                                  ArrowHashTrie$Leaf [:arrow (-> (nth arrow-leaves trie-idx)
                                                                                 (assoc :page-idx (.getPageIndex ^ArrowHashTrie$Leaf leaf)))]
                                                  LiveHashTrie$Leaf [:live {:leaf leaf, :live-table-wm live-table-wm}]))
                                              leaves)}))})))))

(defn ->4r-cursor [^BufferAllocator allocator, ^ObjectStore obj-store, ^IBufferPool buffer-pool, ^IWatermark wm
                   table-name, col-names, ^longs temporal-range
                   ^Map col-preds, params]
  (let [^ILiveTableWatermark live-table-wm (some-> (.liveIndex wm) (.liveTable table-name))
        {:keys [arrow-leaves ^List merge-tasks]} (read-tries obj-store buffer-pool table-name live-table-wm)]
    (try
      (->PointPointCursor allocator arrow-leaves (.iterator merge-tasks)
                          (int-array (cond-> (count arrow-leaves)
                                       live-table-wm inc))
                          (doto (int-array (count arrow-leaves))
                            (Arrays/fill -1))
                          col-names col-preds
                          (temporal-range->temporal-timestamp temporal-range)
                          params)
      (catch Throwable t
        (util/close (map :leaf-buf arrow-leaves))
        (throw t)))))

(defmethod ig/prep-key ::scan-emitter [_ opts]
  (merge opts
         {:metadata-mgr (ig/ref ::meta/metadata-manager)
          :buffer-pool (ig/ref ::bp/buffer-pool)
          :object-store (ig/ref :xtdb/object-store)}))

(defmethod ig/init-key ::scan-emitter [_ {:keys [^ObjectStore object-store ^IMetadataManager metadata-mgr, ^IBufferPool buffer-pool]}]
  (reify IScanEmitter
    (tableColNames [_ wm table-name]
      (let [normalized-table (util/str->normal-form-str table-name)]
        (into #{} cat [(keys (.columnTypes metadata-mgr normalized-table))
                       (some-> (.liveChunk wm)
                               (.liveTable normalized-table)
                               (.columnTypes)
                               keys)])))

    (allTableColNames [_ wm]
      (merge-with set/union
                  (update-vals (.allColumnTypes metadata-mgr)
                               (comp set keys))
                  (update-vals (some-> (.liveChunk wm)
                                       (.allColumnTypes))
                               (comp set keys))))

    (scanColTypes [_ wm scan-cols]
      (letfn [(->col-type [[table col-name]]
                (let [normalized-table (util/str->normal-form-str (str table))
                      normalized-col-name (util/str->normal-form-str (str col-name))]
                  (if (temporal/temporal-column? (util/str->normal-form-str (str col-name)))
                    [:timestamp-tz :micro "UTC"]
                    (types/merge-col-types (.columnType metadata-mgr normalized-table normalized-col-name)
                                           (some-> (.liveChunk wm)
                                                   (.liveTable normalized-table)
                                                   (.columnTypes)
                                                   (get normalized-col-name))))))]
        (->> scan-cols
             (into {} (map (juxt identity ->col-type))))))

    (emitScan [_ {:keys [columns], {:keys [table for-valid-time] :as scan-opts} :scan-opts} scan-col-types param-types]
      (let [col-names (->> columns
                           (into [] (comp (map (fn [[col-type arg]]
                                                 (case col-type
                                                   :column arg
                                                   :select (key (first arg)))))

                                          (distinct))))

            {content-col-names false, temporal-col-names true}
            (->> col-names (group-by (comp temporal/temporal-column? util/str->normal-form-str str)))

            content-col-names (-> (set (map str content-col-names)) (conj "_row_id"))
            normalized-content-col-names (set (map (comp util/str->normal-form-str) content-col-names))
            temporal-col-names (into #{} (map (comp str)) temporal-col-names)
            normalized-table-name (util/str->normal-form-str (str table))

            col-types (->> col-names
                           (into {} (map (juxt identity
                                               (fn [col-name]
                                                 (get scan-col-types [table col-name]))))))

            selects (->> (for [[tag arg] columns
                               :when (= tag :select)]
                           (first arg))
                         (into {}))

            col-preds (->> (for [[col-name select-form] selects]
                             ;; for temporal preds, we may not need to re-apply these if they can be represented as a temporal range.
                             (MapEntry/create (str col-name)
                                              (expr/->expression-relation-selector select-form {:col-types col-types, :param-types param-types})))
                           (into {}))

            metadata-args (vec (for [[col-name select] selects
                                     :when (not (temporal/temporal-column? (util/str->normal-form-str (str col-name))))]
                                 select))

            row-count (->> (meta/with-all-metadata metadata-mgr normalized-table-name
                             (util/->jbifn
                               (fn [_chunk-idx ^ITableMetadata table-metadata]
                                 (let [id-col-idx (.rowIndex table-metadata "xt$id" -1)
                                       ^BigIntVector count-vec (-> (.metadataRoot table-metadata)
                                                                   ^ListVector (.getVector "columns")
                                                                   ^StructVector (.getDataVector)
                                                                   (.getChild "count"))]
                                   (.get count-vec id-col-idx)))))
                           (reduce +))]

        {:col-types col-types
         :stats {:row-count row-count}
         :->cursor (fn [{:keys [allocator, ^IWatermark watermark, basis, params default-all-valid-time?]}]
                     (let [metadata-pred (expr.meta/->metadata-selector (cons 'and metadata-args) col-types params)
                           scan-opts (cond-> scan-opts
                                       (nil? for-valid-time)
                                       (assoc :for-valid-time (if default-all-valid-time? [:all-time] [:at [:now :now]])))
                           [temporal-min-range temporal-max-range] (->temporal-min-max-range params basis scan-opts selects)]
                       (if (at-point-point? scan-opts)
                         (->4r-cursor allocator object-store buffer-pool
                                      watermark
                                      normalized-table-name
                                      (set/union content-col-names temporal-col-names)
                                      (->temporal-range temporal-min-range temporal-max-range)
                                      col-preds
                                      params)

                         (let [current-row-ids (when (use-current-row-id-cache? watermark scan-opts basis temporal-col-names)
                                                 (get-current-row-ids watermark basis))]
                           (-> (ScanCursor. allocator metadata-mgr watermark
                                            content-col-names temporal-col-names col-preds
                                            temporal-min-range temporal-max-range current-row-ids
                                            (util/->concat-cursor (->content-chunks allocator metadata-mgr buffer-pool
                                                                                    normalized-table-name normalized-content-col-names
                                                                                    metadata-pred)
                                                                  (some-> (.liveChunk watermark)
                                                                          (.liveTable normalized-table-name)
                                                                          (.liveBlocks normalized-content-col-names metadata-pred)))
                                            params)
                               (coalesce/->coalescing-cursor allocator))))))}))))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter scan-col-types, param-types]}]
  (.emitScan scan-emitter scan-expr scan-col-types param-types))
