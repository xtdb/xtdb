(ns xtdb.operator.scan
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.bloom :as bloom]
            [xtdb.buffer-pool :as bp]
            [xtdb.coalesce :as coalesce]
            [xtdb.expression :as expr]
            [xtdb.expression.comparator :as cmp]
            [xtdb.expression.metadata :as expr.meta]
            [xtdb.expression.walk :as expr.walk]
            xtdb.indexer.live-index
            [xtdb.logical-plan :as lp]
            [xtdb.metadata :as meta]
            [xtdb.temporal :as temporal]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            xtdb.watermark)
  (:import (clojure.lang IPersistentSet MapEntry PersistentHashSet PersistentVector)
           (java.util ArrayList HashMap Iterator LinkedList List Map PriorityQueue Queue Set)
           (java.util.function BiFunction Consumer)
           [java.util.stream IntStream]
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector BigIntVector TimeStampMicroTZVector VarBinaryVector VectorLoader VectorSchemaRoot)
           (org.apache.arrow.vector.complex ListVector StructVector)
           (org.apache.arrow.vector.ipc.message ArrowFooter ArrowRecordBatch)
           (org.roaringbitmap IntConsumer RoaringBitmap)
           org.roaringbitmap.buffer.MutableRoaringBitmap
           (org.roaringbitmap.longlong Roaring64Bitmap)
           xtdb.api.protocols.TransactionInstant
           xtdb.buffer_pool.IBufferPool
           xtdb.ICursor
           (xtdb.indexer.live_index ILiveTableWatermark)
           (xtdb.metadata IMetadataManager ITableMetadata)
           (xtdb.object_store ObjectStore)
           xtdb.operator.IRelationSelector
           [xtdb.trie TrieKeys ArrowHashTrie ArrowHashTrie$Leaf LiveHashTrie LiveHashTrie$Leaf]
           (xtdb.vector IVectorReader RelationReader)
           (xtdb.vector IRowCopier)
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

(defn- scan-op-point? [scan-op]
  (= :at (first scan-op)))

(defn- at-valid-time-point? [{:keys [for-valid-time for-system-time]}]
  (and (or (nil? for-valid-time)
           (scan-op-point? for-valid-time))
       (or (nil? for-system-time)
           (scan-op-at-now for-system-time))))

(defn use-4r? [^IWatermark watermark scan-opts basis]
  (and
   (.txBasis watermark) (= (:tx basis)
                           (.txBasis watermark))
   (at-valid-time-point? scan-opts) (>= (util/instant->micros (:current-time basis))
                                        (util/instant->micros (:system-time (:tx basis))))))

(defn ->temporal-range [^longs temporal-min-range, ^longs temporal-max-range]
  (let [res (long-array 4)]
    (aset res 0 (aget temporal-max-range temporal/app-time-start-idx))
    (aset res 1 (aget temporal-min-range temporal/app-time-end-idx))
    (aset res 2 (aget temporal-max-range temporal/system-time-start-idx))
    (aset res 1 (aget temporal-min-range temporal/system-time-end-idx))
    res))

(defn ->constant-time-stamp-vec [^BufferAllocator allocator name length]
  (let [res (doto (TimeStampMicroTZVector. (types/col-type->field name types/temporal-col-type) allocator)
              (.setInitialCapacity length)
              (.setValueCount length))]
    (dotimes [i length]
      (.set res i 1 util/end-of-time-μs))
    res))

(deftype ValidPointTrieCursor [^BufferAllocator allocator,
                               ^ICursor trie-bucket-cursor,
                               ^longs temporal-range,
                               ^PersistentHashSet col-names
                               ^Map col-preds
                               params]
  ICursor
  (tryAdvance [_ c]
    (.tryAdvance trie-bucket-cursor
                 (reify Consumer
                   (accept [_ block-rel-rdr]
                     (let [^RelationReader block-rel-rdr block-rel-rdr
                           !selection-vec (IntStream/builder)
                           iid-col (.readerForName block-rel-rdr "xt$iid")
                           op-col (.readerForName block-rel-rdr "op")
                           put-vec (.legReader op-col :put #_(byte 0))
                           delete-vec (.legReader op-col :delete #_(byte 1))
                           doc-vec (.structKeyReader put-vec "xt$doc" )
                           valid-time (aget temporal-range 0)
                           put-valid-from-vec (.structKeyReader put-vec "xt$valid_from")
                           put-valid-to-vec (.structKeyReader put-vec "xt$valid_to")
                           delete-valid-from-vec (.structKeyReader delete-vec "xt$valid_from")
                           delete-valid-to-vec (.structKeyReader delete-vec "xt$valid_to")
                           cmp (cmp/->comparator iid-col iid-col :nulls-last)
                           !new (volatile! true)]
                       (dotimes [idx (.rowCount block-rel-rdr)]
                         ;; new iid
                         (when (and (pos? idx) (not (zero? (.applyAsInt cmp (dec idx) idx))))
                           (vreset! !new true))
                         (when @!new
                           (case (.getTypeId op-col idx)
                             ;; a put won
                             0 (when (and ;; TODO one check might be enough here ?
                                      (<= (.getLong put-valid-from-vec idx) valid-time)
                                      (<= valid-time (.getLong put-valid-to-vec idx)))
                                 (vreset! !new false)
                                 (.add !selection-vec idx))
                             ;; a delete won
                             1 (when (and ;; TODO one check might be enough here ?
                                      (<= (.getLong delete-valid-from-vec idx) valid-time)
                                      (<= valid-time (.getLong delete-valid-to-vec idx)))
                                 (vreset! !new false))
                             ;; TODO evict
                             (throw (ex-info "Should not happen!" {:idx idx})))))
                       (let [selection (.toArray (.build !selection-vec))]
                         (with-open [^RelationReader res
                                     (as-> (vr/rel-reader
                                            (->> (for [col-name col-names
                                                       :let [normalized-name (util/str->normal-form-str col-name)]]
                                                   (some-> (cond
                                                             (= normalized-name "xt$system_from")
                                                             (.select (.readerForName block-rel-rdr "xt$system_from") selection)

                                                             ;; FIXME - hack for now
                                                             (= normalized-name "xt$system_to")
                                                             (vr/vec->reader (->constant-time-stamp-vec allocator "xt$system_to" (alength selection)))

                                                             (temporal/temporal-column? normalized-name)
                                                             (.select (.structKeyReader put-vec normalized-name) selection)

                                                             :else
                                                             (when (contains? (.structKeys doc-vec) normalized-name)
                                                               (.select (.structKeyReader doc-vec normalized-name) selection)))
                                                           (.withName col-name)))
                                                 (filter some?))
                                            (alength selection)) rel
                                           (vr/with-absent-cols rel allocator col-names)
                                           (reduce (fn [^RelationReader rel, ^IRelationSelector col-pred]
                                                     (.select rel (.select col-pred allocator rel params)))
                                                   rel
                                                   (vals col-preds)))]
                           (.accept c res)
                           true)))))))
  (close [_]
    (util/close trie-bucket-cursor)))

(deftype LogTriePageCursor [^ArrowBuf buf,
                            ^Queue page-identifiers
                            ^List record-batches
                            ^VectorSchemaRoot root
                            ^VectorLoader loader
                            ^:unsynchronized-mutable ^ArrowRecordBatch current-batch
                            ^int trie-idx]
  ICursor
  (tryAdvance [this c]
    (when current-batch
      (util/try-close current-batch)
      (set! (.current-batch this) nil))

    (if-let [{:keys [path page-idx]} (.poll page-identifiers)]
      (let [record-batch (util/->arrow-record-batch-view (.get record-batches page-idx) buf)]
        (set! (.current-batch this) record-batch)
        (.load loader record-batch)
        (.accept c {:path path
                    :page-rel (vr/<-root root)
                    :position 0
                    :trie-idx trie-idx})
        true)
      false))

  (close [_]
    (when current-batch
      (util/try-close current-batch))
    (util/try-close root)
    (util/try-close buf)))

;; page identifier here is a map of {:path ... :page-index ...}
(defn ->log-trie-page-cursor ^xtdb.ICursor
  [^IBufferPool buffer-pool, leaf-filename, ^PersistentVector page-identifiers, trie-idx]
  (let [^ArrowBuf buf @(.getBuffer buffer-pool leaf-filename)
        footer (util/read-arrow-footer buf)
        root (VectorSchemaRoot/create (.getSchema footer) (.getAllocator (.getReferenceManager buf)))]
    (LogTriePageCursor. buf
                        (LinkedList. page-identifiers)
                        (.getRecordBatches footer)
                        root
                        (VectorLoader. root #_CommonsCompressionFactory/INSTANCE)
                        nil
                        trie-idx)))

(deftype LiveTriePageCursor [^RelationReader live-relation,
                             ^Queue page-identifiers,
                             ^int trie-idx]
  ICursor
  (tryAdvance [_ c]
    (if-let [{:keys [path selection]} (.poll page-identifiers)]
      (let [rel (.select live-relation selection)]
        (.accept c {:path path
                    :page-rel rel
                    :position 0
                    :trie-idx trie-idx})
        true)
      false))

  (close [_]))

;; page identifier here is a map of {:path ... :selection ...}
(defn ->live-trie-page-cursor ^xtdb.ICursor [^RelationReader live-relation ^PersistentVector page-identifiers, trie-idx]
  (LiveTriePageCursor. live-relation (LinkedList. page-identifiers) trie-idx))


(defn load-leaf-vsr ^RelationReader [^ArrowBuf leaf-buf, ^VectorSchemaRoot leaf-vsr,
                                     ^ArrowFooter leaf-footer, page-idx]
  (with-open [leaf-record-batch (util/->arrow-record-batch-view (.get (.getRecordBatches leaf-footer) page-idx) leaf-buf)]
    ;; TODO could the vector-loader be repurposed
    (.load (VectorLoader. leaf-vsr) leaf-record-batch)
    (vr/<-root leaf-vsr)))

;; only for debugging for now
(defn print-leaf-paths [^IBufferPool buffer-pool, trie-filename leaf-filename]
  (with-open [^ArrowBuf trie-buf @(.getBuffer buffer-pool trie-filename)
              ^ArrowBuf leaf-buf @(.getBuffer buffer-pool leaf-filename)]
    (let [trie-footer (util/read-arrow-footer trie-buf)
          leaf-footer (util/read-arrow-footer leaf-buf)]
      (with-open [^VectorSchemaRoot  trie-batch (VectorSchemaRoot/create (.getSchema trie-footer)
                                                                         (.getAllocator (.getReferenceManager trie-buf)))
                  trie-record-batch (util/->arrow-record-batch-view (first (.getRecordBatches trie-footer)) trie-buf)
                  ^VectorSchemaRoot  leaf-batch (VectorSchemaRoot/create (.getSchema leaf-footer)
                                                                         (.getAllocator (.getReferenceManager leaf-buf)))]

        (let [iid-vec (.getVector leaf-batch "xt$iid")]
          (.load (VectorLoader. trie-batch) trie-record-batch)
          (->> (ArrowHashTrie/from trie-batch)
               (.rootNode)
               (.leaves)
               (mapv (fn [^ArrowHashTrie$Leaf leaf]
                       (with-open [leaf-record-batch (util/->arrow-record-batch-view (.get (.getRecordBatches leaf-footer)
                                                                                           (.getPageIndex leaf)) leaf-buf)]

                         (.load (VectorLoader. leaf-batch) leaf-record-batch))
                       {:path (seq (.path leaf))
                        :iids (->> (range 0 (.getValueCount iid-vec))
                                   (mapv #(util/bytes->uuid (.getObject iid-vec %))))}))))))))

(defn calc-leaf-paths [^IBufferPool buffer-pool, trie-filename]
  (with-open [^ArrowBuf trie-buf @(.getBuffer buffer-pool trie-filename)]
    (let [trie-footer (util/read-arrow-footer trie-buf)]
      (with-open [^VectorSchemaRoot  trie-batch (VectorSchemaRoot/create (.getSchema trie-footer)
                                                                         (.getAllocator (.getReferenceManager trie-buf)))
                  trie-record-batch (util/->arrow-record-batch-view (first (.getRecordBatches trie-footer)) trie-buf)]

        (.load (VectorLoader. trie-batch) trie-record-batch)
        (->> (ArrowHashTrie/from trie-batch)
             (.rootNode)
             (.leaves)
             (map (fn [^ArrowHashTrie$Leaf leaf] (conj (vec (.path leaf)) [(.getPageIndex leaf)])))
             ^PersistentVector vec
             (.iterator))))))

(defn calc-trie-log-page-identifiers [^IBufferPool buffer-pool, trie-filename]
  (with-open [^ArrowBuf trie-buf @(.getBuffer buffer-pool trie-filename)]
    (let [trie-footer (util/read-arrow-footer trie-buf)]
      (with-open [^VectorSchemaRoot  trie-batch (VectorSchemaRoot/create (.getSchema trie-footer)
                                                                         (.getAllocator (.getReferenceManager trie-buf)))
                  trie-record-batch (util/->arrow-record-batch-view (first (.getRecordBatches trie-footer)) trie-buf)]

        (.load (VectorLoader. trie-batch) trie-record-batch)
        (->> (ArrowHashTrie/from trie-batch)
             (.rootNode)
             (.leaves)
             (mapv (fn [^ArrowHashTrie$Leaf leaf] {:path (.path leaf) :page-idx (.getPageIndex leaf)})))))))


;; FIXME look at trieKeys.compareToPath
(defn path->bytes ^bytes [path]
  (assert (every? #(<= 0 % 0xf) path))
  (->> (partition-all 2 path)
       (map (fn [[high low]]
              (bit-or (bit-shift-left high 4) (or low 0))))
       byte-array))

(defn bytes->path [bytes]
  (mapcat #(list (mod (bit-shift-right % 4) 16)
                 (bit-and % (dec (bit-shift-left 1 4))))
          bytes))

#_(defn compare-paths [path1 path2]
    (cond (and (nil? path1) (nil? path2)) 0
          (nil? path1) -1
          (nil? path2) 1
          :else
          (if-let [res (->> (map - path1 path2)
                            (drop-while zero?)
                            first)]
            res
            (- (count path2) (count path1)))))
(do
  (defn compare-paths [path1 path2]
    (loop [p1 (seq path1) p2 (seq path2)]
      (cond (and (empty? p1) (empty? p2)) 0
            (empty? p1) -1
            (empty? p2) 1
            :else (let [res (Integer/compare (first p1) (first p2))]
                    (if-not (zero? res)
                      res
                      (recur (rest p1) (rest p2)))))))

  (defn sub-path?
    "returns true if p1 is a subpath of p2."
    [p1 p2]
    (nat-int? (compare-paths p2 p1)))

  (defn uuid-byte-prefix? [path bytes]
    (if (nil? path)
      true
      (= path (take (count path) (bytes->path bytes)))))

  (defn- byte-buffer->bytes [^java.nio.ByteBuffer bb]
    (let [res (byte-array (.remaining bb))]
      (.get bb res)
      res))

  (def null-or-neg-int? (complement pos-int?))

  ;; TODO use more info about the sorting of the relations
  (defn merge-page-rels [^BufferAllocator allocator page-rels page-identifiers]
    (let [path (:path (last page-identifiers))
          page-positions (int-array (map :position page-identifiers))
          rel-wtr (vw/->strict-rel-writer allocator)
          rel-cnt (count page-rels)
          cmps (HashMap.)
          trie-idxs (map :trie-idx page-identifiers)
          trie-idx->idx (zipmap (range) trie-idxs)
          iid-vecs (object-array (map #(.readerForName ^RelationReader % "xt$iid") page-rels))
          copiers (object-array (map #(.rowCopier rel-wtr %) page-rels))
          prio (PriorityQueue. ^java.util.Comparator
                               (comparator (fn [[rel-idx1 idx1] [rel-idx2 idx2]]
                                             (let [res (.applyAsInt ^java.util.function.IntBinaryOperator
                                                                    (.get cmps [rel-idx1 rel-idx2]) idx1 idx2)]
                                               (or (neg? res)
                                                   (and (zero? res) (> (trie-idx->idx rel-idx1)
                                                                       (trie-idx->idx rel-idx2))))))))]
      (doseq [i (range rel-cnt)
              j (range i)]
        ;; FIXME quick hack
        (.put cmps [i j] (cmp/->comparator (aget iid-vecs i) (aget iid-vecs j) :nulls-last))
        (.put cmps [j i] (cmp/->comparator (aget iid-vecs j) (aget iid-vecs i) :nulls-last)))
      (doseq [i (range rel-cnt)]
        (when (pos? (.valueCount ^IVectorReader (aget iid-vecs i)))
          (.add prio [i (aget page-positions i)])))

      ;; TODO we don't need to put in the element every time
      ;; could wait until we hit an item larger than the second one
      (let [^ints new-page-positions (int-array (repeat rel-cnt -1))]
        (loop []
          (when (not (.isEmpty prio))
            (let [[rel-idx ^int idx] (.poll prio)
                  new-idx (inc idx)]
              (if (null-or-neg-int? (TrieKeys/compareToPath (.getPointer ^IVectorReader (aget iid-vecs rel-idx) idx) path))
                (do
                  (.copyRow ^IRowCopier (aget copiers rel-idx) idx)
                  (when (< new-idx (.valueCount ^IVectorReader (aget iid-vecs rel-idx)))
                    (.add prio [rel-idx new-idx]))
                  (recur))
                (aset new-page-positions rel-idx idx)))))
        (while (not (.isEmpty prio))
          (let [[rel-idx ^int idx] (.poll prio)]
            (aset new-page-positions rel-idx idx)))
        ;; TODO can this be better maintained
        [(vw/rel-wtr->rdr rel-wtr) new-page-positions])))

  ;; indices in page-cursors maps to trie-idx of page-identifier
  (deftype TrieBucketCursor [^BufferAllocator allocator,
                             ^ArrayList page-cursors
                             ^PriorityQueue pq]
    ICursor
    (tryAdvance [_ c]
      (if-not (.isEmpty pq)
        (let [smallest-page-identifier (.poll pq)
              page-identifiers (doto (ArrayList.) (.add smallest-page-identifier))]
          (while (and (not (.isEmpty pq))
                      (sub-path? (:path (.get page-identifiers (dec (.size page-identifiers)))) (:path (.peek pq))))
            (.add page-identifiers (.poll pq)))


          ;; setting up merging + merging
          (let [page-rels (mapv :page-rel page-identifiers)
                ;; if new-page-position is -1 the page was finished
                [^RelationReader block-rel new-page-positions] (merge-page-rels allocator page-rels page-identifiers)]

            (try
              ;; get a new page or not
              (doseq [[idx new-position] (map-indexed vector new-page-positions)]
                (if (nat-int? new-position)
                  (.add pq (assoc (.get page-identifiers idx) :position new-position))

                  ;; FIXME breaking RAII

                  (.tryAdvance ^ICursor (.get page-cursors (:trie-idx (.get page-identifiers idx)))
                               (reify Consumer
                                 (accept [_ page-identifier]
                                   (.add pq page-identifier))))))
              (.accept c block-rel)
              true
              (finally (.close block-rel)))))
        false))

    (close [_]
      (run! util/close page-cursors)))


  ;; filenames is a list of [trie-filename leaf-filename]
  (defn ->trie-bucket-cursor ^xtdb.ICursor [^BufferAllocator allocator, ^IBufferPool buffer-pool, filenames
                                            ^RelationReader live-relation, ^LiveHashTrie live-trie]
    ;; (clojure.pprint/pprint (map #(print-leaf-paths buffer-pool (first %) (second %)) filenames))

    (let [nb-log-tries (count filenames)
          log-trie-page-identifiers (mapv (comp #(calc-trie-log-page-identifiers buffer-pool %) first) filenames)
          log-trie-page-cursors (mapv #(->log-trie-page-cursor buffer-pool (second %1) %2 %3)
                                      filenames
                                      log-trie-page-identifiers
                                      (range nb-log-tries))

          ;; _ (mapv #(.close %) log-trie-page-cursors)

          live-trie-page-identifiers (some->> live-trie (.compactLogs) (.rootNode) (.leaves)
                                              (mapv (fn [^LiveHashTrie$Leaf leaf]
                                                      {:path (.path leaf) :selection (.data leaf)})))
          live-trie-page-cursor (when live-trie-page-identifiers
                                  (->live-trie-page-cursor live-relation live-trie-page-identifiers nb-log-tries))

          ;; _ (when live-trie-page-cursor (.close live-trie-page-cursor))

          pq (PriorityQueue. (fn [{path1 :path} {path2 :path}] (compare-paths path1 path2)))

          page-cursors (ArrayList. ^PersistentVector (vec (cond-> log-trie-page-cursors
                                                            live-trie-page-cursor (conj live-trie-page-cursor))))]
      (doseq [^ICursor page-cursor page-cursors]
        ;;FIXME breaking RAII
        (.tryAdvance page-cursor
                     (reify Consumer
                       (accept [_ page-identifier]
                         (.add pq page-identifier)))))
      (TrieBucketCursor. allocator page-cursors pq)))


  (defn- table-names->filenames [^ObjectStore object-store, table-name]
    (let [table-dir (format "tables/%s/chunks/" table-name)
          objects (.listObjects object-store (format "tables/%s/chunks/" table-name))
          trie-files (-> (filter #(re-matches (re-pattern (str table-dir "trie.*")) %) objects) sort reverse)
          leaf-files (-> (filter #(re-matches (re-pattern (str table-dir "leaf.*")) %) objects) sort reverse)]
      (map vector trie-files leaf-files)))

  ;; TODO object-store to be replaced with buffer pool as soon as metadata-mgr
  ;; knows for which chunk a trie is available (for this table)
  (defn- ->4r-cursor [^BufferAllocator allocator, ^ObjectStore object-store,
                      ^IBufferPool buffer-pool, ^ILiveTableWatermark wm,
                      table-name, col-names, ^longs temporal-range
                      ^Map col-preds, params, scan-opts, _basis]
    (let [filenames (table-names->filenames object-store table-name)
          live-relation (some-> wm (.liveRelation))
          trie-bucket-curser (->trie-bucket-cursor allocator buffer-pool filenames
                                                   live-relation (some-> wm (.liveTrie)))]
      (cond
        ;; TODO if knowledge of no future data is available this can be optimized further
        (at-now? scan-opts)
        (ValidPointTrieCursor. allocator trie-bucket-curser temporal-range col-names col-preds params)

        (at-valid-time-point? scan-opts)
        (ValidPointTrieCursor. allocator trie-bucket-curser temporal-range col-names col-preds params)

        :else (throw (ex-info "TODO - invalid 4r option" {}))))))

(defn tables-with-cols [basis ^IWatermarkSource wm-src ^IScanEmitter scan-emitter]
  (let [{:keys [tx, after-tx]} basis
        wm-tx (or tx after-tx)]
    (with-open [^Watermark wm (.openWatermark wm-src wm-tx)]
      (.allTableColNames scan-emitter wm))))

(defmethod ig/prep-key ::scan-emitter [_ opts]
  (merge opts
         {:metadata-mgr (ig/ref ::meta/metadata-manager)
          :buffer-pool (ig/ref ::bp/buffer-pool)
          :object-store (ig/ref :xtdb/object-store)}))

(defmethod ig/init-key ::scan-emitter [_ {:keys [^IMetadataManager metadata-mgr, ^IBufferPool buffer-pool
                                                 ^ObjectStore object-store]}]
  (reify IScanEmitter
    (tableColNames [_ wm table-name]
      (let [normalized-table (util/str->normal-form-str table-name)]
        (into #{} cat [(keys (.columnTypes metadata-mgr normalized-table))
                       (some-> (.liveChunk wm)
                               (.liveTable normalized-table)
                               (.columnTypes)
                               keys)])))

    (allTableColNames [_ wm]
      (merge-with
       set/union
       (update-vals
        (.allColumnTypes metadata-mgr)
        (comp set keys))
       (update-vals
        (some-> (.liveChunk wm)
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
                           [temporal-min-range temporal-max-range] (->temporal-min-max-range params basis scan-opts selects)
                           current-row-ids (when (use-current-row-id-cache? watermark scan-opts basis temporal-col-names)
                                             (get-current-row-ids watermark basis))]
                       (->
                        (if (use-4r? watermark scan-opts basis)
                          (->4r-cursor allocator object-store buffer-pool
                                       (some-> (.liveIndex watermark) (.liveTable normalized-table-name))
                                       normalized-table-name
                                       (-> (set/union content-col-names temporal-col-names)
                                           (disj "_row_id"))
                                       (->temporal-range temporal-min-range temporal-max-range)
                                       col-preds
                                       params
                                       scan-opts
                                       basis)

                          (ScanCursor. allocator metadata-mgr watermark
                                       content-col-names temporal-col-names col-preds
                                       temporal-min-range temporal-max-range current-row-ids
                                       (util/->concat-cursor (->content-chunks allocator metadata-mgr buffer-pool
                                                                               normalized-table-name normalized-content-col-names
                                                                               metadata-pred)
                                                             (some-> (.liveChunk watermark)
                                                                     (.liveTable normalized-table-name)
                                                                     (.liveBlocks normalized-content-col-names metadata-pred)))
                                       params))
                        (coalesce/->coalescing-cursor allocator))))}))))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter scan-col-types, param-types]}]
  (.emitScan scan-emitter scan-expr scan-col-types param-types))
