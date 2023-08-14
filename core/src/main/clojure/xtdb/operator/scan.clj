(ns xtdb.operator.scan
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.buffer-pool :as bp]
            [xtdb.expression :as expr]
            [xtdb.expression.metadata :as expr.meta]
            xtdb.indexer.live-index
            [xtdb.logical-plan :as lp]
            [xtdb.metadata :as meta]
            xtdb.object-store
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            xtdb.watermark)
  (:import (clojure.lang IPersistentMap MapEntry )
           (java.util ArrayList Arrays Iterator LinkedList List Map ListIterator)
           (java.util.function IntConsumer)
           org.apache.arrow.memory.ArrowBuf
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.memory.util ArrowBufPointer]
           (org.apache.arrow.vector VectorLoader VectorSchemaRoot)
           xtdb.api.protocols.TransactionInstant
           xtdb.buffer_pool.IBufferPool
           xtdb.ICursor
           xtdb.indexer.live_index.ILiveTableWatermark
           (xtdb.metadata IMetadataManager)
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

#_ ; TODO reinstate pushdown blooms
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

(defn- ->range ^longs []
  (let [res (long-array 8)]
    (doseq [i (range 0 8 2)]
      (aset res i Long/MIN_VALUE)
      (aset res (inc i) Long/MAX_VALUE))
    res))

(def ^:private column->idx {"xt$valid_from" 0
                            "xt$valid_to" 1
                            "xt$system_from" 2
                            "xt$system_to" 3})

(defn- ->temporal-column-idx ^long [col-name]
  (long (get column->idx (name col-name))))

(def ^:const ^int valid-from-lower-idx 0)
(def ^:const ^int valid-from-upper-idx 1)
(def ^:const ^int valid-to-lower-idx 2)
(def ^:const ^int valid-to-upper-idx 3)
(def ^:const ^int system-from-lower-idx 4)
(def ^:const ^int system-from-upper-idx 5)
(def ^:const ^int system-to-lower-idx 6)
(def ^:const ^int system-to-upper-idx 7)

(defn- ->temporal-range [^RelationReader params, {^TransactionInstant basis-tx :tx}, {:keys [for-valid-time for-system-time]}]
  (let [range (->range)]
    (letfn [(apply-bound [f col-name ^long time-μs]
              (let [range-idx-lower (* (->temporal-column-idx (util/str->normal-form-str (str col-name))) 2)
                    range-idx-upper (inc range-idx-lower)]
                (case f
                  :< (aset range range-idx-upper
                           (min (dec time-μs) (aget range range-idx-upper)))
                  :<= (aset range range-idx-upper
                            (min time-μs (aget range range-idx-upper)))
                  :> (aset range range-idx-lower
                           (max (inc time-μs) (aget range range-idx-lower)))
                  :>= (aset range range-idx-lower
                            (max time-μs (aget range range-idx-lower)))
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
        (apply-constraint for-system-time "xt$system_from" "xt$system_to")))
    range))

(defn- scan-op-point? [scan-op]
  (= :at (first scan-op)))

(defn- at-point-point? [{:keys [for-valid-time for-system-time]}]
  (and (or (nil? for-valid-time)
           (scan-op-point? for-valid-time))
       (or (nil? for-system-time)
           (scan-op-point? for-system-time))))

(defn- at-range-point? [{:keys [for-system-time]}]
  (or (nil? for-system-time)
      (scan-op-point? for-system-time)))

(defn tables-with-cols [basis ^IWatermarkSource wm-src ^IScanEmitter scan-emitter]
  (let [{:keys [tx, after-tx]} basis
        wm-tx (or tx after-tx)]
    (with-open [^Watermark wm (.openWatermark wm-src wm-tx)]
      (.allTableColNames scan-emitter wm))))

;; As the algorithm processes events in reverse system time order, one can
;; immediately write out the system-to times when having finished an event.
;; The system-to times are not relevant for processing earlier events.
(deftype Rectangle [^long valid-from, ^long valid-to, ^long sys-from])

(defn- ranges-invariant [^LinkedList !ranges]
  (every? true? (map (fn [^Rectangle r1 ^Rectangle r2] (<= (.valid-to r1) (.valid-from r2)))
                     !ranges (rest !ranges))))


(definterface RowConsumer
  (^void accept [^int idx, ^long validFrom, ^long validTo, ^long systemFrom, ^long systemTo]))

(definterface EventResolver
  (^void resolveEvent [^int idx, ^long validFrom, ^long validTo, ^long systemFrom
                       ^xtdb.operator.scan.RowConsumer rowConsumer])
  (^void nextIid []))

(defn event-resolver
  (^xtdb.operator.scan.EventResolver [range-range?] (event-resolver range-range? (LinkedList.)))

  (^xtdb.operator.scan.EventResolver [range-range?, ^List !ranges]
   (let [!overlapping-ranges (LinkedList.)]
     (reify EventResolver
       (nextIid [_]
         (.clear !ranges))

       (resolveEvent [_ idx valid-from valid-to system-from rc]
         (assert (ranges-invariant !ranges))

         (.clear !overlapping-ranges)

         (let [^ListIterator itr (.iterator !ranges)
               ^Rectangle cur (loop []
                                (when (.hasNext itr)
                                  (let [^Rectangle next (.next itr)]
                                    (if (<= (.valid-to next) valid-from)
                                      (recur)
                                      next))))]
           (loop [^Rectangle cur cur]
             (when cur
               (if (< (.valid-from cur) valid-to)
                 (do
                   (.add !overlapping-ranges cur)
                   (.remove itr)
                   (recur (when (.hasNext itr) (.next itr))))
                 (when (<= valid-to (.valid-from cur))
                   (.previous itr)))))

           (when-let [^Rectangle begin (first !overlapping-ranges)]
             (when (< (.valid-from begin) valid-from)
               (.add itr (Rectangle. (.valid-from begin) valid-from (.sys-from begin))))
             (when (and rc (< valid-from (.valid-from begin)))
               (.accept rc idx valid-from (.valid-from begin) system-from util/end-of-time-μs)))

           (when rc
             (if (seq !overlapping-ranges)
               (do
                 (dorun (map (fn [^Rectangle r1 ^Rectangle r2]
                               (when (< (.valid-to r1) (.valid-from r2))
                                 (.accept rc idx (.valid-to r1) (.valid-from r2) system-from util/end-of-time-μs)))
                             !overlapping-ranges (rest !overlapping-ranges)))

                 (when range-range?
                   (doseq [^Rectangle r !overlapping-ranges]
                     (let [new-valid-from (max valid-from (.valid-from r))
                           new-valid-to (min valid-to (.valid-to r))]
                       (when (< new-valid-from new-valid-to)
                         (.accept rc idx new-valid-from new-valid-to system-from (.sys-from r)))))))

               (.accept rc idx valid-from valid-to system-from util/end-of-time-μs)))

           (.add itr (Rectangle. valid-from valid-to system-from))

           (when-let [^Rectangle end (last !overlapping-ranges)]
             (when (< valid-to (.valid-to end))
               (.add itr (Rectangle. valid-to (.valid-to end) (.sys-from end))))
             (when (and rc (< (.valid-to end) valid-to))
               (.accept rc idx (.valid-to end) valid-to system-from util/end-of-time-μs)))))))))

(defn- duplicate-ptr [^ArrowBufPointer dst, ^ArrowBufPointer src]
  (.set dst (.getBuf src) (.getOffset src) (.getLength src)))

(defn- copy-row-consumer [^IRelationWriter out-rel, ^RelationReader leaf-rel, col-names]
  (let [op-rdr (.readerForName leaf-rel "op")
        put-rdr (.legReader op-rdr :put)
        doc-rdr (.structKeyReader put-rdr "xt$doc")

        row-copiers (vec
                     (for [col-name col-names
                           :let [normalized-name (util/str->normal-form-str col-name)
                                 ^IVectorReader rdr (case normalized-name
                                                      "xt$iid" (.readerForName leaf-rel "xt$iid")
                                                      "xt$system_from" nil
                                                      "xt$system_to" nil
                                                      "xt$valid_from" nil
                                                      "xt$valid_to" nil
                                                      (.structKeyReader doc-rdr normalized-name))]
                           :when rdr]
                       (.rowCopier rdr
                                   (case normalized-name
                                     "xt$iid" (.writerForName out-rel col-name [:fixed-size-binary 16])
                                     (.writerForName out-rel col-name)))))

        valid-from-wtrs (vec
                         (for [col-name col-names
                               :when (= "xt$valid_from" (util/str->normal-form-str col-name))]
                           (.writerForName out-rel col-name types/temporal-col-type)))

        valid-to-wtrs (vec
                       (for [col-name col-names
                             :when (= "xt$valid_to" (util/str->normal-form-str col-name))]
                         (.writerForName out-rel col-name types/temporal-col-type)))

        sys-from-wtrs (vec
                       (for [col-name col-names
                             :when (= "xt$system_from" (util/str->normal-form-str col-name))]
                         (.writerForName out-rel col-name types/temporal-col-type)))

        sys-to-wtrs (vec
                     (for [col-name col-names
                           :when (= "xt$system_to" (util/str->normal-form-str col-name))]
                       (.writerForName out-rel col-name types/temporal-col-type)))]

    (reify RowConsumer
      (accept [_ idx valid-from valid-to sys-from sys-to]
        (.startRow out-rel)

        (doseq [^IRowCopier copier row-copiers]
          (.copyRow copier idx))

        (doseq [^IVectorWriter valid-from-wtr valid-from-wtrs]
          (.writeLong valid-from-wtr valid-from))
        (doseq [^IVectorWriter valid-to-wtr valid-to-wtrs]
          (.writeLong valid-to-wtr valid-to))
        (doseq [^IVectorWriter sys-from-wtr sys-from-wtrs]
          (.writeLong sys-from-wtr sys-from))
        (doseq [^IVectorWriter sys-to-wtr sys-to-wtrs]
          (.writeLong sys-to-wtr sys-to))

        (.endRow out-rel)))))

(defn- wrap-temporal-ranges ^xtdb.operator.scan.RowConsumer [^RowConsumer rc, ^longs temporal-ranges]
  (let [valid-from-lower (aget temporal-ranges valid-from-lower-idx)
        valid-from-upper (aget temporal-ranges valid-from-upper-idx)
        valid-to-lower (aget temporal-ranges valid-to-lower-idx)
        valid-to-upper (aget temporal-ranges valid-to-upper-idx)
        sys-from-lower (aget temporal-ranges system-from-lower-idx)
        sys-from-upper (aget temporal-ranges system-from-upper-idx)
        sys-to-lower (aget temporal-ranges system-to-lower-idx)
        sys-to-upper (aget temporal-ranges system-to-upper-idx)]
    (reify RowConsumer
      (accept [_ idx valid-from valid-to sys-from sys-to]
        (when (and (<= valid-from-lower valid-from)
                   (<= valid-from valid-from-upper)
                   (<= valid-to-lower valid-to)
                   (<= valid-to valid-to-upper)
                   (<= sys-from-lower sys-from)
                   (<= sys-from sys-from-upper)
                   (<= sys-to-lower sys-to)
                   (<= sys-to sys-to-upper)
                   (not= valid-from valid-to)
                   (not= sys-from sys-to))
          (.accept rc idx valid-from valid-to sys-from sys-to))))))

(defn range-range-row-picker
  ^java.util.function.IntConsumer [^IRelationWriter out-rel, ^RelationReader leaf-rel
                                   col-names, ^longs temporal-ranges,
                                   {:keys [^EventResolver ev-resolver
                                           skip-iid-ptr prev-iid-ptr current-iid-ptr
                                           point-point?]}]
  (let [iid-rdr (.readerForName leaf-rel "xt$iid")
        sys-from-rdr (.readerForName leaf-rel "xt$system_from")
        op-rdr (.readerForName leaf-rel "op")
        put-rdr (.legReader op-rdr :put)
        put-valid-from-rdr (.structKeyReader put-rdr "xt$valid_from")
        put-valid-to-rdr (.structKeyReader put-rdr "xt$valid_to")

        delete-rdr (.legReader op-rdr :delete)
        delete-valid-from-rdr (.structKeyReader delete-rdr "xt$valid_from")
        delete-valid-to-rdr (.structKeyReader delete-rdr "xt$valid_to")

        sys-from-lower (aget temporal-ranges system-from-lower-idx)
        sys-from-upper (aget temporal-ranges system-from-upper-idx)

        put-rc (-> (copy-row-consumer out-rel leaf-rel col-names)

                   (as-> ^RowConsumer rc (reify RowConsumer
                                           (accept [_ idx valid-from valid-to sys-from sys-to]
                                             (when point-point?
                                               (duplicate-ptr skip-iid-ptr current-iid-ptr))

                                             (.accept rc idx valid-from valid-to sys-from sys-to))))

                   (wrap-temporal-ranges temporal-ranges))]

    (reify IntConsumer
      (accept [_ idx]
        (when-not (= skip-iid-ptr (.getPointer iid-rdr idx current-iid-ptr))
          (when-not (= prev-iid-ptr current-iid-ptr)
            (.nextIid ev-resolver)
            (duplicate-ptr prev-iid-ptr current-iid-ptr))

          (if (= :evict (.getLeg op-rdr idx))
            (duplicate-ptr skip-iid-ptr current-iid-ptr)

            (let [system-from (.getLong sys-from-rdr idx)]
              (when (and (<= sys-from-lower system-from) (<= system-from sys-from-upper))
                (case (.getLeg op-rdr idx)
                  :put
                  (.resolveEvent ev-resolver idx
                                 (.getLong put-valid-from-rdr idx)
                                 (.getLong put-valid-to-rdr idx)
                                 system-from
                                 put-rc)

                  :delete
                  (.resolveEvent ev-resolver idx
                                 (.getLong delete-valid-from-rdr idx)
                                 (.getLong delete-valid-to-rdr idx)
                                 system-from
                                 nil))))))))))

(deftype TrieCursor [^BufferAllocator allocator, arrow-leaves
                     ^Iterator merge-tasks, ^ints leaf-idxs, ^ints current-arrow-page-idxs
                     col-names, ^Map col-preds, ^longs temporal-timestamps,
                     params, ^IPersistentMap picker-state]
  ICursor
  (tryAdvance [_ c]
    (if (.hasNext merge-tasks)
      (let [{task-leaves :leaves, :keys [path]} (.next merge-tasks)]
        (with-open [out-rel (vw/->rel-writer allocator)]
          (letfn [(rel->leaf-ptr [leaf-ordinal ^RelationReader log-rdr]
                    (let [row-count (.rowCount log-rdr)
                          iid-rdr (.readerForName log-rdr "xt$iid")
                          ^IntConsumer picker (range-range-row-picker out-rel log-rdr col-names temporal-timestamps picker-state)

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

;; The consumers for different leafs need to share some state so the logic of how to advance
;; is correct. For example if the `skip-iid-ptr` gets set in one leaf consumer it should also affect
;; the skipping in another leaf consumer.

(defn ->4r-cursor [^BufferAllocator allocator, ^ObjectStore obj-store, ^IBufferPool buffer-pool, ^IWatermark wm
                   table-name, col-names, ^longs temporal-range
                   ^Map col-preds, params, scan-opts]
  (let [^ILiveTableWatermark live-table-wm (some-> (.liveIndex wm) (.liveTable table-name))
        {:keys [arrow-leaves ^List merge-tasks]} (read-tries obj-store buffer-pool table-name live-table-wm)]
    (try
      (->TrieCursor allocator arrow-leaves (.iterator merge-tasks)
                    (int-array (cond-> (count arrow-leaves)
                                 live-table-wm inc))
                    (doto (int-array (count arrow-leaves))
                      (Arrays/fill -1))
                    col-names col-preds
                    temporal-range
                    params
                    (cond-> {:ev-resolver (event-resolver (and (not (at-point-point? scan-opts))
                                                               (not (at-range-point? scan-opts))))
                             :skip-iid-ptr (ArrowBufPointer.)
                             :prev-iid-ptr (ArrowBufPointer.)
                             :current-iid-ptr (ArrowBufPointer.)}
                      (at-point-point? scan-opts) (assoc :point-point? true)))

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
                       (some-> (.liveIndex wm)
                               (.liveTable normalized-table)
                               (.columnTypes)
                               keys)])))

    (allTableColNames [_ wm]
      (merge-with set/union
                  (update-vals (.allColumnTypes metadata-mgr)
                               (comp set keys))
                  (update-vals (some-> (.liveIndex wm)
                                       (.allColumnTypes))
                               (comp set keys))))

    (scanColTypes [_ wm scan-cols]
      (letfn [(->col-type [[table col-name]]
                (let [normalized-table (util/str->normal-form-str (str table))
                      normalized-col-name (util/str->normal-form-str (str col-name))]
                  (if (types/temporal-column? (util/str->normal-form-str (str col-name)))
                    [:timestamp-tz :micro "UTC"]
                    (types/merge-col-types (.columnType metadata-mgr normalized-table normalized-col-name)
                                           (some-> (.liveIndex wm)
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
            (->> col-names (group-by (comp types/temporal-column? util/str->normal-form-str str)))

            content-col-names (set (map str content-col-names))
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
                                     :when (not (types/temporal-column? (util/str->normal-form-str (str col-name))))]
                                 select))

            row-count (->> (for [{:keys [tables]} (vals (.chunksMetadata metadata-mgr))
                                 :let [{:keys [row-count]} (get tables normalized-table-name)]
                                 :when row-count]
                             row-count)
                           (reduce +))]

        {:col-types col-types
         :stats {:row-count row-count}
         :->cursor (fn [{:keys [allocator, ^IWatermark watermark, basis, params default-all-valid-time?]}]
                     ;; TODO reinstate metadata checks on pages
                     (let [_metadata-pred (expr.meta/->metadata-selector (cons 'and metadata-args) col-types params)
                           scan-opts (cond-> scan-opts
                                       (nil? for-valid-time)
                                       (assoc :for-valid-time (if default-all-valid-time? [:all-time] [:at [:now :now]])))]
                       (->4r-cursor allocator object-store buffer-pool
                                    watermark
                                    normalized-table-name
                                    (set/union content-col-names temporal-col-names)
                                    (->temporal-range params basis scan-opts)
                                    col-preds
                                    params
                                    scan-opts)))}))))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter scan-col-types, param-types]}]
  (.emitScan scan-emitter scan-expr scan-col-types param-types))
