(ns xtdb.operator.scan
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.bloom :as bloom]
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
  (:import (clojure.lang IPersistentMap MapEntry)
           (java.io Closeable)
           java.nio.ByteBuffer
           (java.util HashMap Iterator LinkedList List Map)
           (java.util.function IntConsumer IntPredicate)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           [org.apache.arrow.memory.util ArrowBufPointer]
           (org.apache.arrow.vector VectorLoader)
           [org.roaringbitmap.buffer MutableRoaringBitmap]
           xtdb.api.protocols.TransactionInstant
           xtdb.buffer_pool.IBufferPool
           xtdb.ICursor
           (xtdb.metadata IMetadataManager ITableMetadata)
           xtdb.object_store.ObjectStore
           xtdb.operator.IRelationSelector
           (xtdb.trie HashTrie LiveHashTrie$Leaf)
           (xtdb.util TemporalBounds TemporalBounds$TemporalColumn)
           (xtdb.vector IRelationWriter IRowCopier IVectorReader IVectorWriter RelationReader)
           (xtdb.watermark ILiveTableWatermark IWatermark IWatermarkSource Watermark)))

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

(defn filter-pushdown-bloom-page-idx-pred ^IntPredicate [^IMetadataManager metadata-manager ^String col-name
                                                         {:keys [trie-data ^RelationReader trie-rdr] :as _trie-match}]
  (when-let [^MutableRoaringBitmap pushdown-bloom (get *column->pushdown-bloom* (symbol col-name))]
    (let [^ITableMetadata table-metadata (.tableMetadata metadata-manager trie-rdr (:trie-file trie-data))
          metadata-rdr (.metadataReader table-metadata)
          bloom-rdr (-> (.structKeyReader metadata-rdr "columns")
                        (.listElementReader)
                        (.structKeyReader "bloom"))]
      (reify IntPredicate
        (test [_ page-idx]
          (boolean
           (when-let [bloom-vec-idx (.rowIndex table-metadata col-name page-idx)]
             (and (not (nil? (.getObject bloom-rdr bloom-vec-idx)))
                  (MutableRoaringBitmap/intersects pushdown-bloom
                                                   (bloom/bloom->bitmap bloom-rdr bloom-vec-idx))))))))))

(defn- ->temporal-bounds [^RelationReader params, {^TransactionInstant basis-tx :tx}, {:keys [for-valid-time for-system-time]}]
  (let [bounds (TemporalBounds.)]
    (letfn [(->time-μs [[tag arg]]
              (case tag
                :literal (-> arg
                             (util/sql-temporal->micros (.getZone expr/*clock*)))
                :param (-> (-> (.readerForName params (name arg))
                               (.getObject 0))
                           (util/sql-temporal->micros (.getZone expr/*clock*)))
                :now (-> (.instant expr/*clock*)
                         (util/instant->micros))))]

      (when-let [system-time (some-> basis-tx (.system-time) util/instant->micros)]
        (.lte (.systemFrom bounds) system-time)

        (when-not for-system-time
          (.gt (.systemTo bounds) system-time)))

      (letfn [(apply-constraint [constraint ^TemporalBounds$TemporalColumn start-col, ^TemporalBounds$TemporalColumn end-col]
                (when-let [[tag & args] constraint]
                  (case tag
                    :at (let [[at] args
                              at-μs (->time-μs at)]
                          (.lte start-col at-μs)
                          (.gt end-col at-μs))

                    ;; overlaps [time-from time-to]
                    :in (let [[from to] args]
                          (.gt end-col (->time-μs (or from [:now])))
                          (when to
                            (.lt start-col (->time-μs to))))

                    :between (let [[from to] args]
                               (.gt end-col (->time-μs (or from [:now])))
                               (when to
                                 (.lte start-col (->time-μs to))))

                    :all-time nil)))]

        (apply-constraint for-valid-time (.validFrom bounds) (.validTo bounds))
        (apply-constraint for-system-time (.systemFrom bounds) (.systemTo bounds))))
    bounds))

(defn- scan-op-point? [scan-op]
  (= :at (first scan-op)))

(defn- at-point-point? [{:keys [for-valid-time for-system-time]}]
  (and (or (nil? for-valid-time)
           (scan-op-point? for-valid-time))
       (or (nil? for-system-time)
           (scan-op-point? for-system-time))))

(defn tables-with-cols [basis ^IWatermarkSource wm-src ^IScanEmitter scan-emitter]
  (let [{:keys [tx, after-tx]} basis
        wm-tx (or tx after-tx)]
    (with-open [^Watermark wm (.openWatermark wm-src wm-tx)]
      (.allTableColNames scan-emitter wm))))

;; As the algorithm processes events in reverse system time order, one can
;; immediately write out the system-to times when having finished an event.
;; The system-to times are not relevant for processing earlier events.
(defrecord Rectangle [^long valid-from, ^long valid-to, ^long sys-from])

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface RowConsumer
  (^void accept [^int idx, ^long validFrom, ^long validTo, ^long systemFrom, ^long systemTo]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface EventResolver
  (^void resolveEvent [^int idx, ^long validFrom, ^long validTo, ^long systemFrom
                       ^xtdb.operator.scan.RowConsumer rowConsumer])
  (^void nextIid []))

(defn event-resolver
  (^xtdb.operator.scan.EventResolver [] (event-resolver (LinkedList.)))

  (^xtdb.operator.scan.EventResolver [^List !ranges]
   (reify EventResolver
     (nextIid [_]
       (.clear !ranges))

     ;; https://en.wikipedia.org/wiki/Allen%27s_interval_algebra
     (resolveEvent [_ idx valid-from valid-to system-from rc]
       (when rc
         (let [itr (.iterator !ranges)]
           (loop [valid-from valid-from]
             (when (< valid-from valid-to)
               (if-not (.hasNext itr)
                 (.accept rc idx valid-from valid-to system-from util/end-of-time-μs)

                 (let [^Rectangle r (.next itr)]
                   (if (<= (.valid-to r) valid-from)
                     ;; state #{< m} event
                     (recur valid-from)

                     ;; state #{> mi o oi s si d di f fi =} event
                     (do
                       (when (< valid-from (.valid-from r))
                         ;; state #{> mi oi d f} event
                         (let [valid-to (min valid-to (.valid-from r))]
                           (when (< valid-from valid-to)
                             (.accept rc idx valid-from valid-to
                                      system-from util/end-of-time-μs))))

                       (let [valid-from (max valid-from (.valid-from r))
                             valid-to (min valid-to (.valid-to r))]
                         (when (< valid-from valid-to)
                           (.accept rc idx valid-from valid-to system-from (.sys-from r))))

                       (when (< (.valid-to r) valid-to)
                         ;; state #{o s d} event
                         (recur (.valid-to r)))))))))))

       (let [itr (.listIterator !ranges)]
         (loop [ev-added? false]
           (let [^Rectangle r (when (.hasNext itr)
                                (.next itr))]
             (cond
               (nil? r) (when-not ev-added?
                          (.add itr (Rectangle. valid-from valid-to system-from)))

               ;; state #{< m} event
               (<= (.valid-to r) valid-from) (recur ev-added?)

               ;; state #{> mi o oi s si d di f fi =} event
               :else (do
                       (if (< (.valid-from r) valid-from)
                         ;; state #{o di fi} event
                         (.set itr (Rectangle. (.valid-from r) valid-from (.sys-from r)))

                         ;; state #{> mi oi s si d f =} event
                         (.remove itr))

                       (when-not ev-added?
                         (.add itr (Rectangle. valid-from valid-to system-from)))

                       (when (< valid-to (.valid-to r))
                         ;; state #{> mi oi si di}
                         (let [valid-from (max valid-to (.valid-from r))]
                           (when (< valid-from (.valid-to r))
                             (.add itr (Rectangle. valid-from (.valid-to r) (.sys-from r))))))

                       (recur true)))))

         #_ ; asserting the ranges invariant isn't ideal on the fast path
         (assert (->> (partition 2 1 !ranges)
                      (every? (fn [[^Rectangle r1 ^Rectangle r2]]
                                (<= (.valid-to r1) (.valid-from r2)))))
                 {:ranges (mapv (juxt (comp util/micros->instant :valid-from)
                                      (comp util/micros->instant :valid-to)
                                      (comp util/micros->instant :sys-from))
                                !ranges)
                  :ev [(util/micros->instant valid-from)
                       (util/micros->instant valid-to)
                       (util/micros->instant system-from)]}))))))

(defn- duplicate-ptr [^ArrowBufPointer dst, ^ArrowBufPointer src]
  (.set dst (.getBuf src) (.getOffset src) (.getLength src)))

(defn- copy-row-consumer [^IRelationWriter out-rel, ^RelationReader leaf-rel, col-names]
  (letfn [(writer-for [normalised-col-name]
            (let [^objects wtrs (->> col-names
                                     (keep (fn [col-name]
                                             (when (= normalised-col-name (util/str->normal-form-str col-name))
                                               (.writerForName out-rel col-name types/temporal-col-type))))
                                     (object-array))]
              (reify IVectorWriter
                (writeLong [_ l]
                  (dotimes [i (alength wtrs)]
                    (let [^IVectorWriter wtr (aget wtrs i)]
                      (.writeLong wtr l)))))))]
    (let [op-rdr (.readerForName leaf-rel "op")
          put-rdr (.legReader op-rdr :put)
          doc-rdr (.structKeyReader put-rdr "xt$doc")

          row-copiers (object-array
                       (for [col-name col-names
                             :let [normalized-name (util/str->normal-form-str col-name)
                                   ^IVectorReader rdr (case normalized-name
                                                        "xt$iid" (.readerForName leaf-rel "xt$iid")
                                                        ("xt$system_from" "xt$system_to" "xt$valid_from" "xt$valid_to") nil
                                                        (.structKeyReader doc-rdr normalized-name))]
                             :when rdr]
                         (.rowCopier rdr
                                     (case normalized-name
                                       "xt$iid" (.writerForName out-rel col-name [:fixed-size-binary 16])
                                       (.writerForName out-rel col-name)))))

          ^IVectorWriter valid-from-wtr (writer-for "xt$valid_from")
          ^IVectorWriter valid-to-wtr (writer-for "xt$valid_to")
          ^IVectorWriter sys-from-wtr (writer-for "xt$system_from")
          ^IVectorWriter sys-to-wtr (writer-for "xt$system_to")]

      (reify RowConsumer
        (accept [_ idx valid-from valid-to sys-from sys-to]
          (.startRow out-rel)

          (dotimes [i (alength row-copiers)]
            (let [^IRowCopier copier (aget row-copiers i)]
              (.copyRow copier idx)))

          (.writeLong valid-from-wtr valid-from)
          (.writeLong valid-to-wtr valid-to)
          (.writeLong sys-from-wtr sys-from)
          (.writeLong sys-to-wtr sys-to)

          (.endRow out-rel))))))

(defn- wrap-temporal-bounds ^xtdb.operator.scan.RowConsumer [^RowConsumer rc, ^TemporalBounds temporal-bounds]
  (reify RowConsumer
    (accept [_ idx valid-from valid-to sys-from sys-to]
      (when (.inRange temporal-bounds valid-from valid-to sys-from sys-to)
        (.accept rc idx valid-from valid-to sys-from sys-to)))))

(defn range-range-row-picker
  ^java.util.function.IntConsumer [^IRelationWriter out-rel, ^RelationReader leaf-rel
                                   col-names, ^TemporalBounds temporal-bounds,
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

        sys-from-lower (.lower (.systemFrom temporal-bounds))
        sys-from-upper (.upper (.systemFrom temporal-bounds))

        put-rc (-> (copy-row-consumer out-rel leaf-rel col-names)

                   (as-> ^RowConsumer rc (reify RowConsumer
                                           (accept [_ idx valid-from valid-to sys-from sys-to]
                                             (when point-point?
                                               (duplicate-ptr skip-iid-ptr current-iid-ptr))

                                             (.accept rc idx valid-from valid-to sys-from sys-to))))

                   (wrap-temporal-bounds temporal-bounds))]

    (reify IntConsumer
      (accept [_ idx]
        (when-not (= skip-iid-ptr (.getPointer iid-rdr idx current-iid-ptr))
          (when-not (= prev-iid-ptr current-iid-ptr)
            (.nextIid ev-resolver)
            (duplicate-ptr prev-iid-ptr current-iid-ptr))

          (let [leg-name (.getName (.getLeg op-rdr idx))]
            (if (= "evict" leg-name)
              (duplicate-ptr skip-iid-ptr current-iid-ptr)

              (let [system-from (.getLong sys-from-rdr idx)]
                (when (and (<= sys-from-lower system-from) (<= system-from sys-from-upper))
                  (case leg-name
                    "put"
                    (.resolveEvent ev-resolver idx
                                   (.getLong put-valid-from-rdr idx)
                                   (.getLong put-valid-to-rdr idx)
                                   system-from
                                   put-rc)

                    "delete"
                    (.resolveEvent ev-resolver idx
                                   (.getLong delete-valid-from-rdr idx)
                                   (.getLong delete-valid-to-rdr idx)
                                   system-from
                                   nil)))))))))))

(defn iid-selector [^ByteBuffer iid-bb]
  (reify IRelationSelector
    (select [_ allocator rel-rdr _params]
      (with-open [arrow-buf (util/->arrow-buf-view allocator iid-bb)]
        (let [iid-ptr (ArrowBufPointer. arrow-buf 0 (.capacity iid-bb))
              ptr (ArrowBufPointer.)
              iid-rdr (.readerForName rel-rdr "xt$iid")
              value-count (.valueCount iid-rdr)]
          (if (pos-int? value-count)
            ;; lower-bound
            (loop [left 0 right (dec value-count)]
              (if (= left right)
                (if (= iid-ptr (.getPointer iid-rdr left ptr))
                  ;; upper bound
                  (loop [right left]
                    (if (or (>= right value-count) (not= iid-ptr (.getPointer iid-rdr right ptr)))
                      (.toArray (IntStream/range left right))
                      (recur (inc right))))
                  (int-array 0))
                (let [mid (quot (+ left right) 2)]
                  (if (<= (.compareTo iid-ptr (.getPointer iid-rdr mid ptr)) 0)
                    (recur left mid)
                    (recur (inc mid) right)))))
            (int-array 0)))))))

(defn merge-plan->tasks ^java.lang.Iterable [{:keys [path node]}]
  (when-let [[node-tag node-arg] node]
    (case node-tag
      :branch (mapcat merge-plan->tasks node-arg)
      :leaf [{:path path, :leaves node-arg}])))

(defrecord VSRCache [^IBufferPool buffer-pool, ^BufferAllocator allocator, ^Map cache]
  Closeable
  (close [_] (util/close cache)))

(defn ->vsr-cache [buffer-pool allocator]
  (->VSRCache buffer-pool allocator (HashMap.)))

(defn cache-vsr [vsr-cache trie-leaf-file]
  (let [{:keys [^Map cache, buffer-pool, allocator]} vsr-cache
        compute (fn [_] (bp/open-vsr buffer-pool trie-leaf-file allocator))]
    (.computeIfAbsent cache trie-leaf-file (util/->jfn compute))))

(defn merge-task-leaf-reader ^IVectorReader [buffer-pool vsr-cache {:keys [page-idx, trie-leaf-file, rel-rdr, node]}]
  (if page-idx
    (util/with-open [rb (bp/open-record-batch buffer-pool trie-leaf-file page-idx)]
      (let [vsr (cache-vsr vsr-cache trie-leaf-file)
            loader (VectorLoader. vsr)]
        (.load loader rb)
        (vr/<-root vsr)))
    (.select ^RelationReader rel-rdr (.data ^LiveHashTrie$Leaf node))))

(defn merge-task-readers [buffer-pool vsr-cache {:keys [leaves]}]
  (mapv #(when % (merge-task-leaf-reader buffer-pool vsr-cache %)) leaves))

(deftype TrieCursor [^BufferAllocator allocator, ^Iterator merge-tasks
                     col-names, ^Map col-preds, ^longs temporal-timestamps,
                     params, ^IPersistentMap picker-state
                     vsr-cache, buffer-pool]
  ICursor
  (tryAdvance [_ c]
    (if (.hasNext merge-tasks)
      (let [merge-task (.next merge-tasks)]
        (with-open [out-rel (vw/->rel-writer allocator)]
          (let [^IRelationSelector iid-pred (get col-preds "xt$iid")
                unfiltered-readers (merge-task-readers buffer-pool vsr-cache merge-task)
                apply-iid-pred (fn [^RelationReader rel-rdr] (.select rel-rdr (.select iid-pred allocator rel-rdr params)))
                filtered-readers
                (if iid-pred
                  (mapv (fn [rel-rdr] (some-> rel-rdr apply-iid-pred)) unfiltered-readers)
                  unfiltered-readers)

                merge-q (trie/->merge-queue filtered-readers merge-task)
                ^"[Ljava.util.function.IntConsumer;"
                row-pickers (make-array IntConsumer (count filtered-readers))]

            (dotimes [i (count filtered-readers)]
              (when-some [rel-rdr (nth filtered-readers i)]
                (let [row-picker (range-range-row-picker out-rel rel-rdr col-names temporal-timestamps picker-state)]
                  (aset row-pickers i row-picker))))

            (loop []
              (when-let [lp (.poll merge-q)]
                (.accept ^IntConsumer (aget row-pickers (.getOrdinal lp)) (.getIndex lp))
                (.advance merge-q lp)
                (recur)))

            (.accept c (-> (vw/rel-wtr->rdr out-rel)
                           (vr/with-absent-cols allocator col-names)

                           (as-> rel (reduce (fn [^RelationReader rel, ^IRelationSelector col-pred]
                                               (.select rel (.select col-pred allocator rel params)))
                                             rel
                                             (vals (dissoc col-preds "xt$iid"))))))))
        true)

      false))

  (close [_]
    (util/close vsr-cache)))

(defn- filter-trie-match [^IMetadataManager metadata-mgr col-names {:keys [page-idx-pred] :as trie-match}]
  (->> (reduce (fn [^IntPredicate page-idx-pred col-name]
                 (if-let [bloom-page-idx-pred (filter-pushdown-bloom-page-idx-pred metadata-mgr col-name trie-match)]
                   (.and page-idx-pred bloom-page-idx-pred)
                   page-idx-pred))
               page-idx-pred col-names)
       (assoc trie-match :page-idx-pred)))

(defn- eid-select->eid [eid-select]
  (if (= 'xt/id (second eid-select))
    (nth eid-select 2)
    (second eid-select)))

(defn selects->iid-byte-buffer ^ByteBuffer [selects ^RelationReader params-rel]
  (when-let [eid-select (or (get selects "xt/id") (get selects "xt$id"))]
    (when (= '= (first eid-select))
       (let [eid (eid-select->eid eid-select)]
         (cond
          (s/valid? ::lp/value eid)
          (trie/->iid eid)

          (s/valid? ::lp/param eid)
          (let [eid-rdr (.readerForName params-rel (name eid))]
            (when (= 1 (.valueCount eid-rdr))
              (trie/->iid (.getObject eid-rdr 0)))))))))

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

    (emitScan [_ {:keys [columns], {:keys [table] :as scan-opts} :scan-opts} scan-col-types param-types]
      (let [col-names (->> columns
                           (into #{} (map (fn [[col-type arg]]
                                            (case col-type
                                              :column arg
                                              :select (key (first arg)))))))

            col-types (->> col-names
                           (into {} (map (juxt identity
                                               (fn [col-name]
                                                 (get scan-col-types [table col-name]))))))

            col-names (into #{} (map str) col-names)

            normalized-table-name (util/str->normal-form-str (str table))
            normalized-col-names (into #{} (map util/str->normal-form-str) col-names)

            selects (->> (for [[tag arg] columns
                               :when (= tag :select)
                               :let [[col-name pred] (first arg)]]
                           (MapEntry/create (str col-name) pred))
                         (into {}))

            col-preds (->> (for [[col-name select-form] selects]
                             ;; for temporal preds, we may not need to re-apply these if they can be represented as a temporal range.
                             (MapEntry/create col-name
                                              (expr/->expression-relation-selector select-form {:col-types col-types, :param-types param-types})))
                           (into {}))

            metadata-args (vec (for [[col-name select] selects
                                     :when (not (types/temporal-column? (util/str->normal-form-str col-name)))]
                                 select))

            row-count (->> (for [{:keys [tables]} (vals (.chunksMetadata metadata-mgr))
                                 :let [{:keys [row-count]} (get tables normalized-table-name)]
                                 :when row-count]
                             row-count)
                           (reduce +))]

        {:col-types col-types
         :stats {:row-count row-count}
         :->cursor (fn [{:keys [allocator, ^IWatermark watermark, basis, params default-all-valid-time?]}]
                     (let [iid-bb (selects->iid-byte-buffer selects params)
                           col-preds (cond-> col-preds
                                       iid-bb (assoc "xt$iid" (iid-selector iid-bb)))
                           metadata-pred (expr.meta/->metadata-selector (cons 'and metadata-args) col-types params)
                           scan-opts (-> scan-opts
                                         (update :for-valid-time
                                                 (fn [fvt]
                                                   (or fvt (if default-all-valid-time? [:all-time] [:at [:now :now]])))))
                           ^ILiveTableWatermark live-table-wm (some-> (.liveIndex watermark) (.liveTable normalized-table-name))
                           table-tries (->> (trie/list-table-trie-files object-store normalized-table-name)
                                            (trie/current-table-tries))]

                       (util/with-open [iid-arrow-buf (when iid-bb (util/->arrow-buf-view allocator iid-bb))
                                        arrow-tries (trie/open-arrow-trie-files buffer-pool table-tries)]
                         (let [path-pred (if iid-bb
                                           (let [iid-ptr (ArrowBufPointer. iid-arrow-buf 0 (.capacity iid-bb))]
                                             #(zero? (HashTrie/compareToPath iid-ptr %)))
                                           (constantly true))
                               trie-matches (->> (meta/matching-tries metadata-mgr table-tries arrow-tries metadata-pred)
                                                 (map (partial filter-trie-match metadata-mgr col-names))
                                                 (drop-while #(empty? (set/intersection normalized-col-names (:col-names %))))
                                                 vec)
                               merge-plan (trie/table-merge-plan path-pred trie-matches live-table-wm)]

                           ;; The consumers for different leafs need to share some state so the logic of how to advance
                           ;; is correct. For example if the `skip-iid-ptr` gets set in one leaf consumer it should also affect
                           ;; the skipping in another leaf consumer.

                           (->TrieCursor allocator (.iterator (let [^Iterable c (or (merge-plan->tasks merge-plan) [])] c))
                                         col-names col-preds
                                         (->temporal-bounds params basis scan-opts)
                                         params
                                         (cond-> {:ev-resolver (event-resolver)
                                                  :skip-iid-ptr (ArrowBufPointer.)
                                                  :prev-iid-ptr (ArrowBufPointer.)
                                                  :current-iid-ptr (ArrowBufPointer.)}
                                           (at-point-point? scan-opts) (assoc :point-point? true))
                                         (->vsr-cache buffer-pool allocator)
                                         buffer-pool)))))}))))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter scan-col-types, param-types]}]
  (.emitScan scan-emitter scan-expr scan-col-types param-types))
