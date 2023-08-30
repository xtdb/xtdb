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
           java.nio.ByteBuffer
           (java.util Iterator LinkedList List Map)
           (java.util.function IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           [org.apache.arrow.memory.util ArrowBufPointer]
           [org.roaringbitmap RoaringBitmap]
           [org.roaringbitmap.buffer ImmutableRoaringBitmap MutableRoaringBitmap]
           xtdb.api.protocols.TransactionInstant
           xtdb.buffer_pool.IBufferPool
           xtdb.ICursor
           (xtdb.metadata IMetadataManager ITableMetadata)
           xtdb.object_store.ObjectStore
           xtdb.operator.IRelationSelector
           (xtdb.trie LeafMergeQueue$LeafPointer)
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

(defn- filter-pushdown-bloom-page-idxs [^IMetadataManager metadata-manager buf-key ^String col-name ^RoaringBitmap page-idxs]
  (if-let [^MutableRoaringBitmap pushdown-bloom (get *column->pushdown-bloom* (symbol col-name))]
    ;; would prefer this `^long` to be on the param but can only have 4 params in a primitive hinted function in Clojure
    @(meta/with-metadata metadata-manager buf-key
       (util/->jfn
         (fn [^ITableMetadata table-metadata]
           (let [metadata-rdr (.metadataReader table-metadata)
                 bloom-rdr (-> (.structKeyReader metadata-rdr "columns")
                               (.listElementReader)
                               (.structKeyReader "bloom"))
                 filtered-page-idxs (RoaringBitmap.)]
             (.forEach page-idxs
                       (reify org.roaringbitmap.IntConsumer
                         (accept [_ page-idx]
                           (when-let [bloom-vec-idx (.rowIndex table-metadata col-name page-idx)]
                             (when (and (not (nil? (.getObject bloom-rdr bloom-vec-idx)))
                                        (MutableRoaringBitmap/intersects pushdown-bloom
                                                                         (bloom/bloom->bitmap bloom-rdr bloom-vec-idx)))
                               (.add filtered-page-idxs page-idx))))))

             (when-not (.isEmpty filtered-page-idxs)
               filtered-page-idxs)))))
    page-idxs))

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
            (let [wtrs (->> col-names
                            (into [] (keep (fn [col-name]
                                             (when (= normalised-col-name (util/str->normal-form-str col-name))
                                               (.writerForName out-rel col-name types/temporal-col-type))))))]
              (reify IVectorWriter
                (writeLong [_ l]
                  (doseq [^IVectorWriter wtr wtrs]
                    (.writeLong wtr l))))))]
    (let [op-rdr (.readerForName leaf-rel "op")
          put-rdr (.legReader op-rdr :put)
          doc-rdr (.structKeyReader put-rdr "xt$doc")

          row-copiers (vec
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

          (doseq [^IRowCopier copier row-copiers]
            (.copyRow copier idx))

          (.writeLong valid-from-wtr valid-from)
          (.writeLong valid-to-wtr valid-to)
          (.writeLong sys-from-wtr sys-from)
          (.writeLong sys-to-wtr sys-to)

          (.endRow out-rel))))))

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

(deftype TrieCursor [^BufferAllocator allocator, leaves, ^Iterator merge-tasks
                     col-names, ^Map col-preds, ^longs temporal-timestamps,
                     params, ^IPersistentMap picker-state]
  ICursor
  (tryAdvance [_ c]
    (if (.hasNext merge-tasks)
      (let [merge-task (.next merge-tasks)]
        (with-open [out-rel (vw/->rel-writer allocator)]
          (let [^IRelationSelector iid-pred (get col-preds "xt$iid")
                loaded-leaves (cond->> (trie/load-leaves leaves merge-task)
                                iid-pred (mapv #(update % :rel-rdr (fn [^RelationReader rel-rdr]
                                                                     (when rel-rdr
                                                                       (.select rel-rdr (.select iid-pred allocator rel-rdr params)))))))
                merge-q (trie/->merge-queue loaded-leaves merge-task)
                ^"[Ljava.util.function.IntConsumer;"
                row-pickers (make-array IntConsumer (count leaves))]
            (doseq [{:keys [^LeafMergeQueue$LeafPointer leaf-ptr rel-rdr]} loaded-leaves
                    :when leaf-ptr]
              (aset row-pickers (.getOrdinal leaf-ptr)
                    (range-range-row-picker out-rel rel-rdr col-names temporal-timestamps picker-state)))
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
    (util/close leaves)))

(defn filter-trie-match [^IMetadataManager metadata-mgr col-names {:keys [buf-key page-idxs] :as trie-match}]
  (when-let [page-idxs (reduce (fn [page-idxs col-name]
                                 (if-let [page-idxs (filter-pushdown-bloom-page-idxs metadata-mgr buf-key col-name page-idxs)]
                                   page-idxs
                                   (reduced nil)))
                               page-idxs col-names)]
    (assoc trie-match :page-idxs page-idxs)))

(defn selects->iid-byte-buffer ^ByteBuffer [selects ^RelationReader params-rel]
  (when-let [eid-select (or (get selects "xt/id") (get selects "xt$id"))]
    (let [eid (nth eid-select 2)]
      (when (= '= (first eid-select))
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
                                            (trie/current-table-tries))
                           trie-file->page-idxs (->> (meta/matching-tries metadata-mgr (map :trie-file table-tries) metadata-pred)
                                                     (filter #(not-empty (set/intersection normalized-col-names (:col-names %))))
                                                     (map (partial filter-trie-match metadata-mgr col-names))
                                                     (remove nil?)
                                                     (into {} (map (juxt :buf-key :page-idxs))))
                           merge-plan (trie/table-merge-plan buffer-pool metadata-mgr table-tries trie-file->page-idxs live-table-wm)]

                       ;; The consumers for different leafs need to share some state so the logic of how to advance
                       ;; is correct. For example if the `skip-iid-ptr` gets set in one leaf consumer it should also affect
                       ;; the skipping in another leaf consumer.

                       (util/with-close-on-catch [leaves (trie/open-leaves buffer-pool normalized-table-name table-tries live-table-wm)]
                         (->TrieCursor allocator leaves (.iterator (let [^Iterable c (or (merge-plan->tasks merge-plan) [])] c))
                                       col-names col-preds
                                       (->temporal-range params basis scan-opts)
                                       params
                                       (cond-> {:ev-resolver (event-resolver)
                                                :skip-iid-ptr (ArrowBufPointer.)
                                                :prev-iid-ptr (ArrowBufPointer.)
                                                :current-iid-ptr (ArrowBufPointer.)}
                                         (at-point-point? scan-opts) (assoc :point-point? true))))))}))))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter scan-col-types, param-types]}]
  (.emitScan scan-emitter scan-expr scan-col-types param-types))
