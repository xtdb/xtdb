(ns core2.operator.group-by
  (:require [core2.util :as util]
            [core2.relation :as rel])
  (:import [core2 IChunkCursor ICursor]
           core2.relation.IReadRelation
           [java.util ArrayList Comparator DoubleSummaryStatistics EnumSet HashMap LinkedHashMap List LongSummaryStatistics Map Optional Spliterator]
           [java.util.function BiConsumer Consumer Function IntConsumer ObjDoubleConsumer ObjIntConsumer ObjLongConsumer Supplier]
           [java.util.stream Collector Collector$Characteristics Collectors IntStream]
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.vector.types.Types$MinorType
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(definterface AggregateSpec
  (^String columnName [])
  (^Object aggregate [^core2.relation.IReadRelation inRelation ^Object container ^org.roaringbitmap.RoaringBitmap idx-bitmap])
  (^Object finish [^Object container]))

(deftype GroupSpec [^String col-name]
  AggregateSpec
  (columnName [_] col-name)

  (aggregate [_ in-rel container idx-bitmap]
    (or container
        (.getObject (.readColumn in-rel col-name)
                    (.first idx-bitmap))))

  (finish [_ container]
    container))

(defn ->group-spec ^core2.operator.group_by.AggregateSpec [^String col-name]
  (GroupSpec. col-name))

(defn- <-optional [result]
  (if (instance? Optional result)
    (.orElse ^Optional result nil)
    result))

(deftype FunctionSpec [^String from-name ^String to-name ^Collector collector]
  AggregateSpec
  (columnName [_] to-name)

  (aggregate [_ in-rel container idx-bitmap]
    (let [from-col (.readColumn in-rel from-name)
          accumulator (.accumulator collector)]
      (.collect ^IntStream (.stream idx-bitmap)
                (if (some? container)
                  (reify Supplier
                    (get [_] container))
                  (.supplier collector))
                (reify ObjIntConsumer
                  (accept [_ acc idx]
                    (.accept accumulator acc (.getObject from-col idx))))
                accumulator)))

  (finish [_ container]
    (-> (.apply (.finisher collector) container)
        (<-optional))))

(defn ->function-spec ^core2.operator.group_by.AggregateSpec [^String from-name ^String to-name ^Collector collector]
  (FunctionSpec. from-name to-name collector))

(deftype NumberFunctionSpec [^String from-name
                             ^String to-name
                             ^Supplier supplier
                             accumulator ;; <ObjLongConsumer & ObjDoubleConsumer>
                             ^Function finisher]
  AggregateSpec
  (columnName [_] to-name)

  (aggregate [_ in-rel container idx-bitmap]
    (let [from-col (.readColumn in-rel from-name)
          acc (if (some? container)
                container
                (.get supplier))
          consumer (cond
                     (= (EnumSet/of Types$MinorType/BIGINT) (.minorTypes from-col))
                     (reify IntConsumer
                       (accept [_ idx]
                         (.accept ^ObjLongConsumer accumulator acc
                                  (.getLong from-col idx))))

                     (= (EnumSet/of Types$MinorType/FLOAT8) (.minorTypes from-col))
                     (reify IntConsumer
                       (accept [_ idx]
                         (.accept ^ObjDoubleConsumer accumulator acc
                                  (.getDouble from-col idx))))

                     :else
                     (reify IntConsumer
                       (accept [_ idx]
                         (let [v (.getObject from-col idx)]
                           (if (integer? v)
                             (.accept ^ObjLongConsumer accumulator acc v)
                             (.accept ^ObjDoubleConsumer accumulator acc v))))))]
      (.forEach ^IntStream (.stream idx-bitmap) consumer)
      acc))

  (finish [_ container]
    (-> (.apply finisher container)
        (<-optional))))

(defn ->number-function-spec
  ^core2.operator.group_by.AggregateSpec
  [^String from-name, ^String to-name, ^Supplier supplier, accumulator, ^Function finisher]
  (NumberFunctionSpec. from-name to-name supplier accumulator finisher))

(deftype NumberSummaryStatistics [^LongSummaryStatistics long-summary
                                  ^DoubleSummaryStatistics double-summary])

(def number-summary-supplier
  (reify Supplier
    (get [_]
      (NumberSummaryStatistics. (LongSummaryStatistics.) (DoubleSummaryStatistics.)))))

(def number-summary-accumulator
  (reify
    ObjLongConsumer
    (^void accept [_ acc ^long x]
     (.accept ^LongSummaryStatistics (.long-summary ^NumberSummaryStatistics acc) x))

    ObjDoubleConsumer
    (^void accept [_ acc ^double x]
     (.accept ^DoubleSummaryStatistics (.double-summary ^NumberSummaryStatistics acc) x))))

(def number-sum-finisher
  (reify Function
    (apply [_ acc]
      (let [^NumberSummaryStatistics acc acc
            ^DoubleSummaryStatistics double-summary (.double-summary acc)
            ^LongSummaryStatistics long-summary (.long-summary acc)]
        (if (zero? (.getCount double-summary))
          (if (zero? (.getCount long-summary))
            (.getSum double-summary)
            (.getSum long-summary))
          (+ (.getSum long-summary)
             (.getSum double-summary)))))))

(def number-avg-finisher
  (reify Function
    (apply [_ acc]
      (let [^NumberSummaryStatistics acc acc
            ^DoubleSummaryStatistics double-summary (.double-summary acc)
            ^LongSummaryStatistics long-summary (.long-summary acc)
            cnt (+ (.getCount long-summary)
                   (.getCount double-summary))]
        (if (zero? cnt)
          (.getAverage double-summary)
          (/ (+ (.getSum long-summary)
                (.getSum double-summary)) cnt))))))

(def number-min-finisher
  (reify Function
    (apply [_ acc]
      (let [^NumberSummaryStatistics acc acc
            ^DoubleSummaryStatistics double-summary (.double-summary acc)
            ^LongSummaryStatistics long-summary (.long-summary acc)]
        (if (zero? (.getCount double-summary))
          (if (zero? (.getCount long-summary))
            (.getMin double-summary)
            (.getMin long-summary))
          (min (.getMin ^LongSummaryStatistics long-summary)
               (.getMin ^DoubleSummaryStatistics double-summary)))))))

(def number-max-finisher
  (reify Function
    (apply [_ acc]
      (let [^NumberSummaryStatistics acc acc
            ^DoubleSummaryStatistics double-summary (.double-summary acc)
            ^LongSummaryStatistics long-summary (.long-summary acc)]
        (if (zero? (.getCount double-summary))
          (if (zero? (.getCount long-summary))
            (.getMax double-summary)
            (.getMax long-summary))
          (max (.getMax ^LongSummaryStatistics long-summary)
               (.getMax ^DoubleSummaryStatistics double-summary)))))))

(defn ->avg-number-spec [^String from-name ^String to-name]
  (->number-function-spec from-name to-name
                          number-summary-supplier
                          number-summary-accumulator
                          number-avg-finisher))

(defn ->sum-number-spec [^String from-name ^String to-name]
  (->number-function-spec from-name to-name
                          number-summary-supplier
                          number-summary-accumulator
                          number-sum-finisher))

(defn ->min-number-spec [^String from-name ^String to-name]
  (->number-function-spec from-name to-name
                          number-summary-supplier
                          number-summary-accumulator
                          number-min-finisher))

(defn ->max-number-spec [^String from-name ^String to-name]
  (->number-function-spec from-name to-name
                          number-summary-supplier
                          number-summary-accumulator
                          number-max-finisher))

(defn ->min-spec [^String from-name ^String to-name]
  (->function-spec from-name to-name (Collectors/minBy (Comparator/nullsFirst (Comparator/naturalOrder)))))

(defn ->max-spec [^String from-name ^String to-name]
  (->function-spec from-name to-name (Collectors/maxBy (Comparator/nullsFirst (Comparator/naturalOrder)))))

(defn ->count-spec ^core2.operator.group_by.AggregateSpec [^String from-name ^String to-name]
  (->function-spec from-name to-name (Collectors/counting)))

(defn ->count-not-null-spec ^core2.operator.group_by.AggregateSpec [^String from-name ^String to-name]
  (let [counting-collector (Collectors/counting)
        accumulator (.accumulator counting-collector)]
    (->function-spec from-name to-name (Collector/of (.supplier counting-collector)
                                                     (reify BiConsumer
                                                       (accept [_ acc x]
                                                         (when-not (nil? x)
                                                           (.accept accumulator acc x))))
                                                     (.combiner counting-collector)
                                                     (.finisher counting-collector)
                                                     (into-array Collector$Characteristics (.characteristics counting-collector))))))



(defn- copy-group-key [^BufferAllocator allocator ^List k]
  (dotimes [n (.size k)]
    (let [x (.get k n)]
      (.set k n (util/maybe-copy-pointer allocator x))))
  k)

(defn- release-group-key [k]
  (doseq [x k
          :when (instance? ArrowBufPointer x)]
    (util/try-close (.getBuf ^ArrowBufPointer x)))
  k)

(defn- ->group-key [^IReadRelation in-rel ^List group-specs ^long idx]
  (let [ks (ArrayList. (.size group-specs))]
    (doseq [^GroupSpec group-spec group-specs]
      (let [from-col (.readColumn in-rel (.col-name group-spec))]
        (.add ks (util/pointer-or-object (._getInternalVector from-col idx)
                                         (._getInternalIndex from-col idx)))))
    ks))

(defn- aggregate-groups [^BufferAllocator allocator ^IReadRelation in-rel ^List aggregate-specs ^Map group->accs]
  (let [group->idx-bitmap (HashMap.)
        ^List group-specs (vec (filter #(instance? GroupSpec %) aggregate-specs))]

    (dotimes [idx (.rowCount in-rel)]
      (let [group-key (->group-key in-rel group-specs idx)
            ^RoaringBitmap idx-bitmap (.computeIfAbsent group->idx-bitmap group-key
                                                        (reify Function
                                                          (apply [_ _]
                                                            (RoaringBitmap.))))]
        (.add idx-bitmap idx)))

    (doseq [[group-key ^RoaringBitmap idx-bitmap] group->idx-bitmap
            :let [^objects accs (if-let [accs (.get group->accs group-key)]
                                  accs
                                  (let [accs (object-array (.size aggregate-specs))]
                                    (.put group->accs (copy-group-key allocator group-key) accs)
                                    accs))]]
      (dotimes [n (.size aggregate-specs)]
        (aset accs n (.aggregate ^AggregateSpec (.get aggregate-specs n)
                                 in-rel
                                 (aget accs n)
                                 idx-bitmap))))))

(defn- finish-groups ^core2.relation.IReadRelation [^BufferAllocator allocator
                                                  ^List aggregate-specs
                                                  ^Map group->accs]
  (let [^List all-accs (ArrayList. ^List (vals group->accs))
        ^Map out-cols (LinkedHashMap.)
        row-count (.size all-accs)]
    (dotimes [n (.size aggregate-specs)]
      (let [^AggregateSpec aggregate-spec (.get aggregate-specs n)
            col-name (.columnName aggregate-spec)
            append-col (rel/->fresh-append-column allocator col-name)]
        (dotimes [idx row-count]
          (let [^objects accs (.get all-accs idx)]
            (.appendObject append-col
                           (.finish aggregate-spec (aget accs n)))))

        (.put out-cols col-name append-col)))
    (rel/->read-relation (rel/append->read-cols out-cols))))

(deftype GroupByCursor [^BufferAllocator allocator
                        ^IChunkCursor in-cursor
                        ^List aggregate-specs]
  ICursor
  (tryAdvance [_ c]
    (let [group->accs (HashMap.)]
      (try
        (.forEachRemaining in-cursor
                           (reify Consumer
                             (accept [_ in-rel]
                               (aggregate-groups allocator in-rel aggregate-specs group->accs))))

        (if-not (.isEmpty group->accs)
          (with-open [out-rel (finish-groups allocator aggregate-specs group->accs)]
            (.accept c out-rel)
            true)
          false)

        (finally
          (run! release-group-key (keys group->accs))))))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE))

  (close [_]
    (util/try-close in-cursor)))

(defn ->group-by-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List aggregate-specs]
  (GroupByCursor. allocator in-cursor aggregate-specs))
