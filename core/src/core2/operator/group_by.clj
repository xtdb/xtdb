(ns core2.operator.group-by
  (:require [core2.types :as types]
            [core2.types.nested :as nested]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.ICursor
           core2.vector.IIndirectRelation
           [java.util ArrayList Comparator DoubleSummaryStatistics HashMap LinkedList List LongSummaryStatistics Map Optional Spliterator]
           [java.util.function BiConsumer Consumer Function IntConsumer ObjDoubleConsumer ObjIntConsumer ObjLongConsumer Supplier]
           [java.util.stream Collector Collector$Characteristics Collectors IntStream]
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.vector BigIntVector Float8Vector]
           org.apache.arrow.vector.complex.DenseUnionVector
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(definterface AggregateSpec
  (^String columnName [])
  (^Object aggregate [^core2.vector.IIndirectRelation inRelation ^Object container ^org.roaringbitmap.RoaringBitmap idx-bitmap])
  (^Object finish [^Object container]))

(deftype GroupSpec [^String col-name]
  AggregateSpec
  (columnName [_] col-name)

  (aggregate [_ in-rel container idx-bitmap]
    (or container
        (let [col (.vectorForName in-rel col-name)]
          (nested/get-value (.getVector col)
                            (.getIndex col (.first idx-bitmap))))))

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
    (let [from-col (.vectorForName in-rel from-name)
          from-vec (.getVector from-col)
          accumulator (.accumulator collector)]
      (.collect ^IntStream (.stream idx-bitmap)
                (if (some? container)
                  (reify Supplier
                    (get [_] container))
                  (.supplier collector))
                (reify ObjIntConsumer
                  (accept [_ acc idx]
                    (.accept accumulator acc (nested/get-value from-vec (.getIndex from-col idx)))))
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
    (let [from-col (.vectorForName in-rel from-name)
          acc (if (some? container)
                container
                (.get supplier))
          arrow-types (iv/col->arrow-types from-col)
          consumer (cond
                     (= #{types/bigint-type} arrow-types)
                     (let [from-col (-> from-col
                                        (iv/reader-for-type types/bigint-type))
                           ^BigIntVector from-vec (.getVector from-col)]
                       (reify IntConsumer
                         (accept [_ idx]
                           (.accept ^ObjLongConsumer accumulator acc
                                    (.get from-vec (.getIndex from-col idx))))))

                     (= #{types/float8-type} arrow-types)
                     (let [from-col (-> from-col
                                        (iv/reader-for-type types/float8-type))
                           ^Float8Vector from-vec (.getVector from-col)]
                       (reify IntConsumer
                         (accept [_ idx]
                           (.accept ^ObjDoubleConsumer accumulator acc
                                    (.get from-vec (.getIndex from-col idx))))))

                     :else
                     (let [from-vec (.getVector from-col)]
                       (reify IntConsumer
                         (accept [_ idx]
                           (let [v (nested/get-value from-vec idx)]
                             (if (integer? v)
                               (.accept ^ObjLongConsumer accumulator acc v)
                               (.accept ^ObjDoubleConsumer accumulator acc v)))))))]
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

(defn- ->group-key [^IIndirectRelation in-rel ^List group-specs ^long idx]
  (let [ks (ArrayList. (.size group-specs))]
    (doseq [^GroupSpec group-spec group-specs]
      (let [from-col (.vectorForName in-rel (.col-name group-spec))]
        (.add ks (util/pointer-or-object (.getVector from-col) (.getIndex from-col idx)))))
    ks))

(defn- aggregate-groups [^BufferAllocator allocator ^IIndirectRelation in-rel ^List aggregate-specs ^Map group->accs]
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

(defn- finish-groups ^core2.vector.IIndirectRelation [^BufferAllocator allocator
                                                    ^List aggregate-specs
                                                    ^Map group->accs]
  (let [all-accs (ArrayList. ^List (vals group->accs))
        out-cols (LinkedList.)
        row-count (.size all-accs)]
    (dotimes [n (.size aggregate-specs)]
      (let [^AggregateSpec aggregate-spec (.get aggregate-specs n)
            col-name (.columnName aggregate-spec)
            append-vec (DenseUnionVector/empty col-name allocator)]
        (dotimes [idx row-count]
          (let [^objects accs (.get all-accs idx)]
            ;; HACK: using nested for now to get this working,
            ;; we'll want to incorporate this when we bring in a consistent DUV approach.
            (nested/append-value (.finish aggregate-spec (aget accs n)) append-vec)))

        (.add out-cols (iv/->direct-vec append-vec))))
    (iv/->indirect-rel out-cols)))

(deftype GroupByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
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
