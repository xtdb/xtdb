(ns core2.operator.group-by
  (:require [core2.types :as t]
            [core2.util :as util])
  (:import [core2 DenseUnionUtil ICursor]
           [java.util ArrayList Comparator DoubleSummaryStatistics HashMap List LongSummaryStatistics Map Optional Spliterator]
           [java.util.function BiConsumer Consumer Function IntConsumer ObjDoubleConsumer ObjIntConsumer ObjLongConsumer Supplier]
           [java.util.stream Collector Collector$Characteristics Collectors IntStream]
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.vector BaseIntVector FloatingPointVector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types.pojo ArrowType Field Schema]
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(definterface AggregateSpec
  (^org.apache.arrow.vector.types.pojo.Field getToField [^org.apache.arrow.vector.VectorSchemaRoot inRoot])
  (^org.apache.arrow.vector.ValueVector getFromVector [^org.apache.arrow.vector.VectorSchemaRoot inRoot])
  (^Object aggregate [^org.apache.arrow.vector.VectorSchemaRoot inRoot ^Object container ^org.roaringbitmap.RoaringBitmap idx-bitmap])
  (^Object finish [^Object container]))

(deftype GroupSpec [^String name]
  AggregateSpec
  (getToField [this in-root]
    (.getField (.getFromVector this in-root)))

  (getFromVector [_ in-root]
    (.getVector in-root name))

  (aggregate [this in-root container idx-bitmap]
    (or container (t/get-object (.getFromVector this in-root) (.first idx-bitmap))))

  (finish [_ container]
    container))

(defn ->group-spec ^core2.operator.group_by.AggregateSpec [^String name]
  (GroupSpec. name))

(defn- finish-maybe-optional [^Function finisher container]
  (let [result (.apply finisher container)]
    (if (instance? Optional result)
      (.orElse ^Optional result nil)
      result)))

(deftype FunctionSpec [^String from-name ^Field field ^Collector collector]
  AggregateSpec
  (getToField [_ in-root]
    field)

  (getFromVector [_ in-root]
    (.getVector in-root from-name))

  (aggregate [this in-root container idx-bitmap]
    (let [from-vec (util/maybe-single-child-dense-union (.getFromVector this in-root))
          accumulator (.accumulator collector)]
      (.collect ^IntStream (.stream idx-bitmap)
                (if (some? container)
                  (reify Supplier
                    (get [_] container))
                  (.supplier collector))
                (reify ObjIntConsumer
                  (accept [_ acc idx]
                    (.accept accumulator acc (t/get-object from-vec idx))))
                accumulator)))

  (finish [_ container]
    (finish-maybe-optional (.finisher collector) container)))

(defn ->function-spec ^core2.operator.group_by.AggregateSpec [^String from-name ^String to-name ^Collector collector]
  (FunctionSpec. from-name (t/->primitive-dense-union-field to-name) collector))

(deftype NumberFunctionSpec [^String from-name
                             ^Field field
                             ^Supplier supplier
                             accumulator ;; <ObjLongConsumer|ObjDoubleConsumer>
                             ^Function finisher]
  AggregateSpec
  (getToField [_ in-root]
    field)

  (getFromVector [_ in-root]
    (.getVector in-root from-name))

  (aggregate [this in-root container idx-bitmap]
    (let [from-vec (util/maybe-single-child-dense-union (.getFromVector this in-root))
          acc (if (some? container)
                container
                (.get supplier))
          consumer (cond
                     (instance? BaseIntVector from-vec)
                     (let [^BaseIntVector from-vec from-vec]
                       (reify IntConsumer
                         (accept [_ idx]
                           (.accept ^ObjLongConsumer accumulator acc (.getValueAsLong from-vec idx)))))

                     (instance? FloatingPointVector from-vec)
                     (let [^FloatingPointVector from-vec from-vec]
                       (reify IntConsumer
                         (accept [_ idx]
                           (.accept ^ObjDoubleConsumer accumulator acc (.getValueAsDouble from-vec idx)))))

                     :else
                     (reify IntConsumer
                       (accept [_ idx]
                         (let [v (t/get-object from-vec idx)]
                           (if (integer? v)
                             (.accept ^ObjLongConsumer accumulator acc v)
                             (.accept ^ObjDoubleConsumer accumulator acc v))))))]
      (.forEach ^IntStream (.stream idx-bitmap) consumer)
      acc))

  (finish [_ container]
    (finish-maybe-optional finisher container)))

(defn ->number-function-spec ^core2.operator.group_by.AggregateSpec [^String from-name
                                                                     ^String to-name
                                                                     ^Supplier supplier
                                                                     accumulator
                                                                     ^Function finisher]
  (NumberFunctionSpec. from-name (t/->primitive-dense-union-field to-name) supplier accumulator finisher))

(deftype NumberSummaryStatistics [^LongSummaryStatistics long-summary
                                  ^DoubleSummaryStatistics double-summary])

(def number-summary-supplier (reify Supplier
                               (get [_]
                                 (NumberSummaryStatistics. (LongSummaryStatistics.) (DoubleSummaryStatistics.)))))
(def number-summary-accumulator (reify
                                  ObjLongConsumer
                                  (^void accept [_ acc ^long x]
                                   (.accept ^LongSummaryStatistics (.long-summary ^NumberSummaryStatistics acc) x))

                                  ObjDoubleConsumer
                                  (^void accept [_ acc ^double x]
                                   (.accept ^DoubleSummaryStatistics (.double-summary ^NumberSummaryStatistics acc) x))))
(def number-sum-finisher (reify Function
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
(def number-avg-finisher (reify Function
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
(def number-min-finisher (reify Function
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
(def number-max-finisher (reify Function
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

(defn- ->group-key [^VectorSchemaRoot in-root ^List group-specs ^long idx]
  (reduce
   (fn [^List acc ^GroupSpec group-spec]
     (let [from-vec (.getFromVector group-spec in-root)
           k (util/pointer-or-object from-vec idx)]
       (doto acc
         (.add k))))
   (ArrayList. (.size group-specs))
   group-specs))

(defn- aggregate-groups [^BufferAllocator allocator ^VectorSchemaRoot in-root ^List aggregate-specs ^Map group->accs]
  (let [group->idx-bitmap (HashMap.)
        ^List group-specs (vec (filter #(instance? GroupSpec %) aggregate-specs))]

    (dotimes [idx (.getRowCount in-root)]
      (let [group-key (->group-key in-root group-specs idx)
            ^RoaringBitmap idx-bitmap (.computeIfAbsent group->idx-bitmap group-key (reify Function
                                                                                      (apply [_ _]
                                                                                        (RoaringBitmap.))))]
        (.add idx-bitmap idx)))

    (doseq [[group-key ^RoaringBitmap idx-bitmap] group->idx-bitmap
            :let [^List accs (if-let [accs (.get group->accs group-key)]
                               accs
                               (doto (ArrayList. ^List (repeat (.size aggregate-specs) nil))
                                 (->> (.put group->accs (copy-group-key allocator group-key)))))]]
      (dotimes [n (.size aggregate-specs)]
        (.set accs n (.aggregate ^AggregateSpec (.get aggregate-specs n) in-root (.get accs n) idx-bitmap))))))

(defn- finish-groups ^org.apache.arrow.vector.VectorSchemaRoot [^List aggregate-specs ^Map group->accs ^VectorSchemaRoot out-root]
  (let [^List all-accs (ArrayList. ^List (vals group->accs))
        row-count (.size all-accs)]
    (dotimes [n (.size aggregate-specs)]
      (let [out-vec (.getVector out-root n)]
        (dotimes [out-idx row-count]
          (let [^List accs (.get all-accs out-idx)]
            (let [v (.finish ^AggregateSpec (.get aggregate-specs n) (.get accs n))]
              (if (instance? DenseUnionVector out-vec)
                (let [type-id (.getFlatbufID (.getTypeID ^ArrowType (t/->arrow-type (type v))))
                      value-offset (DenseUnionUtil/writeTypeId out-vec out-idx type-id)]
                  (t/set-safe! (.getVectorByType ^DenseUnionVector out-vec type-id) value-offset v))
                (t/set-safe! out-vec out-idx v)))))))
    (util/set-vector-schema-root-row-count out-root row-count)
    out-root))

(deftype GroupByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^List aggregate-specs
                        ^:unsynchronized-mutable ^VectorSchemaRoot out-root]
  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root)
      (set! (.out-root this) nil))

    (let [group->accs (HashMap.)]
      (try
        (.forEachRemaining in-cursor
                           (reify Consumer
                             (accept [_ in-root]
                               (when-not (.out-root this)
                                 (let [group-by-schema (Schema. (for [^AggregateSpec aggregate-spec aggregate-specs]
                                                                  (.getToField aggregate-spec ^VectorSchemaRoot in-root)))]
                                   (set! (.out-root this) (VectorSchemaRoot/create group-by-schema allocator))))
                               (aggregate-groups allocator in-root aggregate-specs group->accs))))

        (if-not (.isEmpty group->accs)
          (do (.accept c (finish-groups aggregate-specs group->accs out-root))
              true)
          false)
        (finally
          (doseq [k (keys group->accs)]
            (release-group-key k))))))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE))

  (close [_]
    (util/try-close out-root)
    (util/try-close in-cursor)))

(defn ->group-by-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List aggregate-specs]
  (GroupByCursor. allocator in-cursor aggregate-specs nil))
