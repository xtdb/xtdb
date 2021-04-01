(ns core2.operator.group-by
  (:require [core2.util :as util]
            [core2.types :as t])
  (:import core2.ICursor
           [java.util ArrayList DoubleSummaryStatistics HashMap List LongSummaryStatistics Map Optional Spliterator]
           [java.util.function BiConsumer Consumer Function IntConsumer Supplier ObjDoubleConsumer ObjIntConsumer ObjLongConsumer]
           [java.util.stream Collector Collectors IntStream]
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector BaseIntVector BitVector ElementAddressableVector FloatingPointVector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.types.pojo ArrowType Field Schema]
           org.roaringbitmap.RoaringBitmap))

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
    (or container (.getObject (.getFromVector this in-root) (.first idx-bitmap))))

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
                    (.accept accumulator acc (.getObject from-vec idx))))
                accumulator)))

  (finish [_ container]
    (finish-maybe-optional (.finisher collector) container)))

(defn ->function-spec ^core2.operator.group_by.AggregateSpec [^String from-name ^String to-name ^Collector collector]
  (FunctionSpec. from-name (t/->primitive-dense-union-field to-name) collector))

(deftype DoubleFunctionSpec [^String from-name ^Field field ^Supplier supplier ^ObjDoubleConsumer accumulator ^Function finisher]
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
                           (.accept accumulator acc (.getValueAsLong from-vec idx)))))

                     (instance? FloatingPointVector from-vec)
                     (let [^FloatingPointVector from-vec from-vec]
                       (reify IntConsumer
                         (accept [_ idx]
                           (.accept accumulator acc (.getValueAsDouble from-vec idx)))))

                     :else
                     (reify IntConsumer
                       (accept [_ idx]
                         (.accept accumulator acc (.getObject from-vec idx)))))]
      (.forEach ^IntStream (.stream idx-bitmap) consumer)
      acc))

  (finish [_ container]
    (finish-maybe-optional finisher container)))

(defn ->double-function-spec ^core2.operator.group_by.AggregateSpec [^String from-name
                                                                     ^String to-name
                                                                     ^Supplier supplier
                                                                     ^ObjDoubleConsumer accumulator
                                                                     ^Function finisher]
  (DoubleFunctionSpec. from-name (t/->primitive-dense-union-field to-name) supplier accumulator finisher))

(deftype LongFunctionSpec [^String from-name ^Field field ^Supplier supplier ^ObjLongConsumer accumulator ^Function finisher]
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
                           (.accept accumulator acc (.getValueAsLong from-vec idx)))))

                     (instance? FloatingPointVector from-vec)
                     (let [^FloatingPointVector from-vec from-vec]
                       (reify IntConsumer
                         (accept [_ idx]
                           (.accept accumulator acc (.getValueAsDouble from-vec idx)))))

                     :else
                     (reify IntConsumer
                       (accept [_ idx]
                         (.accept accumulator acc (.getObject from-vec idx)))))]
      (.forEach ^IntStream (.stream idx-bitmap) consumer)
      acc))

  (finish [_ container]
    (finish-maybe-optional finisher container)))

(defn ->long-function-spec ^core2.operator.group_by.AggregateSpec [^String from-name
                                                                   ^String to-name
                                                                   ^Supplier supplier
                                                                   ^ObjLongConsumer accumulator
                                                                   ^Function finisher]
  (LongFunctionSpec. from-name (t/->primitive-dense-union-field to-name) supplier accumulator finisher))

(def long-summary-supplier (reify Supplier
                             (get [_]
                               (LongSummaryStatistics.))))
(def long-summary-accumulator (reify ObjLongConsumer
                                (accept [_ acc x]
                                  (.accept ^LongSummaryStatistics acc x))))
(def long-sum-finisher (reify Function
                         (apply [_ acc]
                           (.getSum ^LongSummaryStatistics acc))))
(def long-avg-finisher (reify Function
                         (apply [_ acc]
                           (.getAverage ^LongSummaryStatistics acc))))
(def double-summary-supplier (reify Supplier
                               (get [_]
                                 (DoubleSummaryStatistics.))))
(def double-summary-accumulator (reify ObjDoubleConsumer
                                  (accept [_ acc x]
                                    (.accept ^DoubleSummaryStatistics acc x))))
(def double-sum-finisher (reify Function
                           (apply [_ acc]
                             (.getSum ^DoubleSummaryStatistics acc))))
(def double-avg-finisher (reify Function
                           (apply [_ acc]
                             (.getAverage ^DoubleSummaryStatistics acc))))

(defn ->avg-long-spec [^String from-name ^String to-name]
  (->long-function-spec from-name to-name
                        long-summary-supplier
                        long-summary-accumulator
                        long-avg-finisher))

(defn ->sum-long-spec [^String from-name ^String to-name]
  (->long-function-spec from-name to-name
                        long-summary-supplier
                        long-summary-accumulator
                        long-sum-finisher))

(defn ->avg-double-spec [^String from-name ^String to-name]
  (->double-function-spec from-name to-name
                          double-summary-supplier
                          double-summary-accumulator
                          double-avg-finisher))

(defn ->sum-double-spec [^String from-name ^String to-name]
  (->double-function-spec from-name to-name
                          double-summary-supplier
                          double-summary-accumulator
                          double-sum-finisher))

(defn ->count-spec [^String from-name ^String to-name]
  (->function-spec from-name to-name (Collectors/counting)))

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
                      value-offset (util/write-type-id out-vec out-idx type-id)]
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
