(ns core2.operator.group-by
  (:require [core2.util :as util]
            [core2.types :as t])
  (:import core2.ICursor
           [java.util ArrayList HashMap List Map]
           [java.util.function BiConsumer Consumer IntConsumer Function Supplier ObjIntConsumer]
           java.util.stream.IntStream
           java.time.LocalDateTime
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector BitVector ElementAddressableVector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.types.pojo ArrowType Field Schema]
           org.apache.arrow.vector.util.Text
           org.roaringbitmap.RoaringBitmap))

;; NOTE: the use of getObject here is problematic, apart from
;; performance, the returned values cannot be set back onto the
;; vectors. set-safe tries to deal with that, but doesn't handle
;; Arrow's own return types at times, for Text, LocalDateTime
;; etc. Further, byte arrays are not hashable.

(definterface AggregateSpec
  (^org.apache.arrow.vector.types.pojo.Field getToField [^org.apache.arrow.vector.VectorSchemaRoot inRoot])
  (^org.apache.arrow.vector.ValueVector getFromVector [^org.apache.arrow.vector.VectorSchemaRoot inRoot])
  (^Object aggregate [^org.apache.arrow.vector.VectorSchemaRoot inRoot ^Object accumulator ^org.roaringbitmap.RoaringBitmap idx-bitmap])
  (^Object finish [^Object accumulator]))

(defn- arrow->clojure [x]
  (cond
    (instance? Text x)
    (str x)

    (instance? LocalDateTime x)
    (util/local-date-time->date x)

    :else
    x))

(deftype GroupSpec [^String name]
  AggregateSpec
  (getToField [this in-root]
    (.getField (.getFromVector this in-root)))

  (getFromVector [_ in-root]
    (.getVector in-root name))

  (aggregate [this in-root accumulator idx-bitmap]
    (or accumulator (.getObject (.getFromVector this in-root) (.first idx-bitmap))))

  (finish [_ accumulator]
    (arrow->clojure accumulator)))

(defn ->group-spec ^core2.operator.group_by.AggregateSpec [^String name]
  (GroupSpec. name))

(deftype FunctionSpec [^String from-name ^Field field aggregate-fn]
  AggregateSpec
  (getToField [_ in-root]
    field)

  (getFromVector [_ in-root]
    (.getVector in-root from-name))

  (aggregate [this in-root accumulator idx-bitmap]
    (let [from-vec (util/maybe-single-child-dense-union (.getFromVector this in-root))]
      (.collect ^IntStream (.stream idx-bitmap)
                (reify Supplier
                  (get [_] (or accumulator (aggregate-fn))))
                (reify ObjIntConsumer
                  (accept [_ accumulator idx]
                    (aggregate-fn accumulator (.getObject from-vec idx))))
                (reify BiConsumer
                  (accept [_ x y]
                    (aggregate-fn x y))))))

  (finish [_ accumulator]
    (arrow->clojure (aggregate-fn accumulator))))

(defn ->function-spec ^core2.operator.group_by.AggregateSpec [^String from-name ^String out-name aggregate-fn]
  (FunctionSpec. from-name (t/->primitive-dense-union-field out-name) aggregate-fn))

(deftype GroupByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^List aggregate-specs
                        ^:unsynchronized-mutable ^VectorSchemaRoot out-root]
  ICursor
  (tryAdvance [this c]
    (util/try-close out-root)
    (if-not out-root
      (let [group-specs (vec (filter #(instance? GroupSpec %) aggregate-specs))
            aggregate-map (HashMap.)]
        (.forEachRemaining in-cursor
                           (reify Consumer
                             (accept [_ in-root]
                               (let [^VectorSchemaRoot in-root in-root]
                                 (when-not out-root
                                   (let [aggregate-schema (Schema. (for [^AggregateSpec aggregate-spec aggregate-specs]
                                                                     (.getToField aggregate-spec in-root)))]
                                     (set! (.out-root this) (VectorSchemaRoot/create aggregate-schema allocator))))

                                 (let [group->idx-bitmap (HashMap.)]
                                   (dotimes [idx (.getRowCount in-root)]
                                     (let [group-key (reduce
                                                      (fn [^List acc ^AggregateSpec group-spec]
                                                        (let [from-vec (.getFromVector group-spec in-root)
                                                              k (cond
                                                                  (instance? DenseUnionVector from-vec)
                                                                  (let [^DenseUnionVector from-vec from-vec
                                                                        from-vec (.getVectorByType from-vec (.getTypeId from-vec idx))]


                                                                    (if (and (instance? ElementAddressableVector from-vec)
                                                                             (not (instance? BitVector from-vec)))
                                                                      (.getDataPointer ^ElementAddressableVector from-vec idx)
                                                                      (.getObject from-vec idx)))

                                                                  (and (instance? ElementAddressableVector from-vec)
                                                                       (not (instance? BitVector from-vec)))
                                                                  (.getDataPointer ^ElementAddressableVector from-vec idx)

                                                                  :else
                                                                  (.getObject from-vec idx))]
                                                          (doto acc
                                                            (.add k))))
                                                      (ArrayList. (.size aggregate-specs))
                                                      group-specs)
                                           ^RoaringBitmap idx-bitmap (.computeIfAbsent group->idx-bitmap group-key (reify Function
                                                                                                                     (apply [_ _]
                                                                                                                       (RoaringBitmap.))))]
                                       (.add idx-bitmap idx)))

                                   (doseq [[group-key ^RoaringBitmap idx-bitmap] group->idx-bitmap
                                           :let [^List accs (.computeIfAbsent aggregate-map
                                                                              group-key
                                                                              (reify Function
                                                                                (apply [_ _]
                                                                                  (ArrayList. ^List (repeat (.size aggregate-specs) nil)))))]]
                                     (dotimes [n (.size aggregate-specs)]
                                       (.set accs n (.aggregate ^AggregateSpec (.get aggregate-specs n) in-root (.get accs n) idx-bitmap)))))))))

        (let [all-accs (vec (vals aggregate-map))
              row-count (count all-accs)]
          (dotimes [out-idx row-count]
            (let [^List accs (get all-accs out-idx)]
              (dotimes [n (.size aggregate-specs)]
                (let [out-vec (.getVector out-root n)
                      v (.finish ^AggregateSpec (.get aggregate-specs n) (.get accs n))]
                  (if (instance? DenseUnionVector out-vec)
                    (let [type-id (.getFlatbufID (.getTypeID ^ArrowType (t/->arrow-type (type v))))
                          value-offset (util/write-type-id out-vec out-idx type-id)]
                      (t/set-safe! (.getVectorByType ^DenseUnionVector out-vec type-id) value-offset v))
                    (t/set-safe! out-vec out-idx v))))))
          (util/set-vector-schema-root-row-count out-root row-count)
          (.accept c out-root)
          true))
      false))

  (close [_]
    (util/try-close out-root)
    (util/try-close in-cursor)))

(defn ->group-by-cursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List aggregate-specs]
  (GroupByCursor. allocator in-cursor aggregate-specs nil))
