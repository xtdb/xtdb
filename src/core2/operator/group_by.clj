(ns core2.operator.group-by
  (:require [core2.util :as util]
            [core2.types :as t])
  (:import core2.ICursor
           [java.util ArrayList HashMap List Map Optional]
           [java.util.function BiConsumer Consumer IntConsumer Function Supplier ObjIntConsumer]
           [java.util.stream Collector IntStream]
           java.time.LocalDateTime
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector BitVector ElementAddressableVector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.types.pojo ArrowType Field Schema]
           org.apache.arrow.vector.util.Text
           org.roaringbitmap.RoaringBitmap))

(definterface AggregateSpec
  (^org.apache.arrow.vector.types.pojo.Field getToField [^org.apache.arrow.vector.VectorSchemaRoot inRoot])
  (^org.apache.arrow.vector.ValueVector getFromVector [^org.apache.arrow.vector.VectorSchemaRoot inRoot])
  (^Object aggregate [^org.apache.arrow.vector.VectorSchemaRoot inRoot ^Object init ^org.roaringbitmap.RoaringBitmap idx-bitmap])
  (^Object finish [^Object accumulator]))

(deftype GroupSpec [^String name]
  AggregateSpec
  (getToField [this in-root]
    (.getField (.getFromVector this in-root)))

  (getFromVector [_ in-root]
    (.getVector in-root name))

  (aggregate [this in-root init idx-bitmap]
    (or init (.getObject (.getFromVector this in-root) (.first idx-bitmap))))

  (finish [_ acc]
    acc))

(defn ->group-spec ^core2.operator.group_by.AggregateSpec [^String name]
  (GroupSpec. name))

(deftype FunctionSpec [^String from-name ^Field field ^Collector collector]
  AggregateSpec
  (getToField [_ in-root]
    field)

  (getFromVector [_ in-root]
    (.getVector in-root from-name))

  (aggregate [this in-root init idx-bitmap]
    (let [from-vec (util/maybe-single-child-dense-union (.getFromVector this in-root))
          accumulator (.accumulator collector)]
      (.collect ^IntStream (.stream idx-bitmap)
                (if init
                  (reify Supplier
                    (get [_] init))
                  (.supplier collector))
                (reify ObjIntConsumer
                  (accept [_ acc idx]
                    (.accept accumulator acc (.getObject from-vec idx))))
                accumulator)))

  (finish [_ acc]
    (let [result (.apply (.finisher collector) acc)]
      (if (instance? Optional result)
        (.orElse ^Optional result nil)
        result))))

(defn ->function-spec ^core2.operator.group_by.AggregateSpec [^String from-name ^String to-name ^Collector collector]
  (FunctionSpec. from-name (t/->primitive-dense-union-field to-name) collector))

(defn- retain-group-key [k]
  (doseq [x k
          :when (instance? ArrowBufPointer x)]
    (.retain (.getBuf ^ArrowBufPointer x)))
  k)

(defn- release-group-key [k]
  (doseq [x k
          :when (instance? ArrowBufPointer x)]
    (.release (.getBuf ^ArrowBufPointer x)))
  k)

(deftype GroupByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^List aggregate-specs
                        ^:unsynchronized-mutable ^VectorSchemaRoot out-root]
  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root)
      (set! (.out-root this) nil))

    (let [^List group-specs (vec (filter #(instance? GroupSpec %) aggregate-specs))
          aggregate-map (HashMap.)]
      (try
        (.forEachRemaining in-cursor
                           (reify Consumer
                             (accept [_ in-root]
                               (let [^VectorSchemaRoot in-root in-root
                                     group->idx-bitmap (HashMap.)]
                                 (when-not out-root
                                   (let [aggregate-schema (Schema. (for [^AggregateSpec aggregate-spec aggregate-specs]
                                                                     (.getToField aggregate-spec in-root)))]
                                     (set! (.out-root this) (VectorSchemaRoot/create aggregate-schema allocator))))

                                 (dotimes [idx (.getRowCount in-root)]
                                   (let [group-key (reduce
                                                    (fn [^List acc ^GroupSpec group-spec]
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
                                                    (ArrayList. (.size group-specs))
                                                    group-specs)
                                         ^RoaringBitmap idx-bitmap (.computeIfAbsent group->idx-bitmap group-key (reify Function
                                                                                                                   (apply [_ _]
                                                                                                                     (RoaringBitmap.))))]
                                     (.add idx-bitmap idx)))

                                 (doseq [[group-key ^RoaringBitmap idx-bitmap] group->idx-bitmap
                                         :let [^List accs (if-let [accs (.get aggregate-map group-key)]
                                                            accs
                                                            (doto (ArrayList. ^List (repeat (.size aggregate-specs) nil))
                                                              (->> (.put aggregate-map (retain-group-key group-key)))))]]
                                   (dotimes [n (.size aggregate-specs)]
                                     (.set accs n (.aggregate ^AggregateSpec (.get aggregate-specs n) in-root (.get accs n) idx-bitmap))))))))

        (if-not (.isEmpty aggregate-map)
          (let [^List all-accs (ArrayList. ^List (vals aggregate-map))
                row-count (.size all-accs)]
            (dotimes [out-idx row-count]
              (let [^List accs (.get all-accs out-idx)]
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
            true)
          false)
        (finally
          (doseq [k (keys aggregate-map)]
            (release-group-key k))))))

  (close [_]
    (util/try-close out-root)
    (util/try-close in-cursor)))

(defn ->group-by-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List aggregate-specs]
  (GroupByCursor. allocator in-cursor aggregate-specs nil))
