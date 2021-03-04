(ns core2.operator.join
  (:require [core2.util :as util])
  (:import core2.ICursor
           [java.util ArrayList HashMap List Map]
           [java.util.function Consumer Function]
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector BitVector ElementAddressableVector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.util.VectorBatchAppender))

(defn- ->join-schema ^org.apache.arrow.vector.types.pojo.Schema [^Schema left-schema ^Schema right-schema]
  (let [fields (concat (.getFields left-schema) (.getFields right-schema))]
    (assert (apply distinct? fields))
    (Schema. fields)))

(defn- copy-tuple [^VectorSchemaRoot in-root ^long idx ^VectorSchemaRoot out-root]
  (let [out-idx (.getRowCount out-root)]
    (dotimes [n (util/root-field-count in-root)]
      (let [^ValueVector in-vec (.getVector in-root n)
            out-vec (.getVector out-root (.getField in-vec))]
        (.copyFromSafe out-vec idx out-idx in-vec)))))

(defn- copy-tuple-repeated [^VectorSchemaRoot in-root ^long idx ^VectorSchemaRoot out-root ^long repeat]
  (let [out-idx (.getRowCount out-root)]
    (dotimes [n (util/root-field-count in-root)]
      (let [^ValueVector in-vec (.getVector in-root n)
            out-vec (.getVector out-root (.getField in-vec))]
        (util/set-value-count out-vec (+ out-idx repeat))
        (dotimes [m repeat]
          (.copyFrom out-vec idx (+ out-idx m) in-vec))))))

(defn- cross-product [^VectorSchemaRoot left-root ^long left-idx ^VectorSchemaRoot right-root ^VectorSchemaRoot out-root]
  (let [out-idx (.getRowCount out-root)
        row-count (.getRowCount right-root)]
    (dotimes [n (util/root-field-count right-root)]
      (let [^ValueVector right-vec (.getVector right-root n)
            out-vec (.getVector out-root (.getField right-vec))]
        (VectorBatchAppender/batchAppend out-vec (into-array ValueVector [right-vec]))))
    (dotimes [n (util/root-field-count left-root)]
      (let [^ValueVector left-vec (.getVector left-root n)
            out-vec (.getVector out-root (.getField left-vec))]
        (util/set-value-count out-vec (+ out-idx row-count))
        (dotimes [m row-count]
          (.copyFrom out-vec left-idx (+ out-idx m) left-vec))))
    (util/set-vector-schema-root-row-count out-root (+ out-idx row-count))))

(deftype CrossJoinCursor [^BufferAllocator allocator
                          ^ICursor left-cursor
                          ^ICursor right-cursor
                          ^List left-roots
                          ^:unsynchronized-mutable ^VectorSchemaRoot out-root]
  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root)
      (set! (.out-root this) nil))

    (.forEachRemaining left-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (.add left-roots (util/slice-root in-root 0)))))

    (while (and (not (.isEmpty left-roots))
                (nil? out-root)
                (.tryAdvance right-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (let [^VectorSchemaRoot right-root in-root]
                                   (when (pos? (.getRowCount right-root))
                                     (let [left-schema (.getSchema ^VectorSchemaRoot (first left-roots))
                                           join-schema (->join-schema left-schema (.getSchema right-root))]
                                       (set! (.out-root this) (VectorSchemaRoot/create join-schema allocator)))

                                     (doseq [^VectorSchemaRoot left-root left-roots]
                                       (dotimes [left-idx (.getRowCount left-root)]
                                         (cross-product left-root left-idx right-root (.out-root this)))))))))))
    (if out-root
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (util/try-close out-root)
    (when left-roots
      (doseq [root left-roots]
        (util/try-close root))
      (.clear left-roots))
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defn ->cross-join-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (CrossJoinCursor. allocator left-cursor right-cursor (ArrayList.) nil))

(deftype EquiJoinCursor [^BufferAllocator allocator
                         ^ICursor left-cursor
                         ^String left-column-name
                         ^ICursor right-cursor
                         ^String right-column-name
                         ^Map join-key->left-idx-pairs
                         ^List left-roots
                         ^:unsynchronized-mutable ^VectorSchemaRoot out-root]

  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root)
      (set! (.out-root this) nil))

    (.forEachRemaining left-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (let [^VectorSchemaRoot left-root (util/slice-root in-root 0)
                                 left-root-idx (.size left-roots)]
                             (.add left-roots left-root)
                             (let [build-phase (fn [^long left-idx left-key]
                                                 (-> ^List (.computeIfAbsent join-key->left-idx-pairs
                                                                             left-key
                                                                             (reify Function
                                                                               (apply [_ x]
                                                                                 (ArrayList.))))
                                                     (.add (doto (int-array 2)
                                                             (aset 0 left-root-idx)
                                                             (aset 1 left-idx)))))
                                   left-vec (util/maybe-single-child-dense-union (.getVector left-root left-column-name))]
                               (cond
                                 (instance? DenseUnionVector left-vec)
                                 (let [^DenseUnionVector left-vec left-vec]
                                   (dotimes [left-idx (.getValueCount left-vec)]
                                     (let [left-vec (.getVectorByType left-vec (.getTypeId left-vec left-idx))]
                                       (build-phase left-idx (if (and (instance? ElementAddressableVector left-vec)
                                                                      (not (instance? BitVector left-vec)))
                                                               (.getDataPointer ^ElementAddressableVector left-vec left-idx)
                                                               (.getObject left-vec left-idx))))))

                                 (and (instance? ElementAddressableVector left-vec)
                                      (not (instance? BitVector left-vec)))
                                 (dotimes [left-idx (.getValueCount left-vec)]
                                   (build-phase left-idx (.getDataPointer ^ElementAddressableVector left-vec left-idx)))

                                 :else
                                 (dotimes [left-idx (.getValueCount left-vec)]
                                   (build-phase left-idx (.getObject left-vec left-idx)))))))))

    (while (and (not (.isEmpty left-roots))
                (nil? out-root)
                (.tryAdvance right-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (let [^VectorSchemaRoot right-root in-root]
                                   (when (pos? (.getRowCount right-root))
                                     (let [left-schema (.getSchema ^VectorSchemaRoot (first left-roots))
                                           join-schema (->join-schema left-schema (.getSchema right-root))
                                           out-root (VectorSchemaRoot/create join-schema allocator)
                                           probe-phase (fn [^long right-idx right-key]
                                                         (when-let [^List idx-pairs (.get join-key->left-idx-pairs right-key)]
                                                           (doseq [^ints idx-pair idx-pairs
                                                                   :let [left-root-idx (aget idx-pair 0)
                                                                         left-idx (aget idx-pair 1)
                                                                         left-root ^VectorSchemaRoot (.get left-roots left-root-idx)]]
                                                             (copy-tuple left-root left-idx out-root))
                                                           (copy-tuple-repeated right-root right-idx out-root (.size idx-pairs))
                                                           (util/set-vector-schema-root-row-count out-root (+ (.getRowCount out-root) (.size idx-pairs)))))
                                           right-pointer (ArrowBufPointer.)
                                           right-vec (util/maybe-single-child-dense-union (.getVector right-root right-column-name))]
                                       (cond
                                         (instance? DenseUnionVector right-vec)
                                         (let [^DenseUnionVector right-vec right-vec]
                                           (dotimes [right-idx (.getValueCount right-vec)]
                                             (let [right-vec (.getVectorByType right-vec (.getTypeId right-vec right-idx))]
                                               (probe-phase right-idx
                                                            (if (and (instance? ElementAddressableVector right-vec)
                                                                     (not (instance? BitVector right-vec)))
                                                              (.getDataPointer ^ElementAddressableVector right-vec right-idx right-pointer)
                                                              (.getObject right-vec right-idx))))))

                                         (and (instance? ElementAddressableVector right-vec)
                                              (not (instance? BitVector right-vec)))
                                         (dotimes [right-idx (.getValueCount right-vec)]
                                           (probe-phase right-idx (.getDataPointer ^ElementAddressableVector right-vec right-idx right-pointer)))

                                         :else
                                         (dotimes [right-idx (.getValueCount right-vec)]
                                           (probe-phase right-idx (.getObject right-vec right-idx))))

                                       (when (pos? (.getRowCount out-root))
                                         (set! (.out-root this) out-root))))))))))

    (if out-root
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (util/try-close out-root)
    (when left-roots
      (doseq [root left-roots]
        (util/try-close root))
      (.clear left-roots))
    (.clear join-key->left-idx-pairs)
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defn ->equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                         ^ICursor left-cursor,
                                         ^String left-column-name,
                                         ^ICursor right-cursor,
                                         ^String right-column-name]
  (EquiJoinCursor. allocator left-cursor left-column-name right-cursor right-column-name (HashMap.) (ArrayList.) nil))
