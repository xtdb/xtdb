(ns core2.operator.join
  (:require [core2.util :as util])
  (:import core2.ICursor
           [java.util ArrayList HashMap List Map]
           [java.util.function Consumer Function]
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector BitVector ElementAddressableVector IntVector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.util.VectorBatchAppender))

(defn- ->join-schema ^org.apache.arrow.vector.types.pojo.Schema [^Schema left-schema ^Schema right-schema]
  (let [fields (concat (.getFields left-schema) (.getFields right-schema))]
    (assert (apply distinct? fields))
    (Schema. fields)))

(defn- copy-tuple [^VectorSchemaRoot in-root ^long idx ^VectorSchemaRoot out-root]
  (let [out-idx (.getRowCount out-root)]
    (dotimes [n (util/root-field-count in-root)]
      (let [in-vec (.getVector in-root n)
            out-vec (.getVector out-root (.getField in-vec))]
        (.copyFromSafe out-vec idx out-idx in-vec)))))

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
                             (let [left-vec (util/maybe-single-child-dense-union (.getVector left-root left-column-name))]
                               (dotimes [left-idx (.getValueCount left-vec)]
                                 (let [^IntVector idx-pairs-vec (.computeIfAbsent join-key->left-idx-pairs
                                                                                  (.hashCode left-vec left-idx)
                                                                                  (reify Function
                                                                                    (apply [_ x]
                                                                                      (IntVector. "" allocator))))
                                       idx (.getValueCount idx-pairs-vec)]
                                   (.setValueCount idx-pairs-vec (+ idx 2))
                                   (.set idx-pairs-vec idx left-root-idx)
                                   (.set idx-pairs-vec (inc idx) left-idx))))))))

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
                                           right-vec (util/maybe-single-child-dense-union (.getVector right-root right-column-name))
                                           left-pointer (ArrowBufPointer.)
                                           right-pointer (ArrowBufPointer.)]
                                       (dotimes [right-idx (.getValueCount right-vec)]
                                         (when-let [^IntVector idx-pairs-vec (.get join-key->left-idx-pairs (.hashCode right-vec right-idx))]
                                           (let [^ValueVector right-vec (util/vector-at-index right-vec right-idx)
                                                 right-vec-element-addressable? (util/element-addressable-vector? right-vec)]
                                             (loop [n 0]
                                               (when (< n (.getValueCount idx-pairs-vec))
                                                 (let [left-root-idx (.get idx-pairs-vec n)
                                                       left-idx (.get idx-pairs-vec (inc n))
                                                       left-root ^VectorSchemaRoot (.get left-roots left-root-idx)
                                                       left-vec (.getVector left-root left-column-name)
                                                       ^ValueVector left-vec (util/vector-at-index left-vec left-idx)]
                                                   (when (if right-vec-element-addressable?
                                                           (and (util/element-addressable-vector? left-vec)
                                                                (= (.getDataPointer ^ElementAddressableVector left-vec left-idx left-pointer)
                                                                   (.getDataPointer ^ElementAddressableVector right-vec right-idx right-pointer)))
                                                           (= (.getObject left-vec left-idx)
                                                              (.getObject right-vec right-idx)))
                                                     (copy-tuple left-root left-idx out-root)
                                                     (copy-tuple right-root right-idx out-root))
                                                   (recur (+ n 2))))))
                                           (let [row-count (quot (.getValueCount idx-pairs-vec) 2)]
                                             (util/set-vector-schema-root-row-count out-root (+ (.getRowCount out-root) row-count)))))

                                       (when (pos? (.getRowCount out-root))
                                         (set! (.out-root this) out-root))))))))))

    (if out-root
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (util/try-close out-root)
    (doseq [root left-roots]
      (util/try-close root))
    (.clear left-roots)
    (doseq [idx-pair (vals join-key->left-idx-pairs)]
      (util/try-close idx-pair))
    (.clear join-key->left-idx-pairs)
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defn ->equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                         ^ICursor left-cursor,
                                         ^String left-column-name,
                                         ^ICursor right-cursor,
                                         ^String right-column-name]
  (EquiJoinCursor. allocator left-cursor left-column-name right-cursor right-column-name (HashMap.) (ArrayList.) nil))
