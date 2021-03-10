(ns core2.operator.join
  (:require [core2.util :as util])
  (:import core2.ICursor
           [java.util ArrayList HashMap List Map NavigableMap TreeMap]
           [java.util.function Consumer Function]
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BigIntVector BitVector ElementAddressableVector ValueVector VectorSchemaRoot]
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

(defn- build-phase [^BufferAllocator allocator
                    ^VectorSchemaRoot left-root
                    ^NavigableMap left-idx->root
                    ^Map join-key->left-idxs
                    ^String left-column-name]
  (let [^VectorSchemaRoot left-root (util/slice-root left-root 0)
        left-root-idx  (if-let [left-idx-entry (.lastEntry left-idx->root)]
                         (+ (long (.getKey left-idx-entry))
                            (.getRowCount ^VectorSchemaRoot (.getValue left-idx-entry)))
                         0)]
    (.put left-idx->root left-root-idx left-root)
    (let [left-vec (util/maybe-single-child-dense-union (.getVector left-root left-column-name))]
      (dotimes [left-idx (.getValueCount left-vec)]
        (let [^BigIntVector left-idxs-vec (.computeIfAbsent join-key->left-idxs
                                                            (.hashCode left-vec left-idx)
                                                            (reify Function
                                                              (apply [_ x]
                                                                (doto (BigIntVector. "" allocator)
                                                                  (.setInitialCapacity 32)))))
              idx (.getValueCount left-idxs-vec)
              total-left-idx (+ left-root-idx left-idx)]
          (.setValueCount left-idxs-vec (inc idx))
          (.set left-idxs-vec idx total-left-idx))))))

(defn- probe-phase ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator allocator
                                                              ^VectorSchemaRoot right-root
                                                              ^NavigableMap left-idx->root
                                                              ^Map join-key->left-idxs-vec
                                                              ^String left-column-name
                                                              ^String right-column-name]
  (when (pos? (.getRowCount right-root))
    (let [left-schema (.getSchema ^VectorSchemaRoot (.getValue (.firstEntry left-idx->root)))
          join-schema (->join-schema left-schema (.getSchema right-root))
          out-root (VectorSchemaRoot/create join-schema allocator)
          right-vec (util/maybe-single-child-dense-union (.getVector right-root right-column-name))
          left-pointer (ArrowBufPointer.)
          right-pointer (ArrowBufPointer.)]
      (dotimes [right-idx (.getValueCount right-vec)]
        (when-let [^BigIntVector left-idxs-vec (.get join-key->left-idxs-vec (.hashCode right-vec right-idx))]
          (let [right-pointer-or-object (util/pointer-or-object right-vec right-idx right-pointer)]
            (loop [n 0
                   matches 0]
              (if (= n (.getValueCount left-idxs-vec))
                (util/set-vector-schema-root-row-count out-root (+ (.getRowCount out-root) matches))
                (let [total-left-idx (long (.get left-idxs-vec n))
                      left-idx-entry (.floorEntry left-idx->root total-left-idx)
                      ^long left-root-idx (.getKey left-idx-entry)
                      ^VectorSchemaRoot left-root (.getValue left-idx-entry)
                      left-idx (- total-left-idx left-root-idx)
                      left-vec (.getVector left-root left-column-name)]
                  (if (= (util/pointer-or-object left-vec left-idx left-pointer) right-pointer-or-object)
                    (do (copy-tuple left-root left-idx out-root)
                        (copy-tuple right-root right-idx out-root)
                        (recur (inc n) (inc matches)))
                    (recur (inc n) matches))))))))
      out-root)))

(deftype EquiJoinCursor [^BufferAllocator allocator
                         ^ICursor left-cursor
                         ^String left-column-name
                         ^ICursor right-cursor
                         ^String right-column-name
                         ^Map join-key->left-idx-pairs
                         ^NavigableMap left-idx->root
                         ^:unsynchronized-mutable ^VectorSchemaRoot out-root]

  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root)
      (set! (.out-root this) nil))

    (.forEachRemaining left-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (build-phase allocator in-root left-idx->root join-key->left-idx-pairs left-column-name))))

    (while (and (not (.isEmpty left-idx->root))
                (nil? out-root)
                (.tryAdvance right-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (when-let [out-root (probe-phase allocator in-root left-idx->root join-key->left-idx-pairs
                                                                  left-column-name right-column-name)]
                                   (when (pos? (.getRowCount out-root))
                                     (set! (.out-root this) out-root))))))))

    (if out-root
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (util/try-close out-root)
    (doseq [root (vals left-idx->root)]
      (util/try-close root))
    (.clear left-idx->root)
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
  (EquiJoinCursor. allocator left-cursor left-column-name right-cursor right-column-name (HashMap.) (TreeMap.) nil))
