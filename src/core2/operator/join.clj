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

(defn- cross-product [^VectorSchemaRoot left-root ^long left-idx ^VectorSchemaRoot right-root ^VectorSchemaRoot out-root]
  (doseq [^ValueVector right-vec (.getFieldVectors right-root)]
    (VectorBatchAppender/batchAppend (.getVector out-root (.getField right-vec)) (object-array [right-vec])))
  (let [out-idx (.getRowCount out-root)]
    (doseq [^ValueVector left-vec (.getFieldVectors left-root)
            :let [out-vec (.getVector out-root (.getField left-vec))]]
      (dotimes [right-idx (.getRowCount right-root)]
        (.copyFromSafe out-vec (+ out-idx right-idx) left-idx left-vec)))
    (util/set-vector-schema-root-row-count out-root (+ out-idx (.getRowCount right-root))))
  out-root)

(deftype CrossJoinCursor [^BufferAllocator allocator
                          ^ICursor left-cursor
                          ^ICursor right-cursor
                          ^:unsynchronized-mutable ^List left-roots
                          ^:unsynchronized-mutable ^VectorSchemaRoot out-root]
  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root))

    (when-not left-roots
      (set! (.left-roots this) (ArrayList.))
      (.forEachRemaining left-cursor
                         (reify Consumer
                           (accept [_ in-root]
                             (.add left-roots (util/slice-root in-root 0))))))

    (if (and left-roots (.isEmpty left-roots))
      false
      (if (.tryAdvance right-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (let [^VectorSchemaRoot right-root in-root]
                             (when-not out-root
                               (let [left-schema (.getSchema ^VectorSchemaRoot (first left-roots))
                                     join-schema (->join-schema left-schema (.getSchema right-root))]
                                 (set! (.out-root this) (VectorSchemaRoot/create join-schema allocator))))

                             (doseq [^VectorSchemaRoot left-root left-roots]
                               (dotimes [left-idx (.getRowCount left-root)]
                                 (cross-product left-root left-idx right-root out-root)))))))
        (do
          (.accept c out-root)
          true)
        false)))

  (close [_]
    (util/try-close out-root)
    (when left-roots
      (doseq [root left-roots]
        (util/try-close left-roots))
      (.clear left-roots))
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defn ->cross-join-cursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (CrossJoinCursor. allocator left-cursor right-cursor nil nil))

(deftype EquiJoinCursor [^BufferAllocator allocator
                         ^ICursor left-cursor
                         ^String left-column-name
                         ^ICursor right-cursor
                         ^String right-column-name
                         ^Map join-map
                         ^:unsynchronized-mutable ^List left-roots
                         ^:unsynchronized-mutable ^VectorSchemaRoot out-root]

  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root))

    (when-not left-roots
      (set! (.left-roots this) (ArrayList.))
      (.forEachRemaining left-cursor
                         (reify Consumer
                           (accept [_ in-root]
                             (let [^VectorSchemaRoot left-root in-root
                                   left-root-idx (.size left-roots)]
                               (.add left-roots (util/slice-root left-root 0))
                               (let [build-phase (fn [^long left-idx left-key]
                                                   (-> ^List (.computeIfAbsent join-map left-key (reify Function
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
                                       (let [^ElementAddressableVector left-vec (.getVectorByType left-vec (.getTypeId left-vec left-idx))]
                                         (build-phase left-idx (.getDataPointer left-vec left-idx)))))

                                   (instance? BitVector left-vec)
                                   (dotimes [left-idx (.getValueCount left-vec)]
                                     (build-phase left-idx (= (.get ^BitVector left-vec left-idx) 1)))

                                   :else
                                   (dotimes [left-idx (.getValueCount left-vec)]
                                     (build-phase left-idx (.getDataPointer ^ElementAddressableVector left-vec left-idx))))))))))

    (if (and left-roots (.isEmpty left-roots))
      false
      (if (.tryAdvance right-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (let [^VectorSchemaRoot right-root in-root]
                             (when-not out-root
                               (let [left-schema (.getSchema ^VectorSchemaRoot (first left-roots))
                                     join-schema (->join-schema left-schema (.getSchema right-root))]
                                 (set! (.out-root this) (VectorSchemaRoot/create join-schema allocator))))

                             (let [probe-phase (fn [right-pointer]
                                                 (doseq [^ints idx-pair (.get join-map right-pointer)
                                                         :let [left-root-idx (aget idx-pair 0)
                                                               left-idx (aget idx-pair 1)
                                                               left-root ^VectorSchemaRoot (.get left-roots left-root-idx)]]
                                                   (cross-product left-root left-idx right-root out-root)))
                                   right-pointer (ArrowBufPointer.)
                                   right-vec (util/maybe-single-child-dense-union (.getVector right-root right-column-name))]
                               (cond
                                 (instance? DenseUnionVector right-vec)
                                 (let [^DenseUnionVector right-vec right-vec]
                                   (dotimes [right-idx (.getValueCount right-vec)]
                                     (let [^ElementAddressableVector right-vec (.getVectorByType right-vec (.getTypeId right-vec right-idx))]
                                       (probe-phase (.getDataPointer right-vec right-idx right-pointer)))))

                                 (instance? BitVector right-vec)
                                 (dotimes [right-idx (.getValueCount right-vec)]
                                   (probe-phase (= (.get ^BitVector right-vec right-idx) 1)))

                                 :else
                                 (dotimes [right-idx (.getValueCount right-vec)]
                                   (probe-phase (.getDataPointer ^ElementAddressableVector right-vec right-idx right-pointer)))))))))
        (do
          (.accept c out-root)
          true)
        false)))

  (close [_]
    (util/try-close out-root)
    (when left-roots
      (doseq [root left-roots]
        (util/try-close left-roots))
      (.clear left-roots))
    (.clear join-map)
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defn ->equi-join-cursor [^BufferAllocator allocator, ^ICursor left-cursor, ^String left-column-name, ^ICursor right-cursor, ^String right-column-name]
  (EquiJoinCursor. allocator left-cursor left-column-name right-cursor right-column-name (HashMap.) nil nil))
