(ns core2.operator.join
  (:require [core2.util :as util])
  (:import core2.ICursor
           [java.util ArrayList HashMap List Map]
           [java.util.function Consumer Function]
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector ElementAddressableVector VectorSchemaRoot]
           org.apache.arrow.vector.types.pojo.Schema))

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
                                     right-schema (.getSchema right-root)
                                     fields (concat (.getFields left-schema) (.getFields right-schema))]
                                 (assert (apply distinct? fields))
                                 (set! (.out-root this) (VectorSchemaRoot/create (Schema. fields) allocator))))

                             (doseq [^VectorSchemaRoot left-root left-roots]
                               (dotimes [left-idx (.getRowCount left-root)]
                                 (dotimes [right-idx (.getRowCount right-root)]
                                   (let [out-idx (.getRowCount out-root)]
                                     (doseq [^ValueVector v (.getFieldVectors left-root)]
                                       (.copyFromSafe (.getVector out-root (.getField v)) out-idx left-idx v))
                                     (doseq [^ValueVector v (.getFieldVectors right-root)]
                                       (.copyFromSafe (.getVector out-root (.getField v)) out-idx right-idx v))
                                     (util/set-vector-schema-root-row-count out-root (inc out-idx))))))))))
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

;; TODO: will not work with DUVs or BitVectors.

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
                               (let [^ElementAddressableVector left-vec (.getVector left-root left-column-name)
                                     left-pointer (ArrowBufPointer.)]
                                 (dotimes [left-idx (.getValueCount left-vec)]
                                   (.getDataPointer left-vec left-idx left-pointer)
                                   (let [^List left-indexes (.computeIfAbsent join-map left-pointer (reify Function
                                                                                                      (apply [_ x]
                                                                                                        (ArrayList.))))]
                                     (.add left-indexes (doto (int-array 2)
                                                          (aset 0 left-root-idx)
                                                          (aset 1 left-idx)))))))))))

    (if (and left-roots (.isEmpty left-roots))
      false
      (if (.tryAdvance right-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (let [^VectorSchemaRoot right-root in-root]
                             (when-not out-root
                               (let [left-schema (.getSchema ^VectorSchemaRoot (first left-roots))
                                     right-schema (.getSchema right-root)
                                     fields (concat (.getFields left-schema) (.getFields right-schema))]
                                 (assert (apply distinct? fields))
                                 (set! (.out-root this) (VectorSchemaRoot/create (Schema. fields) allocator))))

                             (let [^ElementAddressableVector right-vec (.getVector right-root right-column-name)
                                   right-pointer (ArrowBufPointer.)]
                               (dotimes [n (.getValueCount right-vec)]
                                 (.getDataPointer right-vec right-pointer)
                                 (let [left-indexes (.get join-map right-pointer)]
                                   (doseq [^ints idx-pair left-indexes
                                           :let [left-root-idx (aget idx-pair 0)
                                                 left-idx (aget idx-pair 1)
                                                 left-root ^VectorSchemaRoot (.get left-roots left-root-idx)]]
                                     (dotimes [right-idx (.getRowCount right-root)]
                                       (let [out-idx (.getRowCount out-root)]
                                         (doseq [^ValueVector v (.getFieldVectors left-root)]
                                           (.copyFromSafe (.getVector out-root (.getField v)) out-idx left-idx v))
                                         (doseq [^ValueVector v (.getFieldVectors right-root)]
                                           (.copyFromSafe (.getVector out-root (.getField v)) out-idx right-idx v))
                                         (util/set-vector-schema-root-row-count out-root (inc out-idx))))))))))))
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
