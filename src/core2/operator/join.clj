(ns core2.operator.join
  (:require [core2.util :as util])
  (:import core2.ICursor
           [java.util ArrayList List]
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.VectorSchemaRoot
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
                               (dotimes [n (.getRowCount left-root)]
                                 (dotimes [m (.getRowCount right-root)]
                                   (let [out-idx (.getRowCount out-root)]
                                     (doseq [^ValueVector v (.getFieldVectors left-root)]
                                       (.copyFromSafe (.getVector out-root (.getField v)) out-idx n v))
                                     (doseq [^ValueVector v (.getFieldVectors right-root)]
                                       (.copyFromSafe (.getVector out-root (.getField v)) out-idx m v))
                                     (util/set-vector-schema-root-row-count out-root (inc out-idx))))))))))
        (do
          (.accept c out-root)
          true)
        false)))

  (close [_]
    (util/try-close out-root)
    (doseq [root left-roots]
      (util/try-close left-roots))
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defn ->cross-join-cursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (CrossJoinCursor. allocator left-cursor right-cursor nil nil))
