(ns core2.operator.set
  (:require [core2.util :as util])
  (:import core2.ICursor
           [java.util ArrayList List Set HashSet]
           java.util.function.Consumer
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.VectorSchemaRoot))

(deftype UnionCursor [^BufferAllocator allocator
                      ^ICursor left-cursor
                      ^ICursor right-cursor
                      ^:unsynchronized-mutable ^Schema schema]
  ICursor
  (tryAdvance [this c]
    (if (or (.tryAdvance left-cursor
                         (reify Consumer
                           (accept [_ in-root]
                             (if (nil? (.schema this))
                               (set! (.schema this) (.getSchema ^VectorSchemaRoot in-root))
                               (assert (= (.schema this) (.getSchema ^VectorSchemaRoot in-root))))
                             (.accept c in-root))))
            (.tryAdvance right-cursor
                         (reify Consumer
                           (accept [_ in-root]
                             (if (nil? (.schema this))
                               (set! (.schema this) (.getSchema ^VectorSchemaRoot in-root))
                               (assert (= (.schema this) (.getSchema ^VectorSchemaRoot in-root))))
                             (.accept c in-root)))))
      true
      false))

  (close [_]
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defn- copy-set-key [^BufferAllocator allocator ^List k]
  (dotimes [n (.size k)]
    (let [x (.get k n)]
      (.set k n (util/maybe-copy-pointer allocator x))))
  k)

(defn- release-set-key [k]
  (doseq [x k
          :when (instance? ArrowBufPointer x)]
    (util/try-close (.getBuf ^ArrowBufPointer x)))
  k)

(defn- ->set-key [^VectorSchemaRoot root ^long idx]
  (let [acc (ArrayList. (util/root-field-count root))]
    (dotimes [m (util/root-field-count root)]
      (.add acc (util/pointer-or-object (.getVector root m) idx)))
    acc))

(deftype IntersectionCursor [^BufferAllocator allocator
                             ^ICursor left-cursor
                             ^ICursor right-cursor
                             ^Set intersection-set
                             ^:unsynchronized-mutable ^VectorSchemaRoot out-root
                             ^:unsynchronized-mutable ^Schema schema
                             difference?]
  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root))

    (.forEachRemaining right-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (let [^VectorSchemaRoot in-root in-root]
                             (if (nil? (.schema this))
                               (set! (.schema this) (.getSchema ^VectorSchemaRoot in-root))
                               (assert (= (.schema this) (.getSchema ^VectorSchemaRoot in-root))))
                             (dotimes [n (.getRowCount in-root)]
                               (let [k (->set-key in-root n)]
                                 (when-not (.contains intersection-set k)
                                   (.add intersection-set (copy-set-key k)))))))))

    (if (.tryAdvance left-cursor
                     (reify Consumer
                       (accept [_ in-root]
                         (let [^VectorSchemaRoot in-root in-root]
                           (when (pos? (.getRowCount in-root))
                             (if (nil? (.schema this))
                               (set! (.schema this) (.getSchema ^VectorSchemaRoot in-root))
                               (assert (= (.schema this) (.getSchema ^VectorSchemaRoot in-root))))
                             (let [out-root (VectorSchemaRoot/create (.getSchema in-root) allocator)]
                               (dotimes [n in-root]
                                 (let [match? (.contains intersection-set (->set-key in-root n))
                                       match (if difference?
                                               (not match?)
                                               match?)]
                                   (when match?
                                     (util/copy-tuple in-root n out-root (.getRowCount out-root))
                                     (util/set-vector-schema-root-row-count out-root (inc (.getRowCount out-root))))))
                               (set! (.out-root this) out-root)))))))
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (doseq [k intersection-set]
      (release-set-key k))
    (.clear intersection-set)
    (util/try-close out-root)
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defn ->union-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (UnionCursor. allocator left-cursor right-cursor nil))

(defn ->difference-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (IntersectionCursor. allocator left-cursor right-cursor (HashSet.) nil nil true))

(defn ->intersection-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (IntersectionCursor. allocator left-cursor right-cursor (HashSet.) nil nil false))
