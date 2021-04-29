(ns core2.operator.set
  (:require [core2.util :as util])
  (:import core2.IChunkCursor
           [java.util ArrayList List Set HashSet]
           java.util.function.Consumer
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector.types.pojo Field Schema]
           org.apache.arrow.vector.VectorSchemaRoot))

(set! *unchecked-math* :warn-on-boxed)

(defn- assert-union-compatible [^Schema x ^Schema y]
  (assert (= (for [^Field field (.getFields x)]
               (.getName field))
             (for [^Field field (.getFields y)]
               (.getName field)))))

(deftype UnionCursor [^IChunkCursor left-cursor
                      ^IChunkCursor right-cursor
                      ^:unsynchronized-mutable ^Schema schema]
  IChunkCursor
  (getSchema [_]
    (assert (= (.getSchema left-cursor) (.getSchema right-cursor)))
    (.getSchema left-cursor))

  (tryAdvance [this c]
    (if (or (.tryAdvance left-cursor
                         (reify Consumer
                           (accept [_ in-root]
                             (let [^VectorSchemaRoot in-root in-root]
                               (when (pos? (.getRowCount in-root))
                                 (if (nil? (.schema this))
                                   (set! (.schema this) (.getSchema in-root))
                                   (assert-union-compatible (.schema this) (.getSchema in-root)))
                                 (.accept c in-root))))))
            (.tryAdvance right-cursor
                         (reify Consumer
                           (accept [_ in-root]
                             (let [^VectorSchemaRoot in-root in-root]
                               (when (pos? (.getRowCount in-root))
                                 (if (nil? (.schema this))
                                   (set! (.schema this) (.getSchema in-root))
                                   (assert-union-compatible (.schema this) (.getSchema in-root)))
                                 (.accept c in-root)))))))
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
                             ^IChunkCursor left-cursor
                             ^IChunkCursor right-cursor
                             ^Set intersection-set
                             ^:unsynchronized-mutable ^VectorSchemaRoot out-root
                             ^:unsynchronized-mutable ^Schema schema
                             difference?]
  IChunkCursor
  (getSchema [_]
    (assert (= (.getSchema left-cursor) (.getSchema right-cursor)))
    (.getSchema left-cursor))

  (tryAdvance [this c]
    (when out-root
      (.close out-root)
      (set! (.out-root this) nil))

    (.forEachRemaining right-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (let [^VectorSchemaRoot in-root in-root]
                             (when (pos? (.getRowCount in-root))
                               (if (nil? (.schema this))
                                 (set! (.schema this) (.getSchema in-root))
                                 (assert-union-compatible (.schema this) (.getSchema in-root)))
                               (dotimes [n (.getRowCount in-root)]
                                 (let [k (->set-key in-root n)]
                                   (when-not (.contains intersection-set k)
                                     (.add intersection-set (copy-set-key allocator k))))))))))

    (while (and (nil? out-root)
                (.tryAdvance left-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (let [^VectorSchemaRoot in-root in-root]
                                   (when (pos? (.getRowCount in-root))
                                     (if (nil? (.schema this))
                                       (set! (.schema this) (.getSchema ^VectorSchemaRoot in-root))
                                       (assert-union-compatible (.schema this) (.getSchema in-root)))
                                     (let [out-root (VectorSchemaRoot/create (.getSchema in-root) allocator)]
                                       (dotimes [n (.getRowCount in-root)]
                                         (let [match? (.contains intersection-set (->set-key in-root n))
                                               match? (if difference?
                                                        (not match?)
                                                        match?)]
                                           (when match?
                                             (util/copy-tuple in-root n out-root (.getRowCount out-root))
                                             (util/set-vector-schema-root-row-count out-root (inc (.getRowCount out-root))))))
                                       (when (pos? (.getRowCount out-root))
                                         (set! (.out-root this) out-root))))))))))

    (if out-root
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

(deftype DistinctCursor [^BufferAllocator allocator
                         ^IChunkCursor in-cursor
                         ^Set seen-set
                         ^:unsynchronized-mutable ^VectorSchemaRoot out-root
                         ^:unsynchronized-mutable ^Schema schema]
  IChunkCursor
  (getSchema [_] (.getSchema in-cursor))

  (tryAdvance [this c]
    (when out-root
      (.close out-root)
      (set! (.out-root this) nil))

    (while (and (nil? out-root)
                (.tryAdvance in-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (let [^VectorSchemaRoot in-root in-root]
                                   (when (pos? (.getRowCount in-root))
                                     (if (nil? (.schema this))
                                       (set! (.schema this) (.getSchema ^VectorSchemaRoot in-root))
                                       (assert-union-compatible (.schema this) (.getSchema in-root)))
                                     (let [out-root (VectorSchemaRoot/create (.getSchema in-root) allocator)]
                                       (dotimes [n (.getRowCount in-root)]
                                         (let [k (->set-key in-root n)]
                                           (when-not (.contains seen-set k)
                                             (.add seen-set (copy-set-key allocator k))
                                             (util/copy-tuple in-root n out-root (.getRowCount out-root))
                                             (util/set-vector-schema-root-row-count out-root (inc (.getRowCount out-root))))))
                                       (when (pos? (.getRowCount out-root))
                                         (set! (.out-root this) out-root))))))))))

    (if out-root
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (doseq [k seen-set]
      (release-set-key k))
    (.clear seen-set)
    (util/try-close out-root)
    (util/try-close in-cursor)))

(definterface ICursorFactory
  (^core2.IChunkCursor createCursor []))

(definterface IFixpointCursorFactory
  (^core2.IChunkCursor createCursor [^core2.operator.set.ICursorFactory cursor-factory]))

;; https://core.ac.uk/download/pdf/11454271.pdf "Algebraic optimization of recursive queries"
;; http://webdam.inria.fr/Alice/pdfs/Chapter-14.pdf "Recursion and Negation"

(deftype FixpointResultCursor [^VectorSchemaRoot out-root ^long fixpoint-offset ^long fixpoint-size ^:volatile-mutable ^boolean done?]
  IChunkCursor
  (getSchema [_] (.getSchema out-root))

  (tryAdvance [this c]
    (if (and (not done?) (pos? fixpoint-size))
      (do (set! (.done? this) true)
          (with-open [^VectorSchemaRoot out-root (util/slice-root (.out-root this)
                                                                  fixpoint-offset
                                                                  (- fixpoint-size fixpoint-offset))]
            (.accept c out-root))
          true)
      false)))

(deftype FixpointCursor [^BufferAllocator allocator
                         ^IChunkCursor base-cursor
                         ^IFixpointCursorFactory recursive-cursor-factory
                         ^Set fixpoint-set
                         ^:unsynchronized-mutable ^VectorSchemaRoot out-root
                         ^:unsynchronized-mutable done?
                         incremental?]
  IChunkCursor
  (getSchema [_] (.getSchema base-cursor))

  (tryAdvance [this c]
    (if done?
      false
      (let [schema (.getSchema this)]
        (set! done? true)

        (let [c (reify Consumer
                  (accept [_ in-root]
                    (let [^VectorSchemaRoot in-root in-root
                          ^VectorSchemaRoot out-root (.out-root this)]
                      (when (pos? (.getRowCount in-root))
                        (assert-union-compatible schema (.getSchema in-root))
                        (when-not out-root
                          (set! (.out-root this) (VectorSchemaRoot/create schema allocator)))
                        (let [^VectorSchemaRoot out-root (.out-root this)]
                          (dotimes [n (.getRowCount in-root)]
                            (let [k (->set-key in-root n)]
                              (when-not (.contains fixpoint-set k)
                                (.add fixpoint-set (copy-set-key allocator k))
                                (util/copy-tuple in-root n out-root (.getRowCount out-root))
                                (util/set-vector-schema-root-row-count out-root (inc (.getRowCount out-root)))))))))))]

          (.forEachRemaining base-cursor c)

          (loop [fixpoint-offset 0
                 fixpoint-size (.size fixpoint-set)]
            (with-open [in-cursor (.createCursor recursive-cursor-factory
                                                 (reify ICursorFactory
                                                   (createCursor [_]
                                                     (FixpointResultCursor. out-root fixpoint-offset fixpoint-size false))))]
              (.forEachRemaining in-cursor c))
            (when-not (= fixpoint-size (.size fixpoint-set))
              (recur (if incremental?
                       fixpoint-size
                       0)
                     (.size fixpoint-set)))))

        (if out-root
          (do
            (.accept c out-root)
            true)
          false))))

  (close [_]
    (doseq [k fixpoint-set]
      (release-set-key k))
    (.clear fixpoint-set)
    (util/try-close out-root)
    (util/try-close base-cursor)))

(defn ->union-cursor ^core2.IChunkCursor [^IChunkCursor left-cursor, ^IChunkCursor right-cursor]
  (UnionCursor. left-cursor right-cursor nil))

(defn ->difference-cursor ^core2.IChunkCursor [^BufferAllocator allocator, ^IChunkCursor left-cursor, ^IChunkCursor right-cursor]
  (IntersectionCursor. allocator left-cursor right-cursor (HashSet.) nil nil true))

(defn ->intersection-cursor ^core2.IChunkCursor [^BufferAllocator allocator, ^IChunkCursor left-cursor, ^IChunkCursor right-cursor]
  (IntersectionCursor. allocator left-cursor right-cursor (HashSet.) nil nil false))

(defn ->distinct-cursor ^core2.IChunkCursor [^BufferAllocator allocator, ^IChunkCursor in-cursor]
  (DistinctCursor. allocator in-cursor (HashSet.) nil nil))

(defn ->fixpoint-cursor ^core2.IChunkCursor [^BufferAllocator allocator,
                                             ^IChunkCursor base-cursor
                                             ^IFixpointCursorFactory recursive-cursor-factory
                                             incremental?]
  (FixpointCursor. allocator base-cursor recursive-cursor-factory
                   (HashSet.) nil false incremental?))
