(ns core2.operator.set
  (:require [core2.error :as err]
            [core2.types :as t]
            [core2.util :as util])
  (:import core2.IChunkCursor
           [java.util ArrayList HashSet List Set]
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo Field Schema]
           org.apache.arrow.vector.VectorSchemaRoot))

(set! *unchecked-math* :warn-on-boxed)

(defn- ->union-compatible-schema [^Schema x ^Schema y]
  (letfn [(union-incompatible []
            (err/illegal-arg :union-incompatible
                             {::err/message "Schemas are not union compatible"
                              :left x, :right y}))]
    (let [x-fields (.getFields x)
          y-fields (.getFields y)]
      (when-not (= (count x-fields) (count y-fields))
        (throw (union-incompatible)))
      (Schema. (for [[^Field x-field, ^Field y-field] (map vector x-fields y-fields)
                     :let [x-name (.getName x-field)
                           x-type (Types/getMinorTypeForArrowType
                                   (.getType x-field))
                           y-name (.getName y-field)
                           y-type (Types/getMinorTypeForArrowType
                                   (.getType y-field))]]
                 (cond
                   (not (and (= x-name y-name) (= x-type y-type)))
                   (throw (union-incompatible))

                   (= Types$MinorType/DENSEUNION x-type y-type)
                   (t/->primitive-dense-union-field x-name)

                   :else x-field))))))

(deftype UnionCursor [^Schema out-schema
                      ^IChunkCursor left-cursor
                      ^IChunkCursor right-cursor]
  IChunkCursor
  (getSchema [_] out-schema)

  (tryAdvance [_ c]
    (boolean
     (or (.tryAdvance left-cursor
                      (reify Consumer
                        (accept [_ in-root]
                          (let [^VectorSchemaRoot in-root in-root]
                            (when (pos? (.getRowCount in-root))
                              (.accept c in-root))))))
         (.tryAdvance right-cursor
                      (reify Consumer
                        (accept [_ in-root]
                          (let [^VectorSchemaRoot in-root in-root]
                            (when (pos? (.getRowCount in-root))
                              (.accept c in-root)))))))))

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
                             ^Schema out-schema
                             ^VectorSchemaRoot out-root
                             ^IChunkCursor left-cursor
                             ^IChunkCursor right-cursor
                             ^Set intersection-set
                             difference?]
  IChunkCursor
  (getSchema [_] out-schema)

  (tryAdvance [_ c]
    (.clear out-root)

    (.forEachRemaining right-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (let [^VectorSchemaRoot in-root in-root]
                             (when (pos? (.getRowCount in-root))
                               (dotimes [n (.getRowCount in-root)]
                                 (let [k (->set-key in-root n)]
                                   (when-not (.contains intersection-set k)
                                     (.add intersection-set (copy-set-key allocator k))))))))))

    (while (and (zero? (.getRowCount out-root))
                (.tryAdvance left-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (let [^VectorSchemaRoot in-root in-root]
                                   (when (pos? (.getRowCount in-root))
                                     (dotimes [n (.getRowCount in-root)]
                                       (when (cond-> (.contains intersection-set (->set-key in-root n))
                                               difference? not)
                                         (util/copy-tuple in-root n out-root (.getRowCount out-root))
                                         (util/set-vector-schema-root-row-count out-root (inc (.getRowCount out-root))))))))))))

    (if (pos? (.getRowCount out-root))
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
                         ^Schema out-schema
                         ^VectorSchemaRoot out-root
                         ^IChunkCursor in-cursor
                         ^Set seen-set]
  IChunkCursor
  (getSchema [_] out-schema)

  (tryAdvance [_ c]
    (.clear out-root)

    (while (and (zero? (.getRowCount out-root))
                (.tryAdvance in-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (let [^VectorSchemaRoot in-root in-root]
                                   (when (pos? (.getRowCount in-root))
                                     (dotimes [n (.getRowCount in-root)]
                                       (let [k (->set-key in-root n)]
                                         (when-not (.contains seen-set k)
                                           (.add seen-set (copy-set-key allocator k))
                                           (util/copy-tuple in-root n out-root (.getRowCount out-root))
                                           (util/set-vector-schema-root-row-count out-root (inc (.getRowCount out-root)))))))))))))

    (if (pos? (.getRowCount out-root))
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
                         ^Schema out-schema
                         ^VectorSchemaRoot out-root
                         ^IChunkCursor base-cursor
                         ^IFixpointCursorFactory recursive-cursor-factory
                         ^Set fixpoint-set
                         ^:unsynchronized-mutable done?
                         incremental?]
  IChunkCursor
  (getSchema [_] out-schema)

  (tryAdvance [_ c]
    (if done?
      false
      (do
        (set! done? true)

        (let [c (reify Consumer
                  (accept [_ in-root]
                    (let [^VectorSchemaRoot in-root in-root]
                      (when (pos? (.getRowCount in-root))
                        (dotimes [n (.getRowCount in-root)]
                          (let [k (->set-key in-root n)]
                            (when-not (.contains fixpoint-set k)
                              (.add fixpoint-set (copy-set-key allocator k))
                              (util/copy-tuple in-root n out-root (.getRowCount out-root))
                              (util/set-vector-schema-root-row-count out-root (inc (.getRowCount out-root))))))))))]

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
  (UnionCursor. (->union-compatible-schema (.getSchema left-cursor) (.getSchema right-cursor))
                left-cursor right-cursor))

(defn ->difference-cursor ^core2.IChunkCursor [^BufferAllocator allocator, ^IChunkCursor left-cursor, ^IChunkCursor right-cursor]
  (let [schema (->union-compatible-schema (.getSchema left-cursor) (.getSchema right-cursor))]
    (IntersectionCursor. allocator schema
                         (VectorSchemaRoot/create schema allocator)
                         left-cursor right-cursor (HashSet.) true)))

(defn ->intersection-cursor ^core2.IChunkCursor [^BufferAllocator allocator, ^IChunkCursor left-cursor, ^IChunkCursor right-cursor]
  (let [schema (->union-compatible-schema (.getSchema left-cursor) (.getSchema right-cursor))]
    (IntersectionCursor. allocator schema
                         (VectorSchemaRoot/create schema allocator)
                         left-cursor right-cursor (HashSet.) false)))

(defn ->distinct-cursor ^core2.IChunkCursor [^BufferAllocator allocator, ^IChunkCursor in-cursor]
  (let [schema (.getSchema in-cursor)]
    (DistinctCursor. allocator schema
                     (VectorSchemaRoot/create schema allocator)
                     in-cursor (HashSet.))))

(defn ->fixpoint-cursor ^core2.IChunkCursor [^BufferAllocator allocator,
                                             ^IChunkCursor base-cursor
                                             ^IFixpointCursorFactory recursive-cursor-factory
                                             incremental?]
  (let [schema (.getSchema base-cursor)]
    (FixpointCursor. allocator schema
                     (VectorSchemaRoot/create schema allocator)
                     base-cursor recursive-cursor-factory
                     (HashSet.) false incremental?)))
