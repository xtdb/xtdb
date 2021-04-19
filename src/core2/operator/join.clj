(ns core2.operator.join
  (:require [core2.util :as util])
  (:import core2.ICursor
           [java.util ArrayList HashMap List Map NavigableMap TreeMap]
           [java.util.function Consumer Function]
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BigIntVector BitVector ElementAddressableVector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types.pojo Field Schema]
           org.apache.arrow.vector.util.VectorBatchAppender))

(defn- ->join-schema
  (^org.apache.arrow.vector.types.pojo.Schema [^Schema left-schema ^Schema right-schema]
   (->join-schema left-schema right-schema #{}))
  (^org.apache.arrow.vector.types.pojo.Schema [^Schema left-schema ^Schema right-schema skip-left-column?]
   (let [fields (concat (for [^Field f (.getFields left-schema)
                              :when (not (skip-left-column? (.getName f)))]
                          f)
                        (.getFields right-schema))]
     (assert (apply distinct? fields))
     (Schema. fields))))

(defn- cross-product [^VectorSchemaRoot left-root ^long left-idx ^VectorSchemaRoot right-root ^VectorSchemaRoot out-root]
  (let [out-idx (.getRowCount out-root)
        row-count (.getRowCount right-root)]
    (dotimes [n (util/root-field-count right-root)]
      (let [^ValueVector right-vec (.getVector right-root n)
            out-vec (.getVector out-root (.getName right-vec))]
        (VectorBatchAppender/batchAppend out-vec (into-array ValueVector [right-vec]))))
    (dotimes [n (util/root-field-count left-root)]
      (let [^ValueVector left-vec (.getVector left-root n)
            out-vec (.getVector out-root (.getName left-vec))]
        (util/set-value-count out-vec (+ out-idx row-count))
        (if (and (instance? DenseUnionVector left-vec)
                 (instance? DenseUnionVector out-vec))
          (dotimes [m row-count]
            (util/du-copy left-vec left-idx out-vec (+ out-idx m)))
          (dotimes [m row-count]
            (.copyFrom out-vec left-idx (+ out-idx m) left-vec)))))
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
                    ^VectorSchemaRoot build-root
                    ^NavigableMap build-idx->root
                    ^Map join-key->build-idxs
                    ^String build-column-name]
  (let [^VectorSchemaRoot build-root (util/slice-root build-root 0)
        build-root-idx  (if-let [build-idx-entry (.lastEntry build-idx->root)]
                          (+ (long (.getKey build-idx-entry))
                             (.getRowCount ^VectorSchemaRoot (.getValue build-idx-entry)))
                          0)]
    (.put build-idx->root build-root-idx build-root)
    (let [build-vec (util/maybe-single-child-dense-union (.getVector build-root build-column-name))]
      (dotimes [build-idx (.getValueCount build-vec)]
        (let [^BigIntVector build-idxs-vec (.computeIfAbsent join-key->build-idxs
                                                             (.hashCode build-vec build-idx)
                                                             (reify Function
                                                               (apply [_ x]
                                                                 (doto (BigIntVector. "" allocator)
                                                                   (.setInitialCapacity 32)))))
              idx (.getValueCount build-idxs-vec)
              total-build-idx (+ build-root-idx build-idx)]
          (.setValueCount build-idxs-vec (inc idx))
          (.set build-idxs-vec idx total-build-idx))))))

(defn- probe-phase ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator allocator
                                                              ^VectorSchemaRoot probe-root
                                                              ^NavigableMap build-idx->root
                                                              ^Map join-key->build-idxs-vec
                                                              ^String build-column-name
                                                              ^String probe-column-name
                                                              semi-join?
                                                              anti-join?
                                                              skip-build-column?]
  (when (pos? (.getRowCount probe-root))
    (let [join-schema (if semi-join?
                        (.getSchema probe-root)
                        (->join-schema (.getSchema ^VectorSchemaRoot (.getValue (.firstEntry build-idx->root)))
                                       (.getSchema probe-root)
                                       skip-build-column?))
          out-root (VectorSchemaRoot/create join-schema allocator)
          probe-vec (util/maybe-single-child-dense-union (.getVector probe-root probe-column-name))
          build-pointer (ArrowBufPointer.)
          probe-pointer (ArrowBufPointer.)]
      (dotimes [probe-idx (.getValueCount probe-vec)]
        (if-let [^BigIntVector build-idxs-vec (.get join-key->build-idxs-vec (.hashCode probe-vec probe-idx))]
          (let [value-count (.getValueCount build-idxs-vec)
                probe-pointer-or-object (util/pointer-or-object probe-vec probe-idx probe-pointer)]
            (loop [n 0
                   out-idx (.getRowCount out-root)]
              (if (= n value-count)
                (util/set-vector-schema-root-row-count out-root out-idx)
                (let [total-build-idx (long (.get build-idxs-vec n))
                      build-idx-entry (.floorEntry build-idx->root total-build-idx)
                      ^long build-root-idx (.getKey build-idx-entry)
                      ^VectorSchemaRoot build-root (.getValue build-idx-entry)
                      build-idx (- total-build-idx build-root-idx)
                      build-vec (.getVector build-root build-column-name)
                      match? (= (util/pointer-or-object build-vec build-idx build-pointer) probe-pointer-or-object)
                      match? (if anti-join?
                               (not match?)
                               match?)]
                  (if match?
                    (cond
                      (not semi-join?)
                      (do (util/copy-tuple build-root build-idx out-root out-idx)
                          (util/copy-tuple probe-root probe-idx out-root out-idx)
                          (recur (inc n) (inc out-idx)))

                      anti-join?
                      (if (= (inc n) value-count)
                        (do (util/copy-tuple probe-root probe-idx out-root out-idx)
                            (recur (inc n) (inc out-idx)))
                        (recur (inc n) out-idx))

                      semi-join?
                      (do (util/copy-tuple probe-root probe-idx out-root out-idx)
                          (recur value-count (inc out-idx))))

                    (recur (inc n) out-idx))))))
          (when anti-join?
            (let [out-idx (.getRowCount out-root)]
              (util/copy-tuple probe-root probe-idx out-root out-idx)
              (util/set-vector-schema-root-row-count out-root (inc out-idx))))))
      out-root)))

(deftype JoinCursor [^BufferAllocator allocator
                     ^ICursor build-cursor
                     ^String build-column-name
                     ^ICursor probe-cursor
                     ^String probe-column-name
                     ^Map join-key->build-idx-pairs
                     ^NavigableMap build-idx->root
                     ^:unsynchronized-mutable ^VectorSchemaRoot out-root
                     semi-join?
                     anti-join?
                     skip-build-column?]

  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root)
      (set! (.out-root this) nil))

    (.forEachRemaining build-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (build-phase allocator in-root build-idx->root join-key->build-idx-pairs build-column-name))))

    (while (and (or (not (.isEmpty build-idx->root)) anti-join?)
                (nil? out-root)
                (.tryAdvance probe-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (when-let [out-root (probe-phase allocator in-root build-idx->root join-key->build-idx-pairs
                                                                  build-column-name probe-column-name semi-join? anti-join? skip-build-column?)]
                                   (when (pos? (.getRowCount out-root))
                                     (set! (.out-root this) out-root))))))))

    (if out-root
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (util/try-close out-root)
    (doseq [root (vals build-idx->root)]
      (util/try-close root))
    (.clear build-idx->root)
    (doseq [idx-pair (vals join-key->build-idx-pairs)]
      (util/try-close idx-pair))
    (.clear join-key->build-idx-pairs)
    (util/try-close build-cursor)
    (util/try-close probe-cursor)))

(defn ->equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                         ^ICursor left-cursor,
                                         ^String left-column-name,
                                         ^ICursor right-cursor,
                                         ^String right-column-name]
  (let [skip-build-column? (if (= left-column-name right-column-name)
                             #{left-column-name}
                             #{})]
    (JoinCursor. allocator left-cursor left-column-name right-cursor right-column-name (HashMap.) (TreeMap.) nil false false skip-build-column?)))

(defn ->semi-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                              ^ICursor left-cursor,
                                              ^String left-column-name,
                                              ^ICursor right-cursor,
                                              ^String right-column-name]
  (JoinCursor. allocator right-cursor right-column-name left-cursor left-column-name (HashMap.) (TreeMap.) nil true false #{}))

(defn ->anti-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                              ^ICursor left-cursor,
                                              ^String left-column-name,
                                              ^ICursor right-cursor,
                                              ^String right-column-name]
  (JoinCursor. allocator right-cursor right-column-name left-cursor left-column-name (HashMap.) (TreeMap.) nil true true #{}))
