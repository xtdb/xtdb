(ns core2.operator.join
  (:require [core2.bloom :as bloom]
            [core2.operator.scan :as scan]
            [core2.util :as util])
  (:import [core2 DenseUnionUtil IChunkCursor ICursor]
           [java.util ArrayList HashMap List Map]
           [java.util.function Consumer Function]
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.vector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types.pojo Field Schema]
           org.apache.arrow.vector.util.VectorBatchAppender
           org.roaringbitmap.buffer.MutableRoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn- ->join-schema
  (^org.apache.arrow.vector.types.pojo.Schema [^Schema left-schema ^Schema right-schema]
   (->join-schema left-schema right-schema #{}))
  (^org.apache.arrow.vector.types.pojo.Schema [^Schema left-schema ^Schema right-schema skip-left-column?]
   (let [fields (concat (for [^Field f (.getFields left-schema)
                              :when (not (skip-left-column? (.getName f)))]
                          f)
                        (.getFields right-schema))]
     (try
       (assert (apply distinct? fields))
       (catch Throwable t
         (clojure.pprint/pprint {:left left-schema
                                 :right right-schema
                                 :skip-left-column? skip-left-column?})
         (throw t)))
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
            (DenseUnionUtil/copyIdxSafe left-vec left-idx out-vec (+ out-idx m)))
          (dotimes [m row-count]
            (.copyFrom out-vec left-idx (+ out-idx m) left-vec)))))
    (util/set-vector-schema-root-row-count out-root (+ out-idx row-count))))

(deftype CrossJoinCursor [^BufferAllocator allocator
                          ^Schema out-schema
                          ^VectorSchemaRoot out-root
                          ^IChunkCursor left-cursor
                          ^IChunkCursor right-cursor
                          ^List left-roots]
  IChunkCursor
  (getSchema [_] out-schema)

  (tryAdvance [this c]
    (.clear out-root)

    (.forEachRemaining left-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (.add left-roots (util/slice-root in-root)))))

    (while (and (not (.isEmpty left-roots))
                (zero? (.getRowCount out-root))
                (.tryAdvance right-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (let [^VectorSchemaRoot right-root in-root]
                                   (when (pos? (.getRowCount right-root))
                                     (doseq [^VectorSchemaRoot left-root left-roots]
                                       (dotimes [left-idx (.getRowCount left-root)]
                                         (cross-product left-root left-idx right-root out-root))))))))))
    (if (pos? (.getRowCount out-root))
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

(defn ->cross-join-cursor ^core2.IChunkCursor [^BufferAllocator allocator, ^IChunkCursor left-cursor, ^IChunkCursor right-cursor]
  (let [schema (->join-schema (.getSchema left-cursor) (.getSchema right-cursor))]
    (CrossJoinCursor. allocator schema
                      (VectorSchemaRoot/create schema allocator)
                      left-cursor right-cursor
                      (ArrayList.))))

(deftype BuildPointer [^VectorSchemaRoot root, ^int idx, pointer-or-object])

(defn- build-phase [^VectorSchemaRoot build-root
                    ^List build-roots
                    ^Map join-key->build-pointers
                    ^String build-column-name
                    ^MutableRoaringBitmap pushdown-bloom]
  (let [^VectorSchemaRoot build-root (util/slice-root build-root)
        build-vec (util/maybe-single-child-dense-union (.getVector build-root build-column-name))]
    (.add build-roots build-root)
    (dotimes [build-idx (.getValueCount build-vec)]
      (.add pushdown-bloom ^ints (bloom/bloom-hashes build-vec build-idx))
      (doto ^List (.computeIfAbsent join-key->build-pointers
                                    (.hashCode build-vec build-idx)
                                    (reify Function
                                      (apply [_ x]
                                        (ArrayList.))))
        (.add (BuildPointer. build-root build-idx
                             (util/pointer-or-object build-vec build-idx)))))))

(defn- probe-phase ^org.apache.arrow.vector.VectorSchemaRoot [^VectorSchemaRoot out-root
                                                              ^VectorSchemaRoot probe-root
                                                              ^Map join-key->build-pointers
                                                              ^String probe-column-name
                                                              semi-join? anti-join?]
  (when (pos? (.getRowCount probe-root))
    (let [probe-vec (util/maybe-single-child-dense-union (.getVector probe-root probe-column-name))
          probe-pointer (ArrowBufPointer.)
          matching-build-pointers (ArrayList.)
          matching-probe-idxs (IntStream/builder)]
      (dotimes [probe-idx (.getValueCount probe-vec)]
        (let [match? (when-let [^List build-pointers (.get join-key->build-pointers (.hashCode probe-vec probe-idx))]
                       (let [probe-pointer-or-object (util/pointer-or-object probe-vec probe-idx probe-pointer)
                             build-pointer-it (.iterator build-pointers)]
                         (loop [match? false]
                           (if-let [^BuildPointer build-pointer (when (.hasNext build-pointer-it)
                                                                  (.next build-pointer-it))]
                             (cond
                               (not= (.pointer-or-object build-pointer) probe-pointer-or-object)
                               (recur match?)

                               semi-join? true

                               :else (do
                                       (.add matching-build-pointers build-pointer)
                                       (.accept matching-probe-idxs probe-idx)
                                       (recur true)))
                             match?))))]
          (cond
            anti-join? (when-not match?
                         (.accept matching-probe-idxs probe-idx))
            semi-join? (when match?
                         (.accept matching-probe-idxs probe-idx)))))

      (let [probe-idxs (.toArray (.build matching-probe-idxs))
            match-count (alength probe-idxs)]
        (when-not semi-join?
          (dotimes [match-idx match-count]
            (let [^BuildPointer build-pointer (nth matching-build-pointers match-idx)]
              (util/copy-tuple (.root build-pointer) (.idx build-pointer)
                               out-root match-idx))))

        (util/copy-tuples probe-root probe-idxs out-root)

        (util/set-vector-schema-root-row-count out-root match-count))

      out-root)))

(deftype JoinCursor [^BufferAllocator allocator
                     ^Schema out-schema
                     ^VectorSchemaRoot out-root
                     ^ICursor build-cursor, ^String build-column-name
                     ^ICursor probe-cursor, ^String probe-column-name
                     ^List build-roots
                     ^Map join-key->build-pointers
                     ^MutableRoaringBitmap pushdown-bloom
                     semi-join? anti-join?]

  IChunkCursor
  (getSchema [_] out-schema)

  (tryAdvance [_ c]
    (.clear out-root)

    (.forEachRemaining build-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (build-phase in-root build-roots join-key->build-pointers build-column-name pushdown-bloom))))

    (while (and (or (not (.isEmpty join-key->build-pointers))
                    anti-join?)
                (zero? (.getRowCount out-root))
                (binding [scan/*column->pushdown-bloom* (if anti-join?
                                                          scan/*column->pushdown-bloom*
                                                          (assoc scan/*column->pushdown-bloom* probe-column-name pushdown-bloom))]
                  (.tryAdvance probe-cursor
                               (reify Consumer
                                 (accept [_ in-root]
                                   (probe-phase out-root in-root join-key->build-pointers
                                                probe-column-name semi-join? anti-join?)))))))

    (if (pos? (.getRowCount out-root))
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (util/try-close out-root)
    (.clear pushdown-bloom)
    (run! util/try-close build-roots)
    (.clear build-roots)
    (util/try-close build-cursor)
    (util/try-close probe-cursor)))

(defn ->equi-join-cursor ^core2.IChunkCursor [^BufferAllocator allocator,
                                              ^IChunkCursor left-cursor,
                                              ^String left-column-name,
                                              ^IChunkCursor right-cursor,
                                              ^String right-column-name]
  (let [schema (->join-schema (.getSchema left-cursor) (.getSchema right-cursor)
                              (if (= left-column-name right-column-name)
                                #{left-column-name}
                                #{}))]
    (JoinCursor. allocator schema
                 (VectorSchemaRoot/create schema allocator)
                 left-cursor left-column-name right-cursor right-column-name
                 (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
                 false false)))

(defn ->semi-equi-join-cursor ^core2.IChunkCursor [^BufferAllocator allocator,
                                                   ^IChunkCursor left-cursor,
                                                   ^String left-column-name,
                                                   ^IChunkCursor right-cursor,
                                                   ^String right-column-name]
  (let [schema (.getSchema left-cursor)]
    (JoinCursor. allocator schema
                 (VectorSchemaRoot/create schema allocator)
                 right-cursor right-column-name left-cursor left-column-name
                 (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
                 true false)))

(defn ->anti-equi-join-cursor ^core2.IChunkCursor [^BufferAllocator allocator,
                                                   ^IChunkCursor left-cursor,
                                                   ^String left-column-name,
                                                   ^IChunkCursor right-cursor,
                                                   ^String right-column-name]
  (let [schema (.getSchema left-cursor)]
    (JoinCursor. allocator schema
                 (VectorSchemaRoot/create schema allocator)
                 right-cursor right-column-name left-cursor left-column-name
                 (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
                 true true)))
