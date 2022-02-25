(ns core2.operator.join
  (:require [clojure.set :as set]
            [core2.bloom :as bloom]
            [core2.expression.map :as emap]
            [core2.operator.scan :as scan]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import core2.expression.map.IRelationMap
           core2.ICursor
           core2.types.LegType
           [core2.vector IIndirectRelation IIndirectVector IRelationWriter IRowCopier IVectorWriter]
           [java.util ArrayList Iterator List]
           java.util.function.Consumer
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           [org.roaringbitmap IntConsumer RoaringBitmap]
           org.roaringbitmap.buffer.MutableRoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn- cross-product ^core2.vector.IIndirectRelation [^IIndirectRelation left-rel, ^IIndirectRelation right-rel]
  (let [left-row-count (.rowCount left-rel)
        right-row-count (.rowCount right-rel)
        row-count (* left-row-count right-row-count)]
    (iv/->indirect-rel (concat (iv/select left-rel
                                          (let [idxs (int-array row-count)]
                                            (dotimes [idx row-count]
                                              (aset idxs idx ^long (quot idx right-row-count)))
                                            idxs))

                               (iv/select right-rel
                                          (let [idxs (int-array row-count)]
                                            (dotimes [idx row-count]
                                              (aset idxs idx ^long (rem idx right-row-count)))
                                            idxs))))))

(deftype CrossJoinCursor [^BufferAllocator allocator
                          ^ICursor left-cursor
                          ^ICursor right-cursor
                          ^List left-rels
                          ^:unsynchronized-mutable ^Iterator left-rel-iterator
                          ^:unsynchronized-mutable ^IIndirectRelation right-rel]
  ICursor
  (getColumnNames [_]
    (set/union (set (.getColumnNames left-cursor))
               (set (.getColumnNames right-cursor))))

  (tryAdvance [this c]
    (.forEachRemaining left-cursor
                       (reify Consumer
                         (accept [_ left-rel]
                           (.add left-rels (iv/copy left-rel allocator)))))

    (boolean
     (when-let [right-rel (or (when (and left-rel-iterator (.hasNext left-rel-iterator))
                                right-rel)
                              (do
                                (when right-rel
                                  (.close right-rel)
                                  (set! (.right-rel this) nil))
                                (when (.tryAdvance right-cursor
                                                   (reify Consumer
                                                     (accept [_ right-rel]
                                                       (set! (.right-rel this) (iv/copy right-rel allocator))
                                                       (set! (.left-rel-iterator this) (.iterator left-rels)))))
                                  (.right-rel this))))]

       (when-let [left-rel (when (.hasNext left-rel-iterator)
                             (.next left-rel-iterator))]
         (.accept c (cross-product left-rel right-rel))
         true))))

  (close [_]
    (when left-rels
      (run! util/try-close left-rels)
      (.clear left-rels))
    (util/try-close left-cursor)
    (util/try-close right-rel)
    (util/try-close right-cursor)))

(defn ->cross-join-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (CrossJoinCursor. allocator left-cursor right-cursor (ArrayList.) nil nil))

(defn- build-phase [^IIndirectRelation build-rel
                    ^String build-column-name
                    ^IRelationMap rel-map
                    ^MutableRoaringBitmap pushdown-bloom]
  (let [build-col (.vectorForName build-rel build-column-name)
        internal-vec (.getVector build-col)
        rel-map-builder (.buildFromRelation rel-map build-rel)]
    (dotimes [build-idx (.rowCount build-rel)]
      (.add rel-map-builder build-idx)
      (.add pushdown-bloom ^ints (bloom/bloom-hashes internal-vec (.getIndex build-col build-idx))))))

(defn- ->nullable-row-copier ^core2.vector.IRowCopier [^IIndirectVector in-col, ^IRelationWriter out-rel, join-type]
  (let [col-name (.getName in-col)
        writer (.writerForName out-rel col-name)
        row-copier (vw/->row-copier writer in-col)
        null-copier (when (contains? #{:full-outer :left-outer} join-type)
                      (vw/->null-row-copier writer))]
    (reify IRowCopier
      (copyRow [_ idx]
        (if (neg? idx)
          (.copyRow null-copier idx)
          (.copyRow row-copier idx))))))

(defn- probe-phase ^core2.vector.IIndirectRelation [^IRelationWriter rel-writer
                                                    ^IIndirectRelation probe-rel
                                                    ^IRelationMap rel-map
                                                    ^String probe-column-name
                                                    ^RoaringBitmap matched-build-idxs
                                                    join-type]
  (when (pos? (.rowCount probe-rel))
    (let [semi-join? (contains? #{:semi :anti-semi} join-type)
          anti-join? (= :anti-semi join-type)
          outer-join? (contains? #{:full-outer :left-outer} join-type)

          probe-col (.vectorForName probe-rel probe-column-name)
          matching-build-idxs (IntStream/builder)
          matching-probe-idxs (IntStream/builder)
          rel-map-prober (.probeFromRelation rel-map probe-rel)]

      (dotimes [probe-idx (.getValueCount probe-col)]
        (let [match? (if semi-join?
                       (not (neg? (.indexOf rel-map-prober probe-idx)))
                       (when-let [build-idxs (.getAll rel-map-prober probe-idx)]
                         (.forEach build-idxs
                                   (reify IntConsumer
                                     (accept [_ build-idx]
                                       (when matched-build-idxs
                                         (.add matched-build-idxs build-idx))
                                       (.add matching-build-idxs build-idx)
                                       (.add matching-probe-idxs probe-idx))))
                         true))]
          (cond
            outer-join? (when-not match?
                          (.add matching-probe-idxs probe-idx)
                          (.add matching-build-idxs -1))
            anti-join? (when-not match?
                         (.add matching-probe-idxs probe-idx))
            semi-join? (when match?
                         (.add matching-probe-idxs probe-idx)))))

      (letfn [(copy-rel [^IIndirectRelation in-rel, ^ints idxs]
                (doseq [^IIndirectVector in-col in-rel
                        :when (or (= in-rel probe-rel)
                                  (not= (.getName in-col) probe-column-name))
                        :let [row-copier (->nullable-row-copier in-col rel-writer join-type)]]
                  (dotimes [idx (alength idxs)]
                    (.copyRow row-copier (aget idxs idx)))))]

        (when-not semi-join?
          (copy-rel (.getBuiltRelation rel-map)
                    (.toArray (.build matching-build-idxs))))

        (copy-rel probe-rel (.toArray (.build matching-probe-idxs)))))))

(deftype JoinCursor [^BufferAllocator allocator
                     ^ICursor build-cursor, ^String build-column-name
                     ^ICursor probe-cursor, ^String probe-column-name
                     ^IRelationWriter rel-writer
                     ^List build-rels
                     ^IRelationMap rel-map
                     ^RoaringBitmap matched-build-idxs
                     ^MutableRoaringBitmap pushdown-bloom
                     join-type]
  ICursor
  (getColumnNames [_]
    (set/union (set (.getColumnNames build-cursor))
               (set (.getColumnNames probe-cursor))))

  (tryAdvance [_ c]
    (.forEachRemaining build-cursor
                       (reify Consumer
                         (accept [_ build-rel]
                           (let [build-rel (iv/copy build-rel allocator)]
                             (.add build-rels build-rel)
                             (build-phase build-rel build-column-name rel-map pushdown-bloom)))))

    (let [yield-missing? (contains? #{:anti-semi :left-outer :full-outer} join-type)]
      (boolean
       (or (let [advanced? (boolean-array 1)]
             (binding [scan/*column->pushdown-bloom* (if yield-missing?
                                                       scan/*column->pushdown-bloom*
                                                       (assoc scan/*column->pushdown-bloom* probe-column-name pushdown-bloom))]
               (while (and (not (aget advanced? 0))
                           (.tryAdvance probe-cursor
                                        (reify Consumer
                                          (accept [_ probe-rel]
                                            (probe-phase rel-writer probe-rel rel-map probe-column-name matched-build-idxs join-type)

                                            (with-open [out-rel (vw/rel-writer->reader rel-writer)]
                                              (try
                                                (when (pos? (.rowCount out-rel))
                                                  (aset advanced? 0 true)
                                                  (.accept c out-rel))
                                                (finally
                                                  (vw/clear-rel rel-writer))))))))))
             (aget advanced? 0))

           (when (= :full-outer join-type)
             (let [build-rel (.getBuiltRelation rel-map)
                   build-row-count (long (.rowCount build-rel))
                   unmatched-build-idxs (RoaringBitmap/flip matched-build-idxs 0 build-row-count)]
               (when-not (.isEmpty unmatched-build-idxs)
                 (.add matched-build-idxs 0 build-row-count)

                 (doseq [^IRowCopier row-copier (concat (for [col-name (.getColumnNames build-cursor)]
                                                          (vw/->row-copier (.writerForName rel-writer col-name) (.vectorForName build-rel col-name)))

                                                        (for [probe-col-name (set/difference (.getColumnNames probe-cursor)
                                                                                             (.getColumnNames build-cursor))]
                                                          (vw/->null-row-copier (.writerForName rel-writer probe-col-name))))]

                   (.forEach unmatched-build-idxs
                             (reify IntConsumer
                               (accept [_ build-idx]
                                 (.copyRow row-copier build-idx)))))

                 (with-open [out-rel (vw/rel-writer->reader rel-writer)]
                   (.accept c out-rel)
                   true))))))))

  (close [_]
    (.clear pushdown-bloom)
    (util/try-close rel-writer)
    (util/try-close rel-map)
    (run! util/try-close build-rels)
    (.clear build-rels)
    (util/try-close build-cursor)
    (util/try-close probe-cursor)))

(defn ->equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                         ^ICursor left-cursor, ^String left-column-name,
                                         ^ICursor right-cursor, ^String right-column-name]
  (JoinCursor. allocator
               left-cursor left-column-name right-cursor right-column-name
               (vw/->rel-writer allocator)
               (ArrayList.)
               (emap/->relation-map allocator {:build-key-col-names [left-column-name]
                                               :probe-key-col-names [right-column-name]})
               nil
               (MutableRoaringBitmap.)
               :inner))

(defn ->left-semi-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                   ^ICursor left-cursor, ^String left-column-name,
                                                   ^ICursor right-cursor, ^String right-column-name]
  (JoinCursor. allocator
               right-cursor right-column-name left-cursor left-column-name
               (vw/->rel-writer allocator)
               (ArrayList.)
               (emap/->relation-map allocator {:build-key-col-names [right-column-name]
                                               :probe-key-col-names [left-column-name]})
               nil
               (MutableRoaringBitmap.)
               :semi))

(defn ->left-outer-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                    ^ICursor left-cursor, ^String left-column-name,
                                                    ^ICursor right-cursor, ^String right-column-name]
  (JoinCursor. allocator
               right-cursor right-column-name left-cursor left-column-name
               (vw/->rel-writer allocator)
               (ArrayList.)
               (emap/->relation-map allocator {:build-key-col-names [right-column-name]
                                               :probe-key-col-names [left-column-name]})
               nil
               (MutableRoaringBitmap.)
               :left-outer))

(defn ->full-outer-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                    ^ICursor left-cursor, ^String left-column-name,
                                                    ^ICursor right-cursor, ^String right-column-name]
  (JoinCursor. allocator
               left-cursor left-column-name right-cursor right-column-name
               (vw/->rel-writer allocator)
               (ArrayList.)
               (emap/->relation-map allocator {:build-key-col-names [left-column-name]

                                               :probe-key-col-names [right-column-name]})
               (RoaringBitmap.)
               (MutableRoaringBitmap.)
               :full-outer))

(defn ->left-anti-semi-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                        ^ICursor left-cursor, ^String left-column-name,
                                                        ^ICursor right-cursor, ^String right-column-name]
  (JoinCursor. allocator
               right-cursor right-column-name left-cursor left-column-name
               (vw/->rel-writer allocator)
               (ArrayList.)
               (emap/->relation-map allocator {:build-key-col-names [right-column-name]
                                               :probe-key-col-names [left-column-name]})
               nil
               (MutableRoaringBitmap.)
               :anti-semi))
