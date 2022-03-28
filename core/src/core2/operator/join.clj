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
                    build-column-names
                    ^IRelationMap rel-map
                    pushdown-blooms]
  (let [rel-map-builder (.buildFromRelation rel-map build-rel)]
    (dotimes [build-idx (.rowCount build-rel)]
      (.add rel-map-builder build-idx))

    (dotimes [col-idx (count build-column-names)]
      (let [build-col-name (nth build-column-names col-idx)
            build-col (.vectorForName build-rel build-col-name)
            internal-vec (.getVector build-col)
            ^MutableRoaringBitmap pushdown-bloom (nth pushdown-blooms col-idx)]
        (dotimes [build-idx (.rowCount build-rel)]
          (.add pushdown-bloom ^ints (bloom/bloom-hashes internal-vec (.getIndex build-col build-idx))))))))

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
                                                    probe-column-names
                                                    ^RoaringBitmap matched-build-idxs
                                                    join-type]
  (when (pos? (.rowCount probe-rel))
    (let [semi-join? (contains? #{:semi :anti-semi} join-type)
          anti-join? (= :anti-semi join-type)
          outer-join? (contains? #{:full-outer :left-outer} join-type)
          matching-build-idxs (IntStream/builder)
          matching-probe-idxs (IntStream/builder)
          rel-map-prober (.probeFromRelation rel-map probe-rel)]

      (dotimes [probe-idx (.rowCount probe-rel)]
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

      (let [probe-column-names-set (set probe-column-names)
            copy-rel (fn copy-rel [^IIndirectRelation in-rel, ^ints idxs]
                       (doseq [^IIndirectVector in-col in-rel
                               :when (or (= in-rel probe-rel)
                                         (not (contains? probe-column-names-set (.getName in-col))))
                               :let [row-copier (->nullable-row-copier in-col rel-writer join-type)]]
                         (dotimes [idx (alength idxs)]
                           (.copyRow row-copier (aget idxs idx)))))]

        (when-not semi-join?
          (copy-rel (.getBuiltRelation rel-map)
                    (.toArray (.build matching-build-idxs))))

        (copy-rel probe-rel (.toArray (.build matching-probe-idxs)))))))

(deftype JoinCursor [^BufferAllocator allocator
                     ^ICursor build-cursor, build-key-column-names, build-column-names
                     ^ICursor probe-cursor, probe-key-column-names, probe-column-names
                     ^IRelationWriter rel-writer
                     ^List build-rels
                     ^IRelationMap rel-map
                     ^RoaringBitmap matched-build-idxs
                     pushdown-blooms
                     join-type]
  ICursor
  (tryAdvance [_ c]
    (.forEachRemaining build-cursor
                       (reify Consumer
                         (accept [_ build-rel]
                           (let [build-rel (iv/copy build-rel allocator)]
                             (.add build-rels build-rel)
                             (build-phase build-rel build-key-column-names rel-map pushdown-blooms)))))

    (let [yield-missing? (contains? #{:anti-semi :left-outer :full-outer} join-type)]
      (boolean
       (or (let [advanced? (boolean-array 1)]
             (binding [scan/*column->pushdown-bloom*
                       (if yield-missing?
                         scan/*column->pushdown-bloom*
                         (conj scan/*column->pushdown-bloom* (zipmap probe-key-column-names pushdown-blooms)))]
               (while (and (not (aget advanced? 0))
                           (.tryAdvance probe-cursor
                                        (reify Consumer
                                          (accept [_ probe-rel]
                                            (probe-phase rel-writer probe-rel rel-map probe-key-column-names matched-build-idxs join-type)

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

                 (doseq [^IRowCopier row-copier (concat (for [col-name (map name build-column-names)]
                                                          (vw/->row-copier (.writerForName rel-writer col-name) (.vectorForName build-rel col-name)))

                                                        (for [probe-col-name (map name (set/difference probe-column-names build-column-names))]
                                                          (vw/->null-row-copier (.writerForName rel-writer probe-col-name))))]

                   (.forEach unmatched-build-idxs
                             (reify IntConsumer
                               (accept [_ build-idx]
                                 (.copyRow row-copier build-idx)))))

                 (with-open [out-rel (vw/rel-writer->reader rel-writer)]
                   (.accept c out-rel)
                   true))))))))

  (close [_]
    (run! #(.clear ^MutableRoaringBitmap %) pushdown-blooms)
    (util/try-close rel-writer)
    (util/try-close rel-map)
    (run! util/try-close build-rels)
    (.clear build-rels)
    (util/try-close build-cursor)
    (util/try-close probe-cursor)))

(defn- vrepeatedly
  "Like clojure.core/repeatedly, but returns a vector."
  [n f]
  (let [n (int n)
        oarr (object-array n)]
    (loop [i (int 0)]
      (if (< i n)
        (do (aset oarr i (f)) (recur (unchecked-inc-int i)))
        (vec oarr)))))

(defn ->equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                         ^ICursor left-cursor, left-key-column-names, left-column-names
                                         ^ICursor right-cursor, right-key-column-names, right-column-names]
  (JoinCursor. allocator
               left-cursor left-key-column-names left-column-names
               right-cursor right-key-column-names right-column-names
               (vw/->rel-writer allocator)
               (ArrayList.)
               (emap/->relation-map allocator {:build-key-col-names left-key-column-names
                                               :probe-key-col-names right-key-column-names})
               nil
               (vrepeatedly (count right-key-column-names) #(MutableRoaringBitmap.))
               :inner))

(defn ->left-semi-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                   ^ICursor left-cursor, left-key-column-names, left-column-names
                                                   ^ICursor right-cursor, right-key-column-names, right-column-names]
  (JoinCursor. allocator
               right-cursor right-key-column-names right-column-names
               left-cursor left-key-column-names left-column-names
               (vw/->rel-writer allocator)
               (ArrayList.)
               (emap/->relation-map allocator {:build-key-col-names right-key-column-names
                                               :probe-key-col-names left-key-column-names})
               nil
               (vrepeatedly (count right-key-column-names) #(MutableRoaringBitmap.))
               :semi))

(defn ->left-outer-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                    ^ICursor left-cursor, left-key-column-names, left-column-names
                                                    ^ICursor right-cursor, right-key-column-names, right-column-names]
  (JoinCursor. allocator
               right-cursor right-key-column-names right-column-names
               left-cursor left-key-column-names left-column-names
               (vw/->rel-writer allocator)
               (ArrayList.)
               (emap/->relation-map allocator {:build-key-col-names right-key-column-names
                                               :probe-key-col-names left-key-column-names})
               nil
               (vrepeatedly (count right-key-column-names) #(MutableRoaringBitmap.))
               :left-outer))

(defn ->full-outer-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                    ^ICursor left-cursor, left-key-column-names, left-column-names
                                                    ^ICursor right-cursor, right-key-column-names, right-column-names]
  (JoinCursor. allocator
               left-cursor left-key-column-names left-column-names
               right-cursor right-key-column-names right-column-names
               (vw/->rel-writer allocator)
               (ArrayList.)
               (emap/->relation-map allocator {:build-key-col-names left-key-column-names
                                               :probe-key-col-names right-key-column-names})
               (RoaringBitmap.)
               (vrepeatedly (count right-key-column-names) #(MutableRoaringBitmap.))
               :full-outer))

(defn ->left-anti-semi-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                        ^ICursor left-cursor, left-key-column-names, left-column-names
                                                        ^ICursor right-cursor, right-key-column-names, right-column-names]
  (JoinCursor. allocator
               right-cursor right-key-column-names right-column-names
               left-cursor left-key-column-names left-column-names
               (vw/->rel-writer allocator)
               (ArrayList.)
               (emap/->relation-map allocator {:build-key-col-names right-key-column-names
                                               :probe-key-col-names left-key-column-names})
               nil
               (vrepeatedly (count right-key-column-names) #(MutableRoaringBitmap.))
               :anti-semi))
