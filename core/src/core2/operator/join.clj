(ns core2.operator.join
  (:require [core2.bloom :as bloom]
            [core2.expression.map :as emap]
            [core2.operator.scan :as scan]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import (core2 ICursor)
           (core2.expression.map IRelationMap)
           (core2.operator IRelationSelector)
           (core2.vector IIndirectRelation)
           (java.util ArrayList Iterator List)
           (java.util.function Consumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (org.roaringbitmap IntConsumer RoaringBitmap)
           (org.roaringbitmap.buffer MutableRoaringBitmap)))

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
                                            idxs)))
                       row-count)))

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

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti ^core2.vector.IIndirectRelation probe-phase
  (fn [join-type allocator probe-rel rel-map probe-column-names matched-build-idxs theta-selector]
    join-type))

(defn- probe-inner-join-select
  "Returns a pair of selections [probe-sel, build-sel].

  The selections represent matched rows in both underlying relations.

  The selections will have the same size."
  [^IIndirectRelation probe-rel ^IRelationMap rel-map ]
  (let [rel-map-prober (.probeFromRelation rel-map probe-rel)
        matching-build-idxs (IntStream/builder)
        matching-probe-idxs (IntStream/builder)]

    (dotimes [probe-idx (.rowCount probe-rel)]
      (when-let [build-idxs (.getAll rel-map-prober probe-idx)]
        (.forEach build-idxs
                  (reify IntConsumer
                    (accept [_ build-idx]
                      (.add matching-build-idxs build-idx)
                      (.add matching-probe-idxs probe-idx))))))

    [(.toArray (.build matching-probe-idxs)) (.toArray (.build matching-build-idxs))]))

(defn- join-rels
  "Takes a relation (probe-rel) and its mapped relation (via rel-map) and returns a relation with the columns of both."
  [^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   [probe-sel build-sel :as _selection-pair]]
  (let [built-rel (.getBuiltRelation rel-map)]
    (iv/->indirect-rel (concat (iv/select built-rel build-sel)
                               (iv/select probe-rel probe-sel)))))

(defn- distinct-selection [^ints sel]
  (.toArray (RoaringBitmap/bitmapOf sel)))

(defn- probe-semi-join-select
  "Returns a single selection of the probe relation, that represents matches for a semi-join."
  ^ints [^IIndirectRelation probe-rel
         ^IRelationMap rel-map
         ^BufferAllocator allocator
         ^IRelationSelector theta-selector]
  (let [rel-map-prober (.probeFromRelation rel-map probe-rel)
        probe-semi-sel (let [matching-probe-idxs (IntStream/builder)]
                         (dotimes [probe-idx (.rowCount probe-rel)]
                           (when-not (neg? (.indexOf rel-map-prober probe-idx))
                             (.add matching-probe-idxs probe-idx)))
                         (.toArray (.build matching-probe-idxs)))]
    (if (nil? theta-selector)
      probe-semi-sel

      (let [probe-filtered-rel (iv/select probe-rel probe-semi-sel)
            [probe-join-sel _build-join-sel :as sel-pair] (probe-inner-join-select probe-filtered-rel rel-map)

            theta-base-rel (join-rels probe-filtered-rel rel-map sel-pair)
            theta-sel (.select theta-selector allocator theta-base-rel)]
        (-> (reduce iv/compose-selection [probe-semi-sel probe-join-sel theta-sel])
            (distinct-selection))))))

(defmethod probe-phase ::inner-join
  [_join-type
   ^BufferAllocator allocator
   ^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   _probe-column-names
   _matched-build-idxs
   ^IRelationSelector theta-selector]
  (let [joined-rel (join-rels probe-rel rel-map (probe-inner-join-select probe-rel rel-map))
        theta-sel (when theta-selector (.select theta-selector allocator joined-rel))]
    (cond-> joined-rel theta-sel (iv/select theta-sel))))

(defmethod probe-phase ::semi-join
  [_join-type
   ^BufferAllocator allocator
   ^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   _probe-column-names
   _matched-build-idxs
   ^IRelationSelector theta-selector]
  (iv/select probe-rel (probe-semi-join-select probe-rel rel-map allocator theta-selector)))

(defmethod probe-phase ::anti-semi-join
  [_join-type
   ^BufferAllocator allocator
   ^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   _probe-column-names
   _matched-build-idxs
   ^IRelationSelector theta-selector]

  (let [anti-stream (IntStream/builder)
        semi-bitmap (doto (MutableRoaringBitmap.) (.add (probe-semi-join-select probe-rel rel-map allocator theta-selector)))
        anti-sel (do (dotimes [idx (.rowCount ^IIndirectRelation probe-rel)]
                       (when-not (.contains semi-bitmap idx)
                         (.add anti-stream idx)))
                     (.toArray (.build anti-stream)))]
    (iv/select probe-rel anti-sel)))

(defn- int-array-concat
  ^ints [^ints arr1 ^ints arr2]
  (let [ret-arr (int-array (+ (alength arr1) (alength arr2)))]
    (System/arraycopy arr1 0 ret-arr 0 (alength arr1))
    (System/arraycopy arr2 0 ret-arr (alength arr1) (alength arr2))
    ret-arr))

(defn- probe-outer-join-select [^IIndirectRelation probe-rel, rel-map
                                ^BufferAllocator allocator
                                ^RoaringBitmap matched-build-idxs
                                ^IRelationSelector theta-selector]
  (let [[probe-sel build-sel :as selection-pair] (probe-inner-join-select probe-rel rel-map)
        inner-join-rel (join-rels probe-rel rel-map selection-pair)

        theta-sel (when theta-selector (.select theta-selector allocator inner-join-rel))
        ^ints theta-probe-sel (cond-> probe-sel theta-sel (iv/compose-selection theta-sel))
        ^ints theta-build-sel (cond-> build-sel theta-sel (iv/compose-selection theta-sel))

        _ (when matched-build-idxs (.add matched-build-idxs ^ints theta-build-sel))

        probe-bm (RoaringBitmap/bitmapOf theta-probe-sel)
        probe-int-stream (IntStream/builder)
        build-int-stream (IntStream/builder)

        _ (dotimes [idx (.rowCount probe-rel)]
            (when-not (.contains probe-bm idx)
              (.add probe-int-stream idx)
              (.add build-int-stream emap/nil-row-idx)))

        extra-probe-sel (.toArray (.build probe-int-stream))
        extra-build-sel (.toArray (.build build-int-stream))

        full-probe-sel (int-array-concat theta-probe-sel extra-probe-sel)
        full-build-sel (int-array-concat theta-build-sel extra-build-sel)]

    [full-probe-sel full-build-sel]))

(derive ::full-outer-join ::outer-join)
(derive ::left-outer-join ::outer-join)

(defmethod probe-phase ::outer-join
  [_join-type
   ^BufferAllocator allocator
   ^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   _probe-column-names
   ^RoaringBitmap matched-build-idxs
   ^IRelationSelector theta-selector]
  (->> (probe-outer-join-select probe-rel rel-map allocator matched-build-idxs theta-selector)
       (join-rels probe-rel rel-map)))

(deftype JoinCursor [^BufferAllocator allocator
                     ^ICursor build-cursor, build-key-column-names, build-column-names
                     ^ICursor probe-cursor, probe-key-column-names, probe-column-names
                     ^IRelationMap rel-map
                     ^RoaringBitmap matched-build-idxs
                     pushdown-blooms
                     join-type
                     ^IRelationSelector theta-selector]
  ICursor
  (tryAdvance [_ c]
    (.forEachRemaining build-cursor
                       (reify Consumer
                         (accept [_ build-rel]
                           (build-phase build-rel build-key-column-names rel-map pushdown-blooms))))

    (let [yield-missing? (contains? #{::anti-semi-join ::left-outer-join ::full-outer-join} join-type)]
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
                                             (when (pos? (.rowCount ^IIndirectRelation probe-rel))
                                               (with-open [out-rel (-> (probe-phase join-type allocator probe-rel rel-map probe-column-names matched-build-idxs theta-selector)
                                                                       (iv/copy allocator))]
                                                 (when (pos? (.rowCount out-rel))
                                                   (aset advanced? 0 true)
                                                   (.accept c out-rel))))))))))
              (aget advanced? 0))

            (when (= ::full-outer-join join-type)
              (let [build-rel (.getBuiltRelation rel-map)
                    build-row-count (long (.rowCount build-rel))
                    unmatched-build-idxs (RoaringBitmap/flip matched-build-idxs 0 build-row-count)]
                (.remove unmatched-build-idxs emap/nil-row-idx)

                (when-not (.isEmpty unmatched-build-idxs)
                  ;; this means .isEmpty will be true on the next iteration (we flip the bitmap)
                  (.add matched-build-idxs 0 build-row-count)

                  (let [nil-rel (emap/->nil-rel probe-column-names)
                        build-sel (.toArray unmatched-build-idxs)
                        probe-sel (int-array (alength build-sel))]
                    (.accept c (join-rels nil-rel rel-map [probe-sel build-sel]))
                    true))))))))

  (close [_]
    (run! #(.clear ^MutableRoaringBitmap %) pushdown-blooms)
    (util/try-close rel-map)
    (util/try-close build-cursor)
    (util/try-close probe-cursor)))

(defn ->equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                         ^ICursor left-cursor, left-key-column-names, left-column-names
                                         ^ICursor right-cursor, right-key-column-names, right-column-names
                                         theta-selector]
  (JoinCursor. allocator
               left-cursor left-key-column-names left-column-names
               right-cursor right-key-column-names right-column-names
               (emap/->relation-map allocator {:build-key-col-names left-key-column-names
                                               :probe-key-col-names right-key-column-names
                                               :store-col-names left-column-names})
               nil
               (vec (repeatedly (count right-key-column-names) #(MutableRoaringBitmap.)))
               ::inner-join
               theta-selector))

(defn ->left-semi-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                   ^ICursor left-cursor, left-key-column-names, left-column-names
                                                   ^ICursor right-cursor, right-key-column-names, right-column-names
                                                   theta-selector]
  (JoinCursor. allocator
               right-cursor right-key-column-names right-column-names
               left-cursor left-key-column-names left-column-names
               (emap/->relation-map allocator {:build-key-col-names right-key-column-names
                                               :probe-key-col-names left-key-column-names
                                               :store-col-names right-column-names})
               nil
               (vec (repeatedly (count right-key-column-names) #(MutableRoaringBitmap.)))
               ::semi-join
               theta-selector))

(defn ->left-outer-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                    ^ICursor left-cursor, left-key-column-names, left-column-names
                                                    ^ICursor right-cursor, right-key-column-names, right-column-names
                                                    theta-selector]
  (JoinCursor. allocator
               right-cursor right-key-column-names right-column-names
               left-cursor left-key-column-names left-column-names
               (emap/->relation-map allocator {:build-key-col-names right-key-column-names
                                               :probe-key-col-names left-key-column-names
                                               :store-col-names right-column-names
                                               :with-nil-row? true})
               nil
               (vec (repeatedly (count right-key-column-names) #(MutableRoaringBitmap.)))
               ::left-outer-join
               theta-selector))

(defn ->full-outer-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                    ^ICursor left-cursor, left-key-column-names, left-column-names
                                                    ^ICursor right-cursor, right-key-column-names, right-column-names
                                                    theta-selector]
  (JoinCursor. allocator
               left-cursor left-key-column-names left-column-names
               right-cursor right-key-column-names right-column-names
               (emap/->relation-map allocator {:build-key-col-names left-key-column-names
                                               :probe-key-col-names right-key-column-names
                                               :store-col-names left-column-names
                                               :with-nil-row? true})
               (RoaringBitmap.)
               (vec (repeatedly (count right-key-column-names) #(MutableRoaringBitmap.)))
               ::full-outer-join
               theta-selector))

(defn ->left-anti-semi-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                        ^ICursor left-cursor, left-key-column-names, left-column-names
                                                        ^ICursor right-cursor, right-key-column-names, right-column-names
                                                        theta-selector]
  (JoinCursor. allocator
               right-cursor right-key-column-names right-column-names
               left-cursor left-key-column-names left-column-names
               (emap/->relation-map allocator {:build-key-col-names right-key-column-names
                                               :probe-key-col-names left-key-column-names
                                               :store-col-names right-column-names})
               nil
               (vec (repeatedly (count right-key-column-names) #(MutableRoaringBitmap.)))
               ::anti-semi-join
               theta-selector))
