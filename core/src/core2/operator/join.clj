(ns core2.operator.join
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [core2.bloom :as bloom]
            [core2.expression :as expr]
            [core2.expression.map :as emap]
            [core2.logical-plan :as lp]
            [core2.operator.project :as project]
            [core2.operator.scan :as scan]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import (core2 ICursor)
           (core2.expression.map IRelationMap)
           (core2.operator IProjectionSpec)
           (core2.vector IIndirectRelation)
           (java.util ArrayList Iterator List)
           (java.util.function Consumer IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           org.apache.arrow.vector.BitVector
           org.roaringbitmap.RoaringBitmap
           (org.roaringbitmap.buffer MutableRoaringBitmap)))

(defmethod lp/ra-expr :cross-join [_]
  (s/cat :op #{:⨯ :cross-join}
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(s/def ::join-equi-clause (s/map-of ::lp/expression ::lp/expression :conform-keys true :count 1))

(s/def ::join-condition-clause
  (s/or :equi-condition ::join-equi-clause
        :pred-expr ::lp/expression))

(s/def ::join-condition
  (s/coll-of ::join-condition-clause :kind vector?))

(defmethod lp/ra-expr :join [_]
  (s/cat :op #{:⋈ :join}
         :condition ::join-condition
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :left-outer-join [_]
  (s/cat :op #{:⟕ :left-outer-join}
         :condition ::join-condition
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :full-outer-join [_]
  (s/cat :op #{:⟗ :full-outer-join}
         :condition ::join-condition
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :semi-join [_]
  (s/cat :op #{:⋉ :semi-join}
         :condition ::join-condition
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :anti-join [_]
  (s/cat :op #{:▷ :anti-join}
         :condition ::join-condition
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :mark-join [_]
  (s/cat :op #{:mark-join}
         :mark-spec (s/map-of ::lp/column ::join-condition, :count 1, :conform-keys true)
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

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

(defmethod lp/emit-expr :cross-join [{:keys [left right]} args]
  (lp/binary-expr left right args
    (fn [left-col-types right-col-types]
      {:col-types (merge left-col-types right-col-types)
       :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                   (CrossJoinCursor. allocator left-cursor right-cursor (ArrayList.) nil nil))})))

(defn- build-phase [^ICursor build-cursor, ^IRelationMap rel-map, pushdown-blooms]
  (.forEachRemaining build-cursor
                     (reify Consumer
                       (accept [_ build-rel]
                         (let [^IIndirectRelation build-rel build-rel rel-map-builder (.buildFromRelation rel-map build-rel)
                               build-key-col-names (vec (.buildKeyColumnNames rel-map))]
                           (dotimes [build-idx (.rowCount build-rel)]
                             (.add rel-map-builder build-idx))

                           (when pushdown-blooms
                             (dotimes [col-idx (count build-key-col-names)]
                               (let [build-col-name (nth build-key-col-names col-idx)
                                     build-col (.vectorForName build-rel (name build-col-name))
                                     internal-vec (.getVector build-col)
                                     ^MutableRoaringBitmap pushdown-bloom (nth pushdown-blooms col-idx)]
                                 (dotimes [build-idx (.rowCount build-rel)]
                                   (.add pushdown-bloom ^ints (bloom/bloom-hashes internal-vec (.getIndex build-col build-idx))))))))))))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti ^core2.vector.IIndirectRelation probe-phase
  (fn [join-type probe-rel rel-map matched-build-idxs]
    join-type))

(defn- probe-inner-join-select
  "Returns a pair of selections [probe-sel, build-sel].

  The selections represent matched rows in both underlying relations.

  The selections will have the same size."
  [^IIndirectRelation probe-rel ^IRelationMap rel-map]
  (let [rel-map-prober (.probeFromRelation rel-map probe-rel)
        matching-build-idxs (IntStream/builder)
        matching-probe-idxs (IntStream/builder)]

    (dotimes [probe-idx (.rowCount probe-rel)]
      (.forEachMatch rel-map-prober probe-idx
                     (reify IntConsumer
                       (accept [_ build-idx]
                         (.add matching-build-idxs build-idx)
                         (.add matching-probe-idxs probe-idx)))))

    [(.toArray (.build matching-probe-idxs)) (.toArray (.build matching-build-idxs))]))

(defn- join-rels
  "Takes a relation (probe-rel) and its mapped relation (via rel-map) and returns a relation with the columns of both."
  [^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   [probe-sel build-sel :as _selection-pair]]
  (let [built-rel (.getBuiltRelation rel-map)]
    (iv/->indirect-rel (concat (iv/select built-rel build-sel)
                               (iv/select probe-rel probe-sel)))))

(defn- probe-semi-join-select
  "Returns a single selection of the probe relation, that represents matches for a semi-join."
  ^ints [^IIndirectRelation probe-rel, ^IRelationMap rel-map]
  (let [rel-map-prober (.probeFromRelation rel-map probe-rel)
        matching-probe-idxs (IntStream/builder)]
    (dotimes [probe-idx (.rowCount probe-rel)]
      (when-not (neg? (.indexOf rel-map-prober probe-idx))
        (.add matching-probe-idxs probe-idx)))
    (.toArray (.build matching-probe-idxs))))

(defmethod probe-phase ::inner-join
  [_join-type
   ^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   _matched-build-idxs]
  (join-rels probe-rel rel-map (probe-inner-join-select probe-rel rel-map)))

(defmethod probe-phase ::semi-join
  [_join-type
   ^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   _matched-build-idxs]
  (iv/select probe-rel (probe-semi-join-select probe-rel rel-map)))

(defmethod probe-phase ::anti-semi-join
  [_join-type, ^IIndirectRelation probe-rel, ^IRelationMap rel-map, _matched-build-idxs]
  (iv/select probe-rel
             (-> (doto (MutableRoaringBitmap.)
                   (.add (probe-semi-join-select probe-rel rel-map))
                   (.flip (int 0) (.rowCount ^IIndirectRelation probe-rel)))
                 (.toArray))))

(defmethod probe-phase ::mark-join
  [_join-type
   ^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   _matched-build-idxs]
  (iv/select probe-rel (probe-semi-join-select probe-rel rel-map)))

(defn- int-array-concat
  ^ints [^ints arr1 ^ints arr2]
  (let [ret-arr (int-array (+ (alength arr1) (alength arr2)))]
    (System/arraycopy arr1 0 ret-arr 0 (alength arr1))
    (System/arraycopy arr2 0 ret-arr (alength arr1) (alength arr2))
    ret-arr))

(defn- probe-outer-join-select [^IIndirectRelation probe-rel, rel-map, ^RoaringBitmap matched-build-idxs]
  (let [[probe-sel build-sel] (probe-inner-join-select probe-rel rel-map)

        _ (when matched-build-idxs (.add matched-build-idxs ^ints build-sel))

        probe-bm (RoaringBitmap/bitmapOf probe-sel)
        probe-int-stream (IntStream/builder)
        build-int-stream (IntStream/builder)

        _ (dotimes [idx (.rowCount probe-rel)]
            (when-not (.contains probe-bm idx)
              (.add probe-int-stream idx)
              (.add build-int-stream emap/nil-row-idx)))

        extra-probe-sel (.toArray (.build probe-int-stream))
        extra-build-sel (.toArray (.build build-int-stream))

        full-probe-sel (int-array-concat probe-sel extra-probe-sel)
        full-build-sel (int-array-concat build-sel extra-build-sel)]

    [full-probe-sel full-build-sel]))

(derive ::full-outer-join ::outer-join)
(derive ::left-outer-join ::outer-join)

(defmethod probe-phase ::outer-join
  [_join-type
   ^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   ^RoaringBitmap matched-build-idxs]
  (->> (probe-outer-join-select probe-rel rel-map matched-build-idxs)
       (join-rels probe-rel rel-map)))

(deftype JoinCursor [^BufferAllocator allocator, ^ICursor build-cursor, ^ICursor probe-cursor
                     ^IRelationMap rel-map
                     ^RoaringBitmap matched-build-idxs
                     pushdown-blooms
                     join-type]
  ICursor
  (tryAdvance [_ c]
    (build-phase build-cursor rel-map pushdown-blooms)

    (boolean
     (or (let [advanced? (boolean-array 1)]
           (binding [scan/*column->pushdown-bloom* (cond-> scan/*column->pushdown-bloom*
                                                     (some? pushdown-blooms) (conj (zipmap (.probeKeyColumnNames rel-map) pushdown-blooms)))]
             (while (and (not (aget advanced? 0))
                         (.tryAdvance probe-cursor
                                      (reify Consumer
                                        (accept [_ probe-rel]
                                          (when (pos? (.rowCount ^IIndirectRelation probe-rel))
                                            (with-open [out-rel (-> (probe-phase join-type probe-rel rel-map matched-build-idxs)
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

               (let [nil-rel (emap/->nil-rel (set (keys (.probeColumnTypes rel-map))))
                     build-sel (.toArray unmatched-build-idxs)
                     probe-sel (int-array (alength build-sel))]
                 (.accept c (join-rels nil-rel rel-map [probe-sel build-sel]))
                 true)))))))

  (close [_]
    (run! #(.clear ^MutableRoaringBitmap %) pushdown-blooms)
    (util/try-close rel-map)
    (util/try-close build-cursor)
    (util/try-close probe-cursor)))

(defn- equi-spec [idx condition left-col-types right-col-types param-types]
  (let [[left-expr right-expr] (first condition)]
    (letfn [(equi-projection [side expr col-types]
              (if (symbol? expr)
                {:key-col-name expr}

                (let [col-name (symbol (format "?join-expr-%s-%d" (name side) idx))]
                  {:key-col-name col-name
                   :projection (expr/->expression-projection-spec col-name expr
                                                                  {:col-types col-types
                                                                   :param-types param-types})})))]

      {:left (equi-projection :left left-expr left-col-types)
       :right (equi-projection :right right-expr right-col-types)})))

(defn- projection-specs->col-types [projection-specs]
  (->> projection-specs
       (into {} (map (juxt #(.getColumnName ^IProjectionSpec %)
                           #(.getColumnType ^IProjectionSpec %))))))

(defn- emit-join-expr {:style/indent 2} [{:keys [condition left right]} {:keys [param-types] :as args} f]
  (lp/binary-expr left right args
    (fn [left-col-types right-col-types]
      (let [{equis :equi-condition, thetas :pred-expr} (group-by first condition)

            theta-expr (when-let [theta-exprs (seq (map second thetas))]
                         (list* 'and theta-exprs))

            equi-specs (->> (vals equis)
                            (into [] (map-indexed (fn [idx condition]
                                                    (equi-spec idx condition left-col-types right-col-types param-types)))))

            left-projections (vec (concat (for [[col-name col-type] left-col-types]
                                            (project/->identity-projection-spec col-name col-type))
                                          (keep (comp :projection :left) equi-specs)))

            right-projections (vec (concat (for [[col-name col-type] right-col-types]
                                             (project/->identity-projection-spec col-name col-type))
                                           (keep (comp :projection :right) equi-specs)))

            {:keys [col-types ->cursor]} (f {:left-col-types (projection-specs->col-types left-projections)
                                             :left-key-col-names (mapv (comp :key-col-name :left) equi-specs)
                                             :right-col-types (projection-specs->col-types right-projections)
                                             :right-key-col-names (mapv (comp :key-col-name :right) equi-specs)
                                             :theta-expr theta-expr})

            project-away-specs (->> (set/difference (set (keys col-types))
                                                    (->> equi-specs
                                                         (into #{} (comp (mapcat (juxt :left :right))
                                                                         (filter :projection)
                                                                         (map :key-col-name)))))
                                    (mapv #(project/->identity-projection-spec % (get col-types %))))]

        {:col-types (projection-specs->col-types project-away-specs)
         :->cursor (fn [opts left-cursor right-cursor]
                     (let [left-project-cursor (project/->project-cursor opts left-cursor left-projections)
                           right-project-cursor (project/->project-cursor opts right-cursor right-projections)

                           join-cursor (->cursor opts left-project-cursor right-project-cursor)]

                       (project/->project-cursor opts join-cursor project-away-specs)))}))))

(defn- ->pushdown-blooms [key-col-names]
  (vec (repeatedly (count key-col-names) #(MutableRoaringBitmap.))))

(defmethod lp/emit-expr :join [join-expr {:keys [param-types] :as args}]
  (emit-join-expr join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
      {:col-types (merge-with types/merge-col-types left-col-types right-col-types)
       :->cursor (fn [{:keys [allocator params]} left-cursor right-cursor]
                   (JoinCursor. allocator left-cursor right-cursor
                                (emap/->relation-map allocator {:build-col-types left-col-types
                                                                :build-key-col-names left-key-col-names
                                                                :probe-col-types right-col-types
                                                                :probe-key-col-names right-key-col-names
                                                                :store-full-build-rel? true
                                                                :theta-expr theta-expr
                                                                :param-types param-types
                                                                :params params})
                                nil (->pushdown-blooms right-key-col-names) ::inner-join))})))

(defmethod lp/emit-expr :left-outer-join [join-expr {:keys [param-types] :as args}]
  (emit-join-expr join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
      {:col-types (merge-with types/merge-col-types left-col-types (-> right-col-types types/with-nullable-cols))
       :->cursor (fn [{:keys [allocator params]}, left-cursor right-cursor]
                   (JoinCursor. allocator right-cursor left-cursor
                                (emap/->relation-map allocator {:build-col-types right-col-types
                                                                :build-key-col-names right-key-col-names
                                                                :probe-col-types left-col-types
                                                                :probe-key-col-names left-key-col-names
                                                                :store-full-build-rel? true
                                                                :with-nil-row? true
                                                                :theta-expr theta-expr
                                                                :param-types param-types
                                                                :params params})
                                nil nil ::left-outer-join))})))

(defmethod lp/emit-expr :full-outer-join [join-expr {:keys [param-types] :as args}]
  (emit-join-expr join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
      {:col-types (merge-with types/merge-col-types (-> left-col-types types/with-nullable-cols) (-> right-col-types types/with-nullable-cols))
       :->cursor (fn [{:keys [allocator params]}, left-cursor right-cursor]
                   (JoinCursor. allocator left-cursor right-cursor
                                (emap/->relation-map allocator {:build-col-types left-col-types
                                                                :build-key-col-names left-key-col-names
                                                                :probe-col-types right-col-types
                                                                :probe-key-col-names right-key-col-names
                                                                :store-full-build-rel? true
                                                                :with-nil-row? true
                                                                :theta-expr theta-expr
                                                                :param-types param-types
                                                                :params params})
                                (RoaringBitmap.) nil ::full-outer-join))})))

(defmethod lp/emit-expr :semi-join [join-expr {:keys [param-types] :as args}]
  (emit-join-expr join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
      {:col-types left-col-types
       :->cursor (fn [{:keys [allocator params]} left-cursor right-cursor]
                   (JoinCursor. allocator right-cursor left-cursor
                                (emap/->relation-map allocator {:build-col-types right-col-types
                                                                :build-key-col-names right-key-col-names
                                                                :probe-col-types left-col-types
                                                                :probe-key-col-names left-key-col-names
                                                                :store-full-build-rel? true
                                                                :theta-expr theta-expr
                                                                :param-types param-types
                                                                :params params})
                                nil (->pushdown-blooms right-key-col-names) ::semi-join))})))

(defmethod lp/emit-expr :anti-join [join-expr {:keys [param-types] :as args}]
  (emit-join-expr join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
      {:col-types left-col-types
       :->cursor (fn [{:keys [allocator params]} left-cursor right-cursor]
                   (JoinCursor. allocator right-cursor left-cursor
                                (emap/->relation-map allocator {:build-col-types right-col-types
                                                                :build-key-col-names right-key-col-names
                                                                :probe-col-types left-col-types
                                                                :probe-key-col-names left-key-col-names
                                                                :store-full-build-rel? true
                                                                :theta-expr theta-expr
                                                                :param-types param-types
                                                                :params params})
                                nil nil ::anti-semi-join))})))

(defn- mark-join-probe-phase [^IRelationMap rel-map, ^IIndirectRelation probe-rel, ^BitVector mark-col]
  (let [rel-prober (.probeFromRelation rel-map probe-rel)]
    (dotimes [idx (.rowCount probe-rel)]
      (let [match-res (.matches rel-prober idx)]
        (if (zero? match-res)
          (.setNull mark-col idx)
          (.set mark-col idx (case match-res 1 1, -1 0)))))))

(defmethod lp/emit-expr :mark-join [{:keys [mark-spec] :as join-expr} {:keys [param-types] :as args}]
  (let [[mark-col-name mark-condition] (first mark-spec)]
    (emit-join-expr (assoc join-expr :condition mark-condition) args
      (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
        {:col-types (assoc left-col-types mark-col-name [:union #{:bool :null}])

         :->cursor
         (fn [{:keys [^BufferAllocator allocator params]} ^ICursor probe-cursor build-cursor]
           (let [rel-map (emap/->relation-map allocator {:build-col-types right-col-types
                                                         :build-key-col-names right-key-col-names
                                                         :probe-col-types left-col-types
                                                         :probe-key-col-names left-key-col-names
                                                         :store-full-build-rel? true
                                                         :theta-expr theta-expr
                                                         :param-types param-types
                                                         :params params})
                 pushdown-blooms (vec (repeatedly (count right-key-col-names) #(MutableRoaringBitmap.)))]

             (reify ICursor
               (tryAdvance [_ c]
                 (build-phase build-cursor rel-map pushdown-blooms)

                 (boolean
                  (let [advanced? (boolean-array 1)]
                    (binding [scan/*column->pushdown-bloom* (conj scan/*column->pushdown-bloom*
                                                                  (zipmap (.probeKeyColumnNames rel-map) pushdown-blooms))]
                      (while (and (not (aget advanced? 0))
                                  (.tryAdvance probe-cursor
                                               (reify Consumer
                                                 (accept [_ probe-rel]
                                                   (let [row-count (.rowCount ^IIndirectRelation probe-rel)]
                                                     (when (pos? row-count)
                                                       (aset advanced? 0 true)

                                                       (with-open [probe-rel (iv/copy probe-rel allocator)
                                                                   mark-col (doto (BitVector. (name mark-col-name) allocator)
                                                                              (.allocateNew row-count)
                                                                              (.setValueCount row-count))]
                                                         (mark-join-probe-phase rel-map probe-rel mark-col)
                                                         (let [out-cols (conj (seq probe-rel) (iv/->direct-vec mark-col))]
                                                           (.accept c (iv/->indirect-rel out-cols row-count))))))))))))
                    (aget advanced? 0))))

               (close [_]
                 (run! #(.clear ^MutableRoaringBitmap %) pushdown-blooms)
                 (util/try-close rel-map)
                 (util/try-close build-cursor)
                 (util/try-close probe-cursor)))))}))))
