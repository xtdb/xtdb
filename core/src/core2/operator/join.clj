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
           (core2.operator IProjectionSpec IRelationSelector)
           (core2.vector IIndirectRelation)
           (java.util ArrayList Iterator List)
           (java.util.function Consumer IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
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

(defn- build-phase [^IIndirectRelation build-rel
                    ^IRelationMap rel-map
                    pushdown-blooms]
  (let [rel-map-builder (.buildFromRelation rel-map build-rel)
        build-key-col-names (vec (.buildKeyColumnNames rel-map))]
    (dotimes [build-idx (.rowCount build-rel)]
      (.add rel-map-builder build-idx))

    (dotimes [col-idx (count build-key-col-names)]
      (let [build-col-name (nth build-key-col-names col-idx)
            build-col (.vectorForName build-rel (name build-col-name))
            internal-vec (.getVector build-col)
            ^MutableRoaringBitmap pushdown-bloom (nth pushdown-blooms col-idx)]
        (dotimes [build-idx (.rowCount build-rel)]
          (.add pushdown-bloom ^ints (bloom/bloom-hashes internal-vec (.getIndex build-col build-idx))))))))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti ^core2.vector.IIndirectRelation probe-phase
  (fn [join-type allocator probe-rel rel-map matched-build-idxs theta-selector params]
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

(defn- distinct-selection [^ints sel]
  (.toArray (RoaringBitmap/bitmapOf sel)))

(defn- probe-semi-join-select
  "Returns a single selection of the probe relation, that represents matches for a semi-join."
  ^ints [^IIndirectRelation probe-rel
         ^IRelationMap rel-map
         ^BufferAllocator allocator
         ^IRelationSelector theta-selector
         params]
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
            theta-sel (.select theta-selector allocator theta-base-rel params)]
        (-> (reduce iv/compose-selection [probe-semi-sel probe-join-sel theta-sel])
            (distinct-selection))))))

(defmethod probe-phase ::inner-join
  [_join-type
   ^BufferAllocator allocator
   ^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   _matched-build-idxs
   ^IRelationSelector theta-selector
   params]
  (let [joined-rel (join-rels probe-rel rel-map (probe-inner-join-select probe-rel rel-map))
        theta-sel (when theta-selector (.select theta-selector allocator joined-rel params))]
    (cond-> joined-rel theta-sel (iv/select theta-sel))))

(defmethod probe-phase ::semi-join
  [_join-type
   ^BufferAllocator allocator
   ^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   _matched-build-idxs
   ^IRelationSelector theta-selector
   params]
  (iv/select probe-rel (probe-semi-join-select probe-rel rel-map allocator theta-selector params)))

(defmethod probe-phase ::anti-semi-join
  [_join-type
   ^BufferAllocator allocator
   ^IIndirectRelation probe-rel
   ^IRelationMap rel-map
   _matched-build-idxs
   ^IRelationSelector theta-selector
   params]
  (let [anti-stream (IntStream/builder)
        semi-bitmap (doto (MutableRoaringBitmap.) (.add (probe-semi-join-select probe-rel rel-map allocator theta-selector params)))
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
                                ^IRelationSelector theta-selector
                                params]
  (let [[probe-sel build-sel :as selection-pair] (probe-inner-join-select probe-rel rel-map)
        inner-join-rel (join-rels probe-rel rel-map selection-pair)

        theta-sel (when theta-selector (.select theta-selector allocator inner-join-rel params))
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
   ^RoaringBitmap matched-build-idxs
   ^IRelationSelector theta-selector
   params]
  (->> (probe-outer-join-select probe-rel rel-map allocator matched-build-idxs theta-selector params)
       (join-rels probe-rel rel-map)))

(deftype JoinCursor [^BufferAllocator allocator, ^ICursor build-cursor, ^ICursor probe-cursor
                     ^IRelationMap rel-map
                     ^RoaringBitmap matched-build-idxs
                     pushdown-blooms
                     join-type
                     ^IRelationSelector theta-selector
                     params]
  ICursor
  (tryAdvance [_ c]
    (.forEachRemaining build-cursor
                       (reify Consumer
                         (accept [_ build-rel]
                           (build-phase build-rel rel-map pushdown-blooms))))

    (let [yield-missing? (contains? #{::anti-semi-join ::left-outer-join ::full-outer-join} join-type)]
      (boolean
        (or (let [advanced? (boolean-array 1)]
              (binding [scan/*column->pushdown-bloom* (cond-> scan/*column->pushdown-bloom*
                                                        (not yield-missing?) (conj (zipmap (.probeKeyColumnNames rel-map) pushdown-blooms)))]
                (while (and (not (aget advanced? 0))
                            (.tryAdvance probe-cursor
                                         (reify Consumer
                                           (accept [_ probe-rel]
                                             (when (pos? (.rowCount ^IIndirectRelation probe-rel))
                                               (with-open [out-rel (-> (probe-phase join-type allocator probe-rel rel-map matched-build-idxs theta-selector params)
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
                    true))))))))

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
      (let [original-col-names (set/union (set (keys left-col-types))
                                          (set (keys right-col-types)))

            {equis :equi-condition, thetas :pred-expr} (group-by first condition)

            theta-selector (when-let [theta-exprs (seq (map second thetas))]
                             (let [col-types (merge-with types/merge-col-types left-col-types right-col-types)]
                               (expr/->expression-relation-selector (list* 'and theta-exprs)
                                                                    {:col-types col-types, :param-types param-types})))

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
                                             :theta-selector theta-selector})

            project-away-specs (->> (set/intersection (set (keys col-types)) original-col-names)
                                    (mapv #(project/->identity-projection-spec % (get col-types %))))]

        {:col-types (projection-specs->col-types project-away-specs)
         :->cursor (fn [opts left-cursor right-cursor]
                     (let [left-project-cursor (project/->project-cursor opts left-cursor left-projections)
                           right-project-cursor (project/->project-cursor opts right-cursor right-projections)

                           join-cursor (->cursor opts left-project-cursor right-project-cursor)]

                       (project/->project-cursor opts join-cursor project-away-specs)))}))))

(defmethod lp/emit-expr :join [join-expr args]
  (emit-join-expr join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-selector]}]
      {:col-types (merge-with types/merge-col-types left-col-types right-col-types)
       :->cursor (fn [{:keys [allocator params]} left-cursor right-cursor]
                   (JoinCursor. allocator left-cursor right-cursor
                                (emap/->relation-map allocator {:build-col-types left-col-types
                                                                :build-key-col-names left-key-col-names
                                                                :probe-col-types right-col-types
                                                                :probe-key-col-names right-key-col-names
                                                                :store-full-build-rel? true})
                                nil
                                (vec (repeatedly (count right-key-col-names) #(MutableRoaringBitmap.)))
                                ::inner-join
                                theta-selector
                                params))})))

(defn- with-nullable-cols [col-types]
  (->> col-types
       (into {} (map (juxt key (comp #(types/merge-col-types % :null) val))))))

(defmethod lp/emit-expr :left-outer-join [join-expr args]
  (emit-join-expr join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-selector]}]
      {:col-types (merge-with types/merge-col-types left-col-types (-> right-col-types with-nullable-cols))
       :->cursor (fn [{:keys [allocator params]}, left-cursor right-cursor]
                   (JoinCursor. allocator right-cursor left-cursor
                                (emap/->relation-map allocator {:build-col-types right-col-types
                                                                :build-key-col-names right-key-col-names
                                                                :probe-col-types left-col-types
                                                                :probe-key-col-names left-key-col-names
                                                                :store-full-build-rel? true
                                                                :with-nil-row? true})
                                nil
                                (vec (repeatedly (count right-key-col-names) #(MutableRoaringBitmap.)))
                                ::left-outer-join
                                theta-selector
                                params))})))

(defmethod lp/emit-expr :full-outer-join [join-expr args]
  (emit-join-expr join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-selector]}]
      {:col-types (merge-with types/merge-col-types (-> left-col-types with-nullable-cols) (-> right-col-types with-nullable-cols))
       :->cursor (fn [{:keys [allocator params]}, left-cursor right-cursor]
                   (JoinCursor. allocator left-cursor right-cursor
                                (emap/->relation-map allocator {:build-col-types left-col-types
                                                                :build-key-col-names left-key-col-names
                                                                :probe-col-types right-col-types
                                                                :probe-key-col-names right-key-col-names
                                                                :store-full-build-rel? true
                                                                :with-nil-row? true})
                                (RoaringBitmap.)
                                (vec (repeatedly (count right-key-col-names) #(MutableRoaringBitmap.)))
                                ::full-outer-join
                                theta-selector
                                params))})))

(defmethod lp/emit-expr :semi-join [join-expr args]
  (emit-join-expr join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-selector]}]
      {:col-types left-col-types
       :->cursor (fn [{:keys [allocator params]} left-cursor right-cursor]
                   (JoinCursor. allocator right-cursor left-cursor
                                (emap/->relation-map allocator {:build-col-types right-col-types
                                                                :build-key-col-names right-key-col-names
                                                                :probe-col-types left-col-types
                                                                :probe-key-col-names left-key-col-names
                                                                :store-full-build-rel? true})
                                nil
                                (vec (repeatedly (count right-key-col-names) #(MutableRoaringBitmap.)))
                                ::semi-join
                                theta-selector
                                params))})))

(defmethod lp/emit-expr :anti-join [join-expr args]
  (emit-join-expr join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-selector]}]
      {:col-types left-col-types
       :->cursor (fn [{:keys [allocator params]} left-cursor right-cursor]
                   (JoinCursor. allocator right-cursor left-cursor
                                (emap/->relation-map allocator {:build-col-types right-col-types
                                                                :build-key-col-names right-key-col-names
                                                                :probe-col-types left-col-types
                                                                :probe-key-col-names left-key-col-names
                                                                :store-full-build-rel? true})
                                nil
                                (vec (repeatedly (count right-key-col-names) #(MutableRoaringBitmap.)))
                                ::anti-semi-join
                                theta-selector
                                params))})))
