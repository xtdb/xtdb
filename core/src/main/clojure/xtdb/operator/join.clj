(ns xtdb.operator.join
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string]
            [clojure.walk :as walk]
            [xtdb.bloom :as bloom]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.expression.map :as emap]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.project :as project]
            [xtdb.operator.scan :as scan]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import [clojure.lang IFn]
           (java.util ArrayList Iterator List)
           (java.util.function Consumer IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           org.apache.arrow.vector.BitVector
           (org.roaringbitmap.buffer MutableRoaringBitmap)
           org.roaringbitmap.RoaringBitmap
           (xtdb ICursor)
           (xtdb.expression.map IRelationMap)
           (xtdb.operator IProjectionSpec)
           (xtdb.vector RelationReader)))

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

(defmethod lp/ra-expr :single-join [_]
  (s/cat :op #{:single-join}
         :condition ::join-condition
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :mega-join [_]
  (s/cat :op #{:mega-join}
         :conditions ::join-condition
         :relations (s/coll-of ::lp/ra-expression)))

(set! *unchecked-math* :warn-on-boxed)

(defn emit-join-children [join-expr args]
  (-> join-expr
      (update :left #(lp/emit-expr % args))
      (update :right #(lp/emit-expr % args))) )

(defn- cross-product ^xtdb.vector.RelationReader [^RelationReader left-rel, ^RelationReader right-rel]
  (let [left-row-count (.rowCount left-rel)
        right-row-count (.rowCount right-rel)
        row-count (* left-row-count right-row-count)]
    (vr/rel-reader (concat (.select left-rel
                                    (let [idxs (int-array row-count)]
                                      (dotimes [idx row-count]
                                        (aset idxs idx ^long (quot idx right-row-count)))
                                      idxs))

                           (.select right-rel
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
                          ^:unsynchronized-mutable ^RelationReader right-rel]
  ICursor
  (tryAdvance [this c]
    (.forEachRemaining left-cursor
                       (reify Consumer
                         (accept [_ left-rel]
                           (.add left-rels (.copy ^RelationReader left-rel allocator)))))

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
                                                        (set! (.right-rel this) (.copy ^RelationReader right-rel allocator))
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

(defn emit-cross-join [{:keys [left right]}]
  (lp/binary-expr left right
    (fn [left-col-types right-col-types]
      {:col-types (merge left-col-types right-col-types)
       :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                   (CrossJoinCursor. allocator left-cursor right-cursor (ArrayList.) nil nil))})))

(defmethod lp/emit-expr :cross-join [join-expr args]
  (emit-cross-join (emit-join-children join-expr args)))

(defn- build-phase [^ICursor build-cursor, ^IRelationMap rel-map, pushdown-blooms]
  (.forEachRemaining build-cursor
                     (reify Consumer
                       (accept [_ build-rel]
                         (let [^RelationReader build-rel build-rel rel-map-builder (.buildFromRelation rel-map build-rel)
                               build-key-col-names (vec (.buildKeyColumnNames rel-map))]
                           (dotimes [build-idx (.rowCount build-rel)]
                             (.add rel-map-builder build-idx))

                           (when pushdown-blooms
                             (dotimes [col-idx (count build-key-col-names)]
                               (let [build-col-name (nth build-key-col-names col-idx)
                                     build-col (.readerForName build-rel (name build-col-name))
                                     ^MutableRoaringBitmap pushdown-bloom (nth pushdown-blooms col-idx)]
                                 (dotimes [build-idx (.rowCount build-rel)]
                                   (.add pushdown-bloom ^ints (bloom/bloom-hashes build-col build-idx)))))))))))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti ^xtdb.vector.RelationReader probe-phase
  (fn [join-type probe-rel rel-map matched-build-idxs]
    join-type))

(defn- probe-inner-join-select
  "Returns a pair of selections [probe-sel, build-sel].

  The selections represent matched rows in both underlying relations.

  The selections will have the same size."
  [^RelationReader probe-rel ^IRelationMap rel-map]
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
  [^RelationReader probe-rel
   ^IRelationMap rel-map
   [probe-sel build-sel :as _selection-pair]]
  (let [built-rel (.getBuiltRelation rel-map)]
    (vr/rel-reader (concat (.select built-rel build-sel)
                           (.select probe-rel probe-sel)))))

(defn- probe-semi-join-select
  "Returns a single selection of the probe relation, that represents matches for a semi-join."
  ^ints [^RelationReader probe-rel, ^IRelationMap rel-map]
  (let [rel-map-prober (.probeFromRelation rel-map probe-rel)
        matching-probe-idxs (IntStream/builder)]
    (dotimes [probe-idx (.rowCount probe-rel)]
      (when-not (neg? (.indexOf rel-map-prober probe-idx false))
        (.add matching-probe-idxs probe-idx)))
    (.toArray (.build matching-probe-idxs))))

(defmethod probe-phase ::inner-join
  [_join-type
   ^RelationReader probe-rel
   ^IRelationMap rel-map
   _matched-build-idxs]
  (join-rels probe-rel rel-map (probe-inner-join-select probe-rel rel-map)))

(defmethod probe-phase ::semi-join
  [_join-type
   ^RelationReader probe-rel
   ^IRelationMap rel-map
   _matched-build-idxs]
  (.select probe-rel (probe-semi-join-select probe-rel rel-map)))

(defmethod probe-phase ::anti-semi-join
  [_join-type, ^RelationReader probe-rel, ^IRelationMap rel-map, _matched-build-idxs]
  (.select probe-rel
           (-> (doto (MutableRoaringBitmap.)
                 (.add (probe-semi-join-select probe-rel rel-map))
                 (.flip (int 0) (.rowCount ^RelationReader probe-rel)))
               (.toArray))))

(defmethod probe-phase ::mark-join
  [_join-type
   ^RelationReader probe-rel
   ^IRelationMap rel-map
   _matched-build-idxs]
  (.select probe-rel (probe-semi-join-select probe-rel rel-map)))

(defn- int-array-concat
  ^ints [^ints arr1 ^ints arr2]
  (let [ret-arr (int-array (+ (alength arr1) (alength arr2)))]
    (System/arraycopy arr1 0 ret-arr 0 (alength arr1))
    (System/arraycopy arr2 0 ret-arr (alength arr1) (alength arr2))
    ret-arr))

(defn- probe-outer-join-select [^RelationReader probe-rel, rel-map, ^RoaringBitmap matched-build-idxs]
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
   ^RelationReader probe-rel
   ^IRelationMap rel-map
   ^RoaringBitmap matched-build-idxs]
  (->> (probe-outer-join-select probe-rel rel-map matched-build-idxs)
       (join-rels probe-rel rel-map)))

(defmethod probe-phase ::single-join
  [_join-type
   ^RelationReader probe-rel
   ^IRelationMap rel-map
   _matched-build-idxs]

  (let [rel-map-prober (.probeFromRelation rel-map probe-rel)
        matching-build-idxs (IntStream/builder)
        matching-probe-idxs (IntStream/builder)]

    (dotimes [probe-idx (.rowCount probe-rel)]
      (let [!matched (boolean-array 1)]
        (.forEachMatch rel-map-prober probe-idx
                       (reify IntConsumer
                         (accept [_ build-idx]
                           (if-not (aget !matched 0)
                             (do
                               (aset !matched 0 true)
                               (.add matching-build-idxs build-idx)
                               (.add matching-probe-idxs probe-idx))
                             (throw (err/runtime-err :xtdb.single-join/cardinality-violation
                                                     {::err/message "cardinality violation"}))))))
        (when-not (aget !matched 0)
          (.add matching-probe-idxs probe-idx)
          (.add matching-build-idxs emap/nil-row-idx))))

    (->> [(.toArray (.build matching-probe-idxs)) (.toArray (.build matching-build-idxs))]
         (join-rels probe-rel rel-map))))

(deftype JoinCursor [^BufferAllocator allocator, ^ICursor build-cursor,
                     ^:unsynchronized-mutable ^ICursor probe-cursor
                     ^IFn ->probe-cursor
                     ^IRelationMap rel-map
                     ^RoaringBitmap matched-build-idxs
                     pushdown-blooms
                     join-type]
  ICursor
  (tryAdvance [this c]
    (build-phase build-cursor rel-map pushdown-blooms)

    (boolean
     (or (let [advanced? (boolean-array 1)]
           (binding [scan/*column->pushdown-bloom* (cond-> scan/*column->pushdown-bloom*
                                                     (some? pushdown-blooms) (conj (zipmap (.probeKeyColumnNames rel-map) pushdown-blooms)))]
             (when-not probe-cursor
               (util/with-close-on-catch [probe-cursor (->probe-cursor)]
                 (set! (.probe-cursor this) probe-cursor)))

             (while (and (not (aget advanced? 0))
                         (.tryAdvance ^ICursor (.probe-cursor this)
                                      (reify Consumer
                                        (accept [_ probe-rel]
                                          (when (pos? (.rowCount ^RelationReader probe-rel))
                                            (with-open [out-rel (-> (probe-phase join-type probe-rel rel-map matched-build-idxs)
                                                                    (.copy allocator))]
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

(defn- emit-join-expr {:style/indent 2} [{:keys [condition left right]} {:keys [param-types] :as _args} f]
  (let [{left-col-types :col-types, ->left-cursor :->cursor} left
        {right-col-types :col-types, ->right-cursor :->cursor} right
        {equis :equi-condition, thetas :pred-expr} (group-by first condition)

        theta-expr (when-let [theta-exprs (seq (map second thetas))]
                     (list* 'and theta-exprs))

        equi-specs (->> (map last equis)
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
     :->cursor (fn [opts]
                 (let [->left-project-cursor #(project/->project-cursor opts (->left-cursor opts) left-projections)
                       ->right-project-cursor #(project/->project-cursor opts (->right-cursor opts) right-projections)

                       join-cursor (->cursor opts ->left-project-cursor ->right-project-cursor)]

                   (project/->project-cursor opts join-cursor project-away-specs)))}))

(defn- ->pushdown-blooms [key-col-names]
  (vec (repeatedly (count key-col-names) #(MutableRoaringBitmap.))))

(defn emit-join-expr-and-children {:style/indent 2} [join-expr args f]
  (emit-join-expr
    (emit-join-children join-expr args)
    args f))

(defn emit-inner-join-expr
  [join-expr {:keys [param-types] :as args}]
  (emit-join-expr join-expr args
                  (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
                    {:col-types (merge-with types/merge-col-types left-col-types right-col-types)
                     :->cursor (fn [{:keys [allocator params]} ->left-cursor ->right-cursor]
                                 (util/with-close-on-catch [left-cursor (->left-cursor)]
                                   (JoinCursor. allocator left-cursor nil ->right-cursor
                                                  (emap/->relation-map allocator {:build-col-types left-col-types
                                                                                  :build-key-col-names left-key-col-names
                                                                                  :probe-col-types right-col-types
                                                                                  :probe-key-col-names right-key-col-names
                                                                                  :store-full-build-rel? true
                                                                                  :theta-expr theta-expr
                                                                                  :param-types param-types
                                                                                  :params params})
                                                  nil (->pushdown-blooms right-key-col-names) ::inner-join)))})))
(defmethod lp/emit-expr :join [join-expr args]
  (emit-inner-join-expr (emit-join-children join-expr args) args))

(defmethod lp/emit-expr :left-outer-join [join-expr {:keys [param-types] :as args}]
  (emit-join-expr-and-children join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
      {:col-types (merge-with types/merge-col-types left-col-types (-> right-col-types types/with-nullable-cols))
       :->cursor (fn [{:keys [allocator params]}, ->left-cursor ->right-cursor]
                   (util/with-close-on-catch [right-cursor (->right-cursor)]
                     (JoinCursor. allocator right-cursor nil ->left-cursor
                                    (emap/->relation-map allocator {:build-col-types right-col-types
                                                                    :build-key-col-names right-key-col-names
                                                                    :probe-col-types left-col-types
                                                                    :probe-key-col-names left-key-col-names
                                                                    :store-full-build-rel? true
                                                                    :with-nil-row? true
                                                                    :theta-expr theta-expr
                                                                    :param-types param-types
                                                                    :params params})
                                    nil nil ::left-outer-join)))})))

(defmethod lp/emit-expr :full-outer-join [join-expr {:keys [param-types] :as args}]
  (emit-join-expr-and-children join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
      {:col-types (merge-with types/merge-col-types (-> left-col-types types/with-nullable-cols) (-> right-col-types types/with-nullable-cols))
       :->cursor (fn [{:keys [allocator params]} ->left-cursor ->right-cursor]
                   (util/with-close-on-catch [left-cursor (->left-cursor)]
                     (JoinCursor. allocator left-cursor nil ->right-cursor
                                    (emap/->relation-map allocator {:build-col-types left-col-types
                                                                    :build-key-col-names left-key-col-names
                                                                    :probe-col-types right-col-types
                                                                    :probe-key-col-names right-key-col-names
                                                                    :store-full-build-rel? true
                                                                    :with-nil-row? true
                                                                    :theta-expr theta-expr
                                                                    :param-types param-types
                                                                    :params params})
                                    (RoaringBitmap.) nil ::full-outer-join)))})))

(defmethod lp/emit-expr :semi-join [join-expr {:keys [param-types] :as args}]
  (emit-join-expr-and-children join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
      {:col-types left-col-types
       :->cursor (fn [{:keys [allocator params]} ->left-cursor ->right-cursor]
                   (util/with-close-on-catch [right-cursor (->right-cursor)]
                     (JoinCursor. allocator right-cursor nil ->left-cursor
                                    (emap/->relation-map allocator {:build-col-types right-col-types
                                                                    :build-key-col-names right-key-col-names
                                                                    :probe-col-types left-col-types
                                                                    :probe-key-col-names left-key-col-names
                                                                    :store-full-build-rel? true
                                                                    :theta-expr theta-expr
                                                                    :param-types param-types
                                                                    :params params})
                                    nil (->pushdown-blooms right-key-col-names) ::semi-join)))})))

(defmethod lp/emit-expr :anti-join [join-expr {:keys [param-types] :as args}]
  (emit-join-expr-and-children join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
      {:col-types left-col-types
       :->cursor (fn [{:keys [allocator params]} ->left-cursor ->right-cursor]
                   (util/with-close-on-catch [right-cursor (->right-cursor)]
                     (JoinCursor. allocator right-cursor nil ->left-cursor
                                    (emap/->relation-map allocator {:build-col-types right-col-types
                                                                    :build-key-col-names right-key-col-names
                                                                    :probe-col-types left-col-types
                                                                    :probe-key-col-names left-key-col-names
                                                                    :store-full-build-rel? true
                                                                    :theta-expr theta-expr
                                                                    :param-types param-types
                                                                    :params params})
                                    nil nil ::anti-semi-join)))})))

(defn- mark-join-probe-phase [^IRelationMap rel-map, ^RelationReader probe-rel, ^BitVector mark-col]
  (let [rel-prober (.probeFromRelation rel-map probe-rel)]
    (dotimes [idx (.rowCount probe-rel)]
      (let [match-res (.matches rel-prober idx)]
        (if (zero? match-res)
          (.setNull mark-col idx)
          (.set mark-col idx (case match-res 1 1, -1 0)))))))

(defmethod lp/emit-expr :mark-join [{:keys [mark-spec] :as join-expr} {:keys [param-types] :as args}]
  (let [[mark-col-name mark-condition] (first mark-spec)]
    (emit-join-expr-and-children (assoc join-expr :condition mark-condition) args
      (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
        {:col-types (assoc left-col-types mark-col-name [:union #{:bool :null}])

         :->cursor
         (fn [{:keys [^BufferAllocator allocator params]} ->probe-cursor ->build-cursor]
           (util/with-close-on-catch [build-cursor (->build-cursor)]
             (let [!probe-cursor (volatile! nil)
                   rel-map (emap/->relation-map allocator {:build-col-types right-col-types
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
                        (when-not @!probe-cursor
                          (util/with-close-on-catch [probe-cursor (->probe-cursor)]
                            (vreset! !probe-cursor probe-cursor)))
                        (while (and (not (aget advanced? 0))
                                    (.tryAdvance ^ICursor @!probe-cursor
                                                 (reify Consumer
                                                   (accept [_ probe-rel]
                                                     (let [^RelationReader probe-rel probe-rel
                                                           row-count (.rowCount probe-rel)]
                                                       (when (pos? row-count)
                                                         (aset advanced? 0 true)

                                                         (with-open [probe-rel (.copy probe-rel allocator)
                                                                     mark-col (doto (BitVector. (name mark-col-name) allocator)
                                                                                (.allocateNew row-count)
                                                                                (.setValueCount row-count))]
                                                           (mark-join-probe-phase rel-map probe-rel mark-col)
                                                           (let [out-cols (conj (seq probe-rel) (vr/vec->reader mark-col))]
                                                             (.accept c (vr/rel-reader out-cols row-count))))))))))))
                      (aget advanced? 0))))

                 (close [_]
                   (run! #(.clear ^MutableRoaringBitmap %) pushdown-blooms)
                   (util/try-close rel-map)
                   (util/try-close build-cursor)
                   (util/try-close @!probe-cursor))))))}))))

(defmethod lp/emit-expr :single-join [join-expr {:keys [param-types] :as args}]
  (emit-join-expr-and-children join-expr args
    (fn [{:keys [left-col-types right-col-types left-key-col-names right-key-col-names theta-expr]}]
      {:col-types (merge-with types/merge-col-types left-col-types (-> right-col-types types/with-nullable-cols))

       :->cursor (fn [{:keys [allocator params]} ->left-cursor ->right-cursor]
                   (util/with-close-on-catch [right-cursor (->right-cursor)]
                     (JoinCursor. allocator right-cursor nil ->left-cursor
                                    (emap/->relation-map allocator {:build-col-types right-col-types
                                                                    :build-key-col-names right-key-col-names
                                                                    :probe-col-types left-col-types
                                                                    :probe-key-col-names left-key-col-names
                                                                    :store-full-build-rel? true
                                                                    :with-nil-row? true
                                                                    :theta-expr theta-expr
                                                                    :param-types param-types
                                                                    :params params})
                                    nil nil ::single-join)))})))

(defn columns [relation]
  (set (keys (:col-types relation))))

(defn expr->columns [expr]
  (if (symbol? expr)
    (if (not (clojure.string/starts-with? (str expr) "?"))
      #{expr}
      #{})
    (set
      (walk/postwalk
        (fn [token]
          (if (seq? token)
            (mapcat
              (fn [child]
                (cond
                  (seq? child)
                  child

                  (and (symbol? child)
                       (not (clojure.string/starts-with? (str child) "?")))
                  [child]))
              (rest token))
            token))
        expr))))

(defn adjust-to-equi-condition
  "Swaps the sides of equi conditions to match location of cols in plan
  or rewrite simple equals predicate condition as equi condition"
  [{:keys [condition cols-from-current-rel other-cols] :as join-condition}]
  (if (= (first condition) :equi-condition)
    (let [equi-join-cond (last condition)
          lhs (first (keys equi-join-cond))
          rhs (first (vals equi-join-cond))
          lhs-cols (expr->columns lhs)]
      (if (= (:cols-from-current-rel join-condition) lhs-cols)
        condition
        [:equi-condition {rhs lhs}]))
    (let [predicate (last condition)]
      (if (lp/equals-predicate? predicate)
        (let [[_ a b] predicate]
          (cond (and (= cols-from-current-rel #{a})
                     (= other-cols #{b}))
                [:equi-condition {a b}]

                (and (= cols-from-current-rel #{b})
                     (= other-cols #{a}))
                [:equi-condition {b a}]

                :else
                condition))
        condition))))

(defn find-join-conditions-which-contain-cols-from-plan
  "Returns join conditions which reference at least one col from the current plan"
  [plan conditions]
  (filter
    (comp seq :cols-from-current-rel)
    (map
      (fn [condition]
        (let [cols-from-current-rel (set/intersection (columns plan) (:cols condition))]
          (assoc
            condition
            :cols-from-current-rel cols-from-current-rel
            :other-cols (set/difference (:cols condition) cols-from-current-rel))))
      conditions)))

(defn match-relations-to-potential-join-clauses
  "Attaches conditions to relations that satisfy the remaining columns not present in the existing plan"
  [rels conditions]
  (keep
    (fn [rel]
      (when-let [valid-join-conditions-for-rel
                 (->>
                   conditions
                   (map
                     (fn [condition]
                       (assoc
                         condition
                         :all-cols-present?
                         (-> condition
                             (:other-cols)
                             (set/difference (columns rel))
                             (empty?)))))
                   (filter #(-> % :all-cols-present?))
                   (not-empty))]
        (assoc rel :valid-join-conditions-for-rel valid-join-conditions-for-rel)))
    rels))

(defn remove-joined-relation [join-candidate rels]
  (remove #(= (:relation-id %) (:relation-id join-candidate)) rels))

(defn remove-used-join-conditions [join-candidate conditions]
  (remove
    #(contains?
       (set
         (map
           :condition-id
           (:valid-join-conditions-for-rel join-candidate)))
       (:condition-id %))
    conditions))

(defn build-plan-for-next-sub-graph [conditions relations args]
  (loop [plan (first relations)
         rels (rest relations)
         conditions conditions
         join-order [(:relation-id plan)]]
    (if (seq rels)
      (let [join-candidate (->> conditions
                                (find-join-conditions-which-contain-cols-from-plan plan)
                                (match-relations-to-potential-join-clauses rels)
                                (first))
            join-conditions (mapv
                              adjust-to-equi-condition
                              (:valid-join-conditions-for-rel join-candidate))]
        (if join-candidate
          (recur
            (emit-inner-join-expr
              {:condition join-conditions
               :left plan
               :right join-candidate}
              args)
            (remove-joined-relation join-candidate rels)
            (remove-used-join-conditions join-candidate conditions)
            (conj join-order (:relation-id join-candidate)))
          {:sub-graph-plan plan
           :sub-graph-unused-rels rels
           :sub-graph-unused-conditions conditions
           :sub-graph-join-order join-order}))
      {:sub-graph-plan plan
       :sub-graph-unused-rels rels
       :sub-graph-unused-conditions conditions
       :sub-graph-join-order join-order})))

(defn condition->cols [[condition-type condition]]
  (if (= condition-type :equi-condition)
    (let [[lhs rhs] (first condition)]
      (set/union
        (expr->columns rhs)
        (expr->columns lhs)))
    (expr->columns condition)))

(defmethod lp/emit-expr :mega-join [{:keys [conditions relations]} args]
  (let [conditions-with-cols (->> conditions
                                  (map (fn [condition]
                                         {:cols (condition->cols condition)
                                          :condition condition}))
                                  (map-indexed #(assoc %2 :condition-id %1)))
        child-relations (->> relations
                             (map #(lp/emit-expr % args))
                             (map-indexed #(assoc %2 :relation-id %1))
                             (sort-by
                               (juxt (comp nil? :row-count :stats)
                                     (comp :row-count :stats))))
        {:keys [sub-graph-plans unused-join-conditions join-order]}
        (loop [sub-graph-plans []
               relations child-relations
               conditions conditions-with-cols
               join-order []]
          (if (seq relations)
            (let [{:keys [sub-graph-plan
                          sub-graph-unused-rels
                          sub-graph-unused-conditions
                          sub-graph-join-order]}
                  (build-plan-for-next-sub-graph conditions relations args)]
              (recur
                (conj sub-graph-plans sub-graph-plan)
                sub-graph-unused-rels
                sub-graph-unused-conditions
                (conj join-order sub-graph-join-order)))
            {:sub-graph-plans sub-graph-plans
             :unused-join-conditions conditions
             :join-order join-order}))]
    ;; bit of a hack as currently mega-join may not choose a join order where
    ;; a condition like the one below is ever valid, but it should always be correct
    ;; to used the unused conditions as conditions for the outermost join
    (assoc
      (if (seq unused-join-conditions)
        (emit-inner-join-expr
          {:condition (mapv :condition unused-join-conditions)
           :left
           (reduce (fn [full-plan sub-graph-plan]
                     (emit-cross-join
                       {:left full-plan
                        :right sub-graph-plan})) (butlast sub-graph-plans))
           :right (last sub-graph-plans)}
          args)
        (reduce (fn [full-plan sub-graph-plan]
                  (emit-cross-join
                    {:left full-plan
                     :right sub-graph-plan})) sub-graph-plans))
      :join-order join-order)))
