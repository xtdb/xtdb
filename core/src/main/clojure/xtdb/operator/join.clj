(ns xtdb.operator.join
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [xtdb.expression :as expr]
            [xtdb.expression.map :as emap]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.project :as project]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.util ArrayList HashSet Iterator List Set)
           (org.apache.arrow.memory BufferAllocator)
           org.apache.arrow.vector.BitVector
           (org.apache.arrow.vector.types.pojo Field Schema)
           (org.roaringbitmap.buffer MutableRoaringBitmap)
           org.roaringbitmap.RoaringBitmap
           (xtdb ICursor)
           (xtdb.arrow RelationReader RelationWriter)
           (xtdb.bloom BloomUtils)
           (xtdb.operator ProjectionSpec)
           (xtdb.operator.join BuildSide JoinType JoinType$OuterJoinType ProbeSide ProbeSide$ComparatorFactory)))

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
      (update :right #(lp/emit-expr % args))))

(defn- cross-product ^xtdb.arrow.RelationReader [^RelationReader left-rel, ^RelationReader right-rel]
  (let [left-row-count (.getRowCount left-rel)
        right-row-count (.getRowCount right-rel)
        row-count (* left-row-count right-row-count)]
    (RelationReader/concatCols (.select left-rel
                                        (let [idxs (int-array row-count)]
                                          (dotimes [idx row-count]
                                            (aset idxs idx ^long (quot idx right-row-count)))
                                          idxs))

                               (.select right-rel
                                        (let [idxs (int-array row-count)]
                                          (dotimes [idx row-count]
                                            (aset idxs idx ^long (rem idx right-row-count)))
                                          idxs)))))

(deftype CrossJoinCursor [^BufferAllocator allocator
                          ^ICursor left-cursor
                          ^ICursor right-cursor
                          ^List left-rels
                          ^:unsynchronized-mutable ^Iterator left-rel-iterator
                          ^:unsynchronized-mutable ^RelationReader right-rel]
  ICursor
  (tryAdvance [this c]
    (.forEachRemaining left-cursor
                       (fn [^RelationReader left-rel]
                         (.add left-rels (.openSlice left-rel allocator))))

    (boolean
      (when-let [right-rel (or (when (and left-rel-iterator (.hasNext left-rel-iterator))
                                 right-rel)
                               (do
                                 (when right-rel
                                   (.close right-rel)
                                   (set! (.right-rel this) nil))
                                 (when (.tryAdvance right-cursor
                                                    (fn [^RelationReader right-rel]
                                                      (set! (.right-rel this) (.openSlice right-rel allocator))
                                                      (set! (.left-rel-iterator this) (.iterator left-rels))))
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
    (fn [left-fields right-fields]
      {:fields (merge left-fields right-fields)
       :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                   (CrossJoinCursor. allocator left-cursor right-cursor (ArrayList.) nil nil))})))

(defmethod lp/emit-expr :cross-join [join-expr args]
  (emit-cross-join (emit-join-children join-expr args)))

(defn- build-phase [^BuildSide build-side, ^ICursor build-cursor, pushdown-blooms, ^Set pushdown-iids]
  (.forEachRemaining build-cursor
                     (fn [^RelationReader build-rel]
                       (.append build-side build-rel)

                       (when pushdown-blooms
                         (let [build-key-col-names (vec (.getKeyColNames build-side))]
                           (dotimes [col-idx (count build-key-col-names)]
                             (let [build-col-name (nth build-key-col-names col-idx)
                                   build-col (.vectorForOrNull build-rel (str build-col-name))
                                   ^MutableRoaringBitmap pushdown-bloom (nth pushdown-blooms col-idx)]
                               (dotimes [build-idx (.getRowCount build-rel)]
                                 (when (and pushdown-iids (= (name build-col-name) "_iid"))
                                   (.add pushdown-iids (.getBytes build-col build-idx)))
                                 (.add pushdown-bloom ^ints (BloomUtils/bloomHashes build-col build-idx)))))))))
  (.build build-side))

(defn- join-rels [^JoinType join-type, ^RelationReader build-rel, ^RelationReader probe-rel, [^ints build-sel, ^ints probe-sel]]
  (let [selected-build-rel (.select build-rel build-sel)
        selected-probe-rel (.select probe-rel probe-sel)]
    (vr/rel-reader (if (= JoinType$OuterJoinType/LEFT_FLIPPED (.getOuterJoinType join-type))
                     (concat selected-probe-rel selected-build-rel)
                     (concat selected-build-rel selected-probe-rel)))))

(deftype JoinCursor [^BufferAllocator allocator,
                     ^BuildSide build-side, ^ICursor build-cursor,
                     probe-fields probe-key-cols
                     ->probe-cursor, ->probe-side
                     ^:unsynchronized-mutable ^ICursor probe-cursor
                     ^:unsynchronized-mutable ^RelationWriter nil-rel-writer
                     pushdown-blooms
                     ^Set pushdown-iids
                     ^JoinType join-type]
  ICursor
  (tryAdvance [this c]
    (when-not probe-cursor
      (build-phase build-side build-cursor pushdown-blooms pushdown-iids)

      (let [probe-iid-keys (filter #(= (name %) "_iid") probe-key-cols)]
        (set! (.probe-cursor this)
              (->probe-cursor (when pushdown-blooms
                                (zipmap (map symbol probe-key-cols) pushdown-blooms))
                              (when pushdown-iids
                                (zipmap probe-iid-keys (repeat pushdown-iids)))))))

    (when (and (.getMatchedBuildIdxs build-side) (not nil-rel-writer))
      (util/with-close-on-catch [nil-rel-writer (emap/->nillable-rel-writer allocator probe-fields)]
        (set! (.nil-rel-writer this) nil-rel-writer)))

    (boolean
     (or (let [advanced? (boolean-array 1)]
           (while (and (not (aget advanced? 0))
                       (.tryAdvance ^ICursor (.probe-cursor this)
                                    (fn [^RelationReader probe-rel]
                                      (when (pos? (.getRowCount probe-rel))
                                        (with-open [out-rel (-> (.probe join-type (->probe-side probe-rel))
                                                                (.openSlice allocator))]
                                          (when (pos? (.getRowCount out-rel))
                                            (aset advanced? 0 true)
                                            (.accept c out-rel))))))))
           (aget advanced? 0))

         (when-let [matched-build-idxs (.getMatchedBuildIdxs build-side)]
           (let [build-rel (.getBuiltRel build-side)
                 build-row-count (long (.getRowCount build-rel))
                 unmatched-build-idxs (RoaringBitmap/flip matched-build-idxs 0 build-row-count)]


             ;; Only need to remove the nil-row-idx for full outer joins
             (when (= (.getOuterJoinType join-type) JoinType$OuterJoinType/FULL)
               (.remove unmatched-build-idxs emap/nil-row-idx))

             (when-not (.isEmpty unmatched-build-idxs)
               ;; this means .isEmpty will be true on the next iteration (we flip the bitmap)
               (.add matched-build-idxs 0 build-row-count)

               (let [nil-rel (vw/rel-wtr->rdr nil-rel-writer)
                     build-sel (.toArray unmatched-build-idxs)
                     probe-sel (int-array (alength build-sel))]
                 (.accept c (join-rels join-type build-rel nil-rel [build-sel probe-sel]))
                 true)))))))

  (close [_]
    (run! #(.clear ^MutableRoaringBitmap %) pushdown-blooms)
    (util/try-close build-side)
    (util/try-close nil-rel-writer)
    (util/try-close build-cursor)
    (util/try-close probe-cursor)))

(deftype MarkJoinCursor [^BufferAllocator allocator,
                         ^BuildSide build-side, ^ICursor build-cursor,
                         ->probe-cursor, ->probe-side,
                         ^:unsynchronized-mutable ^ICursor probe-cursor
                         mark-col-name
                         pushdown-blooms]
  ICursor
  (tryAdvance [this c]

    (when-not probe-cursor
      (build-phase build-side build-cursor pushdown-blooms nil)
      (set! (.probe-cursor this)
            (->probe-cursor (zipmap (map symbol (.getKeyColNames build-side))
                                    pushdown-blooms)
                            nil)))

    (boolean
     (let [advanced? (boolean-array 1)]
       (while (and (not (aget advanced? 0))
                   (.tryAdvance ^ICursor (.probe-cursor this)
                                (fn [^RelationReader probe-rel]
                                  (let [^ProbeSide probe-side (->probe-side probe-rel)
                                        row-count (.getRowCount probe-rel)]
                                    (when (pos? row-count)
                                      (aset advanced? 0 true)

                                      (with-open [mark-col (doto (BitVector. (name mark-col-name) allocator)
                                                             (.allocateNew row-count)
                                                             (.setValueCount row-count))]
                                        (JoinType/mark probe-side mark-col)
                                        (let [out-cols (conj (seq probe-rel) (vr/vec->reader mark-col))]
                                          (.accept c (vr/rel-reader out-cols row-count))))))))))
       (aget advanced? 0))))

  (close [_]
    (run! #(.clear ^MutableRoaringBitmap %) pushdown-blooms)
    (util/try-close build-side)
    (util/try-close build-cursor)
    (util/try-close probe-cursor)))

(defn- equi-spec [idx condition left-fields right-fields param-fields]
  (let [[left-expr right-expr] (first condition)]
    (letfn [(equi-projection [side form fields]
              (if (symbol? form)
                {:key-col-name form}

                (let [col-name (symbol (format "?join-expr-%s-%d" (name side) idx))
                      input-types {:col-types (update-vals fields types/field->col-type)
                                   :param-types (update-vals param-fields types/field->col-type)}]
                  {:key-col-name col-name
                   :projection (expr/->expression-projection-spec col-name (expr/form->expr form input-types)
                                                                  input-types)})))]

      {:left (equi-projection :left left-expr left-fields)
       :right (equi-projection :right right-expr right-fields)})))

(defn- projection-specs->fields [projection-specs]
  (->> projection-specs
       (into {} (map (comp (juxt #(symbol (.getName ^Field %)) identity)
                           #(.getField ^ProjectionSpec %))))))

(defn- ->pushdown-blooms [key-col-names]
  (vec (repeatedly (count key-col-names) #(MutableRoaringBitmap.))))

(def ^:dynamic *disk-join-threshold* (* 100 1000))

(defn ->build-side ^xtdb.operator.join.BuildSide [^BufferAllocator allocator,
                                                  {:keys [fields, key-col-names, matched-build-idxs?, with-nil-row?]}]
  (let [schema (Schema. (->> fields
                             (mapv (fn [[field-name field]]
                                     (cond-> (-> field (types/field-with-name (str field-name)))
                                       with-nil-row? types/->nullable-field)))))]
    (BuildSide. allocator schema (map str key-col-names)
                (when matched-build-idxs? (RoaringBitmap.))
                (boolean with-nil-row?)
                *disk-join-threshold*)))

(defn ->probe-side [build-side {:keys [build-fields probe-fields key-col-names theta-expr param-fields args with-nil-row?]}]
  (let [param-types (update-vals param-fields types/field->col-type)]
    (fn ^xtdb.operator.join.ProbeSide [probe-rel]
      (ProbeSide. build-side probe-rel (map str key-col-names)
                  (reify ProbeSide$ComparatorFactory
                    (buildEqui [_ build-col probe-col]
                      (emap/->equi-comparator build-col probe-col args
                                              {:nil-keys-equal? with-nil-row?
                                               :param-types param-types}))

                    (buildTheta [_ build-rel probe-rel]
                      (when theta-expr
                        (emap/->theta-comparator build-rel probe-rel theta-expr args
                                                 {:nil-keys-equal? with-nil-row?
                                                  :build-fields build-fields
                                                  :probe-fields probe-fields
                                                  :param-types param-types}))))))))

(defn- emit-join-expr {:style/indent 2}
  [{:keys [condition left right]}
   {:keys [param-fields]}
   {:keys [build-side merge-fields-fn join-type
           with-nil-row? pushdown-blooms? matched-build-idxs? mark-col-name]}]
  (let [{left-fields :fields, ->left-cursor :->cursor} left
        {right-fields :fields, ->right-cursor :->cursor} right
        {equis :equi-condition, thetas :pred-expr} (group-by first condition)

        theta-expr (when-let [theta-exprs (seq (map second thetas))]
                     (list* 'and theta-exprs))

        equi-specs (->> (map last equis)
                        (map-indexed (fn [idx condition]
                                       (equi-spec idx condition left-fields right-fields param-fields)))
                        vec)

        left-projections (vec (concat
                               (for [[_col-name ^Field field] left-fields]
                                 (project/->identity-projection-spec field))
                               (keep (comp :projection :left) equi-specs)))

        right-projections (vec (concat
                                (for [[_col-name ^Field field] right-fields]
                                  (project/->identity-projection-spec field))
                                (keep (comp :projection :right) equi-specs)))

        left-fields-proj (projection-specs->fields left-projections)
        right-fields-proj (projection-specs->fields right-projections)
        left-key-col-names (mapv (comp :key-col-name :left) equi-specs)
        right-key-col-names (mapv (comp :key-col-name :right) equi-specs)

        ->left-project-cursor (fn [opts] (project/->project-cursor opts (->left-cursor opts) left-projections))
        ->right-project-cursor (fn [opts] (project/->project-cursor opts (->right-cursor opts) right-projections))

        [build-fields build-key-col-names ->build-cursor
         probe-fields probe-key-col-names ->probe-cursor]
        (case build-side
          :left [left-fields-proj left-key-col-names ->left-project-cursor
                 right-fields-proj right-key-col-names ->right-project-cursor]
          :right [right-fields-proj right-key-col-names ->right-project-cursor
                  left-fields-proj left-key-col-names ->left-project-cursor])

        merged-fields (merge-fields-fn left-fields-proj right-fields-proj)
        output-projections (->> (set/difference (set (keys merged-fields))
                                                (into #{} (comp (mapcat (juxt :left :right))
                                                                (filter :projection)
                                                                (map :key-col-name))
                                                      equi-specs))
                                (mapv #(project/->identity-projection-spec (get merged-fields %))))]

    {:fields (projection-specs->fields output-projections)
     :->cursor (fn [{:keys [allocator args] :as opts}]
                 (util/with-close-on-catch [build-cursor (->build-cursor opts)
                                            build-side (->build-side allocator {:fields build-fields
                                                                                :key-col-names build-key-col-names
                                                                                :with-nil-row? with-nil-row?
                                                                                :matched-build-idxs? matched-build-idxs?})]
                   (letfn [(->probe-cursor-with-pushdowns [our-pushdown-blooms our-pushdown-iids]
                             (->probe-cursor (cond-> opts
                                               our-pushdown-blooms (update :pushdown-blooms (fnil into {}) our-pushdown-blooms)
                                               our-pushdown-iids (update :pushdown-iids (fnil into {}) our-pushdown-iids))))]
                     (let [pushdown-blooms (when pushdown-blooms? (->pushdown-blooms probe-key-col-names))
                           pushdown-iids (HashSet.)
                           ->probe-side (->probe-side build-side
                                                      {:build-fields build-fields
                                                       :probe-fields probe-fields
                                                       :with-nil-row? with-nil-row?
                                                       :key-col-names probe-key-col-names
                                                       :theta-expr theta-expr
                                                       :param-fields param-fields
                                                       :args args})]
                       (project/->project-cursor opts
                                                 (if (= join-type ::mark-join)
                                                   (MarkJoinCursor. allocator build-side build-cursor
                                                                    ->probe-cursor-with-pushdowns ->probe-side nil
                                                                    mark-col-name pushdown-blooms)
                                                   (JoinCursor. allocator build-side build-cursor
                                                                probe-fields probe-key-col-names
                                                                ->probe-cursor-with-pushdowns ->probe-side nil
                                                                nil
                                                                pushdown-blooms pushdown-iids
                                                                (case join-type
                                                                  ::inner-join JoinType/INNER
                                                                  ::left-outer-join JoinType/LEFT_OUTER
                                                                  ::left-outer-join-flipped JoinType/LEFT_OUTER_FLIPPED
                                                                  ::full-outer-join JoinType/FULL_OUTER
                                                                  ::semi-join JoinType/SEMI
                                                                  ::anti-semi-join JoinType/ANTI
                                                                  ::single-join JoinType/SINGLE)))
                                                 output-projections)))))}))

(defn emit-join-expr-and-children {:style/indent 2} [join-expr args join-impl]
  (emit-join-expr (emit-join-children join-expr args) args join-impl))

(defn emit-inner-join-expr [join-expr args]
  (emit-join-expr join-expr args
                  {:build-side :left
                   :merge-fields-fn (fn [left-fields right-fields] (merge-with types/merge-fields left-fields right-fields))
                   :join-type ::inner-join
                   :pushdown-blooms? true}))

(defmethod lp/emit-expr :join [join-expr args]
  (emit-inner-join-expr (emit-join-children join-expr args) args))

(defn determine-build-side [left right default-side]
  (let [^long left-row-count (or (:row-count (:stats left)) Long/MAX_VALUE)
        ^long right-row-count (or (:row-count (:stats right)) Long/MAX_VALUE)]
    (cond
      (< left-row-count right-row-count) :left
      (< right-row-count left-row-count) :right
      :else default-side)))

(defmethod lp/emit-expr :left-outer-join [join-expr args]
  (let [{:keys [left right] :as emitted-join-children} (emit-join-children join-expr args)
        build-side (determine-build-side left right :right)]
    (emit-join-expr emitted-join-children
                    args
                    (if (= build-side :right)
                      {:build-side build-side
                       :merge-fields-fn (fn [left-fields right-fields] (merge-with types/merge-fields left-fields (types/with-nullable-fields right-fields)))
                       :join-type ::left-outer-join
                       :with-nil-row? true}
                      {:build-side build-side
                       :merge-fields-fn (fn [left-fields right-fields] (merge-with types/merge-fields left-fields (types/with-nullable-fields right-fields)))
                       :join-type ::left-outer-join-flipped
                       :matched-build-idxs? true
                       :pushdown-blooms? true}))))

(defmethod lp/emit-expr :full-outer-join [join-expr args]
  (emit-join-expr-and-children join-expr args
                               {:build-side :left
                                :merge-fields-fn (fn [left-fields right-fields] (merge-with types/merge-fields (types/with-nullable-fields left-fields) (types/with-nullable-fields right-fields)))
                                :join-type ::full-outer-join
                                :with-nil-row? true
                                :matched-build-idxs? true}))

(defmethod lp/emit-expr :semi-join [join-expr args]
  (emit-join-expr-and-children join-expr args
                               {:build-side :right
                                :merge-fields-fn (fn [left-fields _] left-fields)
                                :join-type ::semi-join
                                :pushdown-blooms? true}))

(defmethod lp/emit-expr :anti-join [join-expr args]
  (emit-join-expr-and-children join-expr args
                               {:build-side :right
                                :merge-fields-fn (fn [left-fields _] left-fields)
                                :join-type ::anti-semi-join}))

(defmethod lp/emit-expr :mark-join [{:keys [mark-spec] :as join-expr} args]
  (let [[mark-col-name mark-condition] (first mark-spec)]
    (emit-join-expr-and-children (assoc join-expr :condition mark-condition) args
                                 {:build-side :right
                                  :merge-fields-fn (fn [left-fields _] (assoc left-fields mark-col-name (types/col-type->field mark-col-name [:union #{:null :bool}])))
                                  :mark-col-name mark-col-name
                                  :join-type ::mark-join
                                  :matched-build-idxs? true
                                  :pushdown-blooms? true})))

(defmethod lp/emit-expr :single-join [join-expr args]
  (emit-join-expr-and-children join-expr args
                               {:build-side :right
                                :merge-fields-fn (fn [left-fields right-fields] (merge-with types/merge-fields left-fields (types/with-nullable-fields right-fields)))
                                :join-type ::single-join
                                :with-nil-row? true}))


(defn columns [relation]
  (set (keys (:fields relation))))

(defn expr->columns [expr]
  (-> (if (symbol? expr)
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
          expr)))

      (disj 'xtdb/end-of-time)))

(defn adjust-to-equi-condition
  "Swaps the sides of equi conditions to match location of cols in plan
  or rewrite simple equals predicate condition as equi condition"
  [{:keys [condition cols-from-current-rel other-cols] :as join-condition}]
  (cond
    (= nil cols-from-current-rel other-cols)
    condition
    (= (first condition) :equi-condition)
    (let [equi-join-cond (last condition)
          lhs (first (keys equi-join-cond))
          rhs (first (vals equi-join-cond))
          lhs-cols (expr->columns lhs)]
      (if (= (:cols-from-current-rel join-condition) lhs-cols)
        condition
        [:equi-condition {rhs lhs}]))
    :else
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

(defn remove-used-join-conditions [conditions-to-remove conditions]
  (let [condition-ids-to-remove (set (map :condition-id conditions-to-remove))]
    (remove #(contains? condition-ids-to-remove (:condition-id %)) conditions)))

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
            joining-join-conditions (:valid-join-conditions-for-rel join-candidate)
            post-join-cols (set/union (columns plan) (columns join-candidate))
            extra-join-conditions
            ;;these are conditions that don't reference cols from multiple rels
            ;;ideally these should have been pushed into one of the child rels
            ;;by lp rewrite rules, but mega-join should still be able to handle them
            ;;we need to do this here, as if we only create a single sub-graph this
            ;;may be the only join that takes place.
            (filter #(set/superset? post-join-cols (:cols %))
                    (remove-used-join-conditions joining-join-conditions conditions))
            join-conditions (concat joining-join-conditions extra-join-conditions)
            adjusted-join-conditions (mapv adjust-to-equi-condition join-conditions)]

        (if join-candidate
          (recur
           (emit-inner-join-expr
            {:condition adjusted-join-conditions
             :left plan
             :right join-candidate}
            args)
           (remove-joined-relation join-candidate rels)
           (remove-used-join-conditions join-conditions conditions)
           (conj join-order adjusted-join-conditions (:relation-id join-candidate)))
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

(defn add-unused-join-conditions-to-join-order [join-order unused-join-conditions]
  (conj
   (vec (butlast join-order))
   (map :condition unused-join-conditions)
   (last join-order)))

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
    (if (seq unused-join-conditions)
      ;; if there are unused join conditions that means there must be at least 2
      ;; disconnected sub graphs as all other conditions can and
      ;; should be used already in the sub-graph joins.
      (assoc
       (emit-inner-join-expr
        {:condition (mapv :condition unused-join-conditions)
         :left
         (reduce (fn [full-plan sub-graph-plan]
                   (emit-cross-join
                    {:left full-plan
                     :right sub-graph-plan})) (butlast sub-graph-plans))
         :right (last sub-graph-plans)}
        args)
       :join-order (add-unused-join-conditions-to-join-order join-order unused-join-conditions))
      (assoc
       (reduce (fn [full-plan sub-graph-plan]
                 (emit-cross-join
                  {:left full-plan
                   :right sub-graph-plan})) sub-graph-plans)
       :join-order join-order))))
