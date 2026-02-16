(ns xtdb.operator.join
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [xtdb.expression :as expr]
            [xtdb.expression.map :as emap]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.project :as project]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (java.util ArrayList Iterator List Set SortedSet TreeSet)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb ICursor Bytes)
           (xtdb.arrow BitVector RelationReader VectorType)
           (xtdb.arrow.metadata MetadataFlavour MetadataFlavour$Bytes MetadataFlavour$Number MetadataFlavour$DateTime)
           (xtdb.bloom BloomUtils)
           (xtdb.operator ProjectionSpec)
           (xtdb.operator.join BuildSide ComparatorFactory DiskHashJoin JoinType MemoryHashJoin ProbeSide)))

(defmethod lp/ra-expr :cross-join [_]
  (s/cat :op #{:⨯ :cross-join}
         :opts map?
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(s/def ::join-equi-clause (s/map-of ::lp/expression ::lp/expression :conform-keys true :count 1))

(s/def ::join-condition-clause
  (s/or :equi-condition ::join-equi-clause
        :pred-expr ::lp/expression))

(s/def ::join-condition
  (s/coll-of ::join-condition-clause :kind vector?))

(s/def ::conditions ::join-condition)
(s/def ::mark-spec (s/map-of ::lp/column ::join-condition, :count 1, :conform-keys true))

(defmethod lp/ra-expr :join [_]
  (s/cat :op #{:⋈ :join}
         :opts (s/keys :req-un [::conditions])
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :left-outer-join [_]
  (s/cat :op #{:⟕ :left-outer-join}
         :opts (s/keys :req-un [::conditions])
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :full-outer-join [_]
  (s/cat :op #{:⟗ :full-outer-join}
         :opts (s/keys :req-un [::conditions])
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :semi-join [_]
  (s/cat :op #{:⋉ :semi-join}
         :opts (s/keys :req-un [::conditions])
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :anti-join [_]
  (s/cat :op #{:▷ :anti-join}
         :opts (s/keys :req-un [::conditions])
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :mark-join [_]
  (s/cat :op #{:mark-join}
         :opts (s/keys :req-un [::mark-spec])
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :single-join [_]
  (s/cat :op #{:single-join}
         :opts (s/keys :req-un [::conditions])
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :mega-join [_]
  (s/cat :op #{:mega-join}
         :opts (s/keys :req-un [::conditions])
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
  (getCursorType [_] "cross-join")
  (getChildCursors [_] [left-cursor right-cursor])

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
    (fn [{left-vec-types :vec-types :as left-rel} {right-vec-types :vec-types :as right-rel}]
      (let [out-vec-types (merge left-vec-types right-vec-types)]
        {:op :cross-join
         :children [left-rel right-rel]
         :vec-types out-vec-types
         :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span]} left-cursor right-cursor]
                     (cond-> (CrossJoinCursor. allocator left-cursor right-cursor (ArrayList.) nil nil)
                       (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))}))))

(defmethod lp/emit-expr :cross-join [join-expr args]
  (emit-cross-join (emit-join-children join-expr args)))

(defn- build-phase [^BuildSide build-side, ^ICursor build-cursor]
  (.forEachRemaining build-cursor
                     (fn [build-rel]
                       (.append build-side build-rel)))
  (.end build-side))

(defn- col-metadata-flavour
  "Returns the MetadataFlavour class for a VectorType, or nil if not applicable.
   Returns nil for unsupported types like Union/Poly."
  [^VectorType vec-type]
  (try
    (MetadataFlavour/getMetadataFlavour vec-type)
    (catch UnsupportedOperationException _
      nil)))

(defn- numeric-flavour?
  "Returns true if the flavour is a numeric type (Number or DateTime) that supports min/max."
  [flavour]
  (or (identical? flavour MetadataFlavour$Number)
      (identical? flavour MetadataFlavour$DateTime)))

(defn- bytes-flavour?
  "Returns true if the flavour is Bytes (strings, binary) that supports bloom filters."
  [flavour]
  (identical? flavour MetadataFlavour$Bytes))

(defn- compute-min-max
  "Computes min and max values for a numeric column. Returns {:min v :max v}."
  [col row-count]
  (when (pos? row-count)
    (loop [idx 0
           min-val Double/POSITIVE_INFINITY
           max-val Double/NEGATIVE_INFINITY]
      (if (< idx row-count)
        (if (.isNull col idx)
          (recur (inc idx) min-val max-val)
          (let [v (.getMetaDouble col idx)]
            (recur (inc idx)
                   (min min-val v)
                   (max max-val v))))
        (when (and (not= min-val Double/POSITIVE_INFINITY)
                   (not= max-val Double/NEGATIVE_INFINITY))
          {:min min-val :max max-val})))))

(def ^:private ^:const bloom-pushdown-threshold 1000)

(defn- iid-col?
  "Returns true if the column is an IID or UUID type suitable for direct value pushdown."
  [col-name ^VectorType col-type]
  (or (and (= col-type #xt/type :varbinary)
           (str/ends-with? (str col-name) "/_iid"))
      (= col-type #xt/type :uuid)))

(defn- collect-iids
  "Collects IID/UUID values from a column into a sorted set."
  [build-col row-count]
  (let [iid-set (TreeSet. Bytes/COMPARATOR)]
    (dotimes [idx row-count]
      (let [v (.getObject build-col idx)]
        (.add iid-set (cond-> v (uuid? v) util/uuid->bytes))))
    iid-set))

(defn- build-pushdowns-for-col
  "Builds pushdown info for a single column based on its type and flavour.
   Returns a map with any applicable keys: :min, :max, :bloom-hashes, :iids"
  [build-col col-name col-type row-count small-build?]
  (let [flavour (col-metadata-flavour col-type)]
    (cond-> {}
      ;; Track min/max for Numeric/DateTime flavours
      (numeric-flavour? flavour)
      (merge (compute-min-max build-col row-count))

      ;; Collect bloom hashes for Bytes flavour (only if < threshold rows)
      (and small-build? (bytes-flavour? flavour))
      (assoc :bloom-hashes
             (mapv #(BloomUtils/bloomHashes build-col %) (range row-count)))

      ;; Collect IID values for IID/UUID columns
      (iid-col? col-name col-type)
      (assoc :iids (collect-iids build-col row-count)))))

(defn- remap-pushdowns-to-probe-keys
  "Remaps pushdown info from build-side column names to probe-side column names.
   Both sides are paired by position in the equi-join conditions."
  [pushdowns build-key-col-names probe-key-col-names]
  (when (seq pushdowns)
    (let [build->probe (zipmap (map symbol build-key-col-names) probe-key-col-names)]
      (reduce-kv (fn [acc build-col col-pushdowns]
                   (if-let [probe-col (get build->probe build-col)]
                     (assoc acc probe-col col-pushdowns)
                     acc))
                 {}
                 pushdowns))))

(defn- build-pushdowns
  "Builds pushdown info map for all build-side key columns.
   Returns {col-name {:min v :max v :bloom-hashes [...] :iids TreeSet}} where applicable."
  [^BuildSide build-side, build-vec-types]
  (let [build-rel (.getDataRel build-side)
        row-count (.getRowCount build-rel)
        small-build? (< row-count bloom-pushdown-threshold)
        build-key-col-names (vec (.getKeyColNames build-side))]
    (reduce (fn [pushdowns col-name]
              (let [col-sym (symbol col-name)
                    col-type (get build-vec-types col-sym)
                    build-col (.vectorForOrNull build-rel col-name)]
                (if (and col-type build-col)
                  (let [col-pushdowns (build-pushdowns-for-col build-col col-name col-type row-count small-build?)]
                    (if (seq col-pushdowns)
                      (assoc pushdowns col-sym col-pushdowns)
                      pushdowns))
                  pushdowns)))
            {}
            build-key-col-names)))

(deftype JoinCursor [^BufferAllocator allocator,
                     ^BuildSide build-side, ^ICursor build-cursor,
                     build-vec-types probe-vec-types probe-key-cols ->probe-cursor
                     ^:unsynchronized-mutable ^ICursor hash-join-cursor
                     pushdowns?
                     ^ComparatorFactory cmp-factory, ^JoinType join-type]
  ICursor
  (getCursorType [_] (.getJoinTypeName join-type))
  (getChildCursors [_] (into [build-cursor] (.getChildCursors hash-join-cursor)))

  (tryAdvance [this c]
    (when-not hash-join-cursor
      (build-phase build-side build-cursor)
      (let [shuffle? (boolean (.getShuffle build-side))
            pushdowns (when (and pushdowns? (not shuffle?))
                        (-> (build-pushdowns build-side build-vec-types)
                            (remap-pushdowns-to-probe-keys (.getKeyColNames build-side) probe-key-cols)))]

        (util/with-close-on-catch [probe-cursor (->probe-cursor (when (seq pushdowns) pushdowns))]
          (set! (.hash-join-cursor this)
                (let [probe-vec-types (update-keys probe-vec-types str)]
                  (if shuffle?
                    (DiskHashJoin/open allocator build-side probe-cursor
                                       probe-vec-types (map str probe-key-cols)
                                       join-type cmp-factory)

                    (MemoryHashJoin. build-side probe-cursor
                                     (some-> probe-vec-types keys vec) (map str probe-key-cols)
                                     join-type cmp-factory)))))))

    (.tryAdvance hash-join-cursor c))

  (close [_]
    (util/try-close hash-join-cursor)
    (util/try-close build-side)
    (util/try-close build-cursor)))

(deftype MarkJoinCursor [^BufferAllocator allocator,
                         ^BuildSide build-side, ^ICursor build-cursor,
                         ->probe-cursor, probe-key-col-names, ^ComparatorFactory cmp-factory,
                         ^:unsynchronized-mutable ^ICursor probe-cursor
                         mark-col-name]
  ICursor
  (getCursorType [_] "mark-join")
  (getChildCursors [_] [build-cursor])

  (tryAdvance [this c]

    (when-not probe-cursor
      (build-phase build-side build-cursor)
      ;; Mark-join cannot use pushdowns because filtered pages would still need
      ;; to produce rows with m=false, but we don't know the row count of skipped pages
      (set! (.probe-cursor this) (->probe-cursor nil)))

    (boolean
     (let [advanced? (boolean-array 1)]
       (while (and (not (aget advanced? 0))
                   (.tryAdvance ^ICursor (.probe-cursor this)
                                (fn [^RelationReader probe-rel]
                                  (let [cmp (ComparatorFactory/build cmp-factory build-side probe-rel probe-key-col-names)
                                        ^ProbeSide probe-side (ProbeSide. build-side probe-rel probe-key-col-names cmp)
                                        row-count (.getRowCount probe-rel)]
                                    (when (pos? row-count)
                                      (aset advanced? 0 true)

                                      (with-open [mark-col (doto (BitVector. allocator (name mark-col-name) true)
                                                             (.ensureCapacity row-count))]
                                        (JoinType/mark probe-side mark-col)
                                        (let [out-cols (conj (seq probe-rel) mark-col)]
                                          (.accept c (vr/rel-reader out-cols row-count))))))))))
       (aget advanced? 0))))

  (close [_]
    (util/try-close build-side)
    (util/try-close build-cursor)
    (util/try-close probe-cursor)))

(defn- equi-spec [idx condition left-vec-types right-vec-types param-types]
  (let [[left-expr right-expr] (first condition)]
    (letfn [(equi-projection [side form vec-types]
              (if (symbol? form)
                {:key-col-name form}

                (let [col-name (symbol (format "?join-expr-%s-%d" (name side) idx))
                      input-types {:var-types vec-types
                                   :param-types param-types}]
                  {:key-col-name col-name
                   :projection (expr/->expression-projection-spec col-name (expr/form->expr form input-types)
                                                                  input-types)})))]

      {:left (equi-projection :left left-expr left-vec-types)
       :right (equi-projection :right right-expr right-vec-types)})))

(defn- projection-specs->vec-types [projection-specs]
  (->> projection-specs
       (into {} (map (juxt #(symbol (.getToName ^ProjectionSpec %))
                           #(.getType ^ProjectionSpec %))))))

(def ^:dynamic *disk-join-threshold-rows*
  (or (some-> (System/getenv "XTDB_JOIN_SPILL_THRESHOLD") parse-long)
      100000))

(defn ->build-side ^xtdb.operator.join.BuildSide [^BufferAllocator allocator,
                                                  {:keys [vec-types, key-col-names, track-unmatched-build-idxs?, with-nil-row?]}]
  (let [vec-types (->> vec-types
                       (into {} (map (fn [[col-name ^VectorType vec-type]]
                                       [(str col-name)
                                        (cond-> vec-type
                                          with-nil-row? (VectorType/maybe))]))))]
    (BuildSide. allocator vec-types (map str key-col-names)
                (boolean track-unmatched-build-idxs?)
                (boolean with-nil-row?)
                *disk-join-threshold-rows*)))

(defn ->cmp-factory [{:keys [build-vec-types probe-vec-types theta-expr param-types args with-nil-row?]}]
  (reify ComparatorFactory
    (buildEqui [_ build-col probe-col]
      (emap/->equi-comparator build-col probe-col args
                              {:nil-keys-equal? with-nil-row?
                               :param-types param-types}))

    (buildTheta [_ build-rel probe-rel]
      (when theta-expr
        (emap/->theta-comparator build-rel probe-rel theta-expr args
                                 {:build-vec-types build-vec-types
                                  :probe-vec-types probe-vec-types
                                  :param-types param-types})))))

(defn- emit-join-expr {:style/indent 2}
  [{:keys [condition left right]}
   {:keys [param-types]}
   {:keys [build-side merge-vec-types-fn join-type
           with-nil-row? track-unmatched-build-idxs? mark-col-name pushdowns?]}]
  (let [{left-vec-types :vec-types, ->left-cursor :->cursor} left
        {right-vec-types :vec-types, ->right-cursor :->cursor} right
        {equis :equi-condition, thetas :pred-expr} (group-by first condition)

        theta-expr (when-let [theta-exprs (seq (map second thetas))]
                     (list* 'and theta-exprs))

        equi-specs (->> (map last equis)
                        (map-indexed (fn [idx condition]
                                       (equi-spec idx condition left-vec-types right-vec-types param-types)))
                        vec)

        left-projections (vec (concat
                               (for [[col-name col-type] left-vec-types]
                                 (project/->identity-projection-spec col-name col-type))
                               (keep (comp :projection :left) equi-specs)))

        right-projections (vec (concat
                                (for [[col-name col-type] right-vec-types]
                                  (project/->identity-projection-spec col-name col-type))
                                (keep (comp :projection :right) equi-specs)))

        left-vec-types-proj (projection-specs->vec-types left-projections)
        right-vec-types-proj (projection-specs->vec-types right-projections)
        left-key-col-names (mapv (comp :key-col-name :left) equi-specs)
        right-key-col-names (mapv (comp :key-col-name :right) equi-specs)

        ->left-project-cursor (fn [opts] (project/->project-cursor opts (->left-cursor opts) left-projections))
        ->right-project-cursor (fn [opts] (project/->project-cursor opts (->right-cursor opts) right-projections))

        [build-vec-types build-key-col-names ->build-cursor
         probe-vec-types probe-key-col-names ->probe-cursor]
        (case build-side
          :left [left-vec-types-proj left-key-col-names ->left-project-cursor
                 right-vec-types-proj right-key-col-names ->right-project-cursor]
          :right [right-vec-types-proj right-key-col-names ->right-project-cursor
                  left-vec-types-proj left-key-col-names ->left-project-cursor])

        merged-vec-types (merge-vec-types-fn left-vec-types-proj right-vec-types-proj)
        output-projections (->> (set/difference (set (keys merged-vec-types))
                                                (into #{} (comp (mapcat (juxt :left :right))
                                                                (filter :projection)
                                                                (map :key-col-name))
                                                      equi-specs))
                                (mapv (fn [col-name]
                                        (let [col-type (get merged-vec-types col-name)]
                                          (project/->identity-projection-spec col-name col-type)))))]

    {:op (case join-type
           ::inner-join :inner-join
           ::left-outer-join :left-join
           ::left-outer-join-flipped :left-join
           ::full-outer-join :full-join
           ::semi-join :semi-join
           ::anti-semi-join :anti-join
           ::single-join :single-join
           ::mark-join :mark-join)

     :children [left right]
     :explain {:condition (pr-str (mapv second condition))}

     :vec-types (projection-specs->vec-types output-projections)

     :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span args] :as opts}]
                 (util/with-close-on-catch [build-cursor (->build-cursor opts)
                                            build-side (->build-side allocator {:vec-types build-vec-types
                                                                                :key-col-names build-key-col-names
                                                                                :with-nil-row? with-nil-row?
                                                                                :track-unmatched-build-idxs? track-unmatched-build-idxs?})]
                   (letfn [(->probe-cursor-with-pushdowns [our-pushdowns]
                             (->probe-cursor (cond-> opts
                                               our-pushdowns (assoc :pushdowns (merge (:pushdowns opts) our-pushdowns)))))]
                     (let [cmp-factory (->cmp-factory {:build-vec-types build-vec-types
                                                       :probe-vec-types probe-vec-types
                                                       :with-nil-row? with-nil-row?
                                                       :key-col-names probe-key-col-names
                                                       :theta-expr theta-expr
                                                       :param-types param-types
                                                       :args args})]
                       (project/->project-cursor opts
                                                 (cond-> (if (= join-type ::mark-join)
                                                           (MarkJoinCursor. allocator build-side build-cursor
                                                                            ->probe-cursor-with-pushdowns
                                                                            (map str probe-key-col-names) cmp-factory nil
                                                                            mark-col-name)
                                                           (JoinCursor. allocator build-side build-cursor
                                                                        build-vec-types probe-vec-types probe-key-col-names
                                                                        ->probe-cursor-with-pushdowns nil
                                                                        pushdowns? cmp-factory
                                                                        (case join-type
                                                                          ::inner-join JoinType/INNER
                                                                          ::left-outer-join JoinType/LEFT_OUTER
                                                                          ::left-outer-join-flipped JoinType/LEFT_OUTER_FLIPPED
                                                                          ::full-outer-join JoinType/FULL_OUTER
                                                                          ::semi-join JoinType/SEMI
                                                                          ::anti-semi-join JoinType/ANTI
                                                                          ::single-join JoinType/SINGLE)))
                                                   (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span))
                                                 output-projections)))))}))

(defn determine-build-side [left right default-side]
  (let [^long left-row-count (or (:row-count (:stats left)) Long/MAX_VALUE)
        ^long right-row-count (or (:row-count (:stats right)) Long/MAX_VALUE)]
    (cond
      (< left-row-count right-row-count) :left
      (< right-row-count left-row-count) :right
      :else default-side)))

(defn emit-inner-join-expr [join-expr args]
  (emit-join-expr join-expr args
                  {:build-side :left
                   :merge-vec-types-fn (fn [left-vec-types right-vec-types] (merge-with types/merge-types left-vec-types right-vec-types))
                   :join-type ::inner-join
                   :pushdowns? true}))

(defmethod lp/emit-expr :join [{:keys [opts] :as join-expr} args]
  (let [{:keys [conditions]} opts]
    (emit-inner-join-expr (-> (emit-join-children join-expr args)
                              (assoc :condition conditions))
                          args)))

(defmethod lp/emit-expr :left-outer-join [{:keys [opts] :as join-expr} args]
  (let [{:keys [conditions]} opts
        {:keys [left right] :as emitted-join-children} (emit-join-children join-expr args)
        emitted-join-children (assoc emitted-join-children :condition conditions)
        build-side (determine-build-side left right :right)]
    (emit-join-expr emitted-join-children
                    args
                    (if (= build-side :right)
                      {:build-side build-side
                       :merge-vec-types-fn (fn [left-vec-types right-vec-types] (merge-with types/merge-types left-vec-types (types/with-nullable-types right-vec-types)))
                       :join-type ::left-outer-join
                       :with-nil-row? true}
                      {:build-side build-side
                       :merge-vec-types-fn (fn [left-vec-types right-vec-types] (merge-with types/merge-types left-vec-types (types/with-nullable-types right-vec-types)))
                       :join-type ::left-outer-join-flipped
                       :track-unmatched-build-idxs? true
                       :pushdowns? true}))))

(defmethod lp/emit-expr :full-outer-join [{:keys [opts] :as join-expr} args]
  (let [{:keys [conditions]} opts]
    (emit-join-expr (-> (emit-join-children join-expr args)
                        (assoc :condition conditions))
                    args
                    {:build-side :left
                     :merge-vec-types-fn (fn [left-vec-types right-vec-types] (merge-with types/merge-types (types/with-nullable-types left-vec-types) (types/with-nullable-types right-vec-types)))
                     :join-type ::full-outer-join
                     :with-nil-row? true
                     :track-unmatched-build-idxs? true})))

(defmethod lp/emit-expr :semi-join [{:keys [opts] :as join-expr} args]
  (let [{:keys [conditions]} opts]
    (emit-join-expr (-> (emit-join-children join-expr args)
                        (assoc :condition conditions))
                    args
                    {:build-side :right
                     :merge-vec-types-fn (fn [left-vec-types _] left-vec-types)
                     :join-type ::semi-join
                     :pushdowns? true})))

(defmethod lp/emit-expr :anti-join [{:keys [opts] :as join-expr} args]
  (let [{:keys [conditions]} opts]
    (emit-join-expr (-> (emit-join-children join-expr args)
                        (assoc :condition conditions))
                    args
                    {:build-side :right
                     :merge-vec-types-fn (fn [left-vec-types _] left-vec-types)
                     :join-type ::anti-semi-join})))

(defmethod lp/emit-expr :mark-join [{:keys [opts] :as join-expr} args]
  (let [{:keys [mark-spec]} opts
        [mark-col-name mark-condition] (first mark-spec)]
    (emit-join-expr (-> (emit-join-children join-expr args)
                        (assoc :condition mark-condition))
                    args
                    {:build-side :right
                     :merge-vec-types-fn (fn [left-vec-types _] (assoc left-vec-types mark-col-name #xt/type [:? :bool]))
                     :mark-col-name mark-col-name
                     :join-type ::mark-join
                     :track-unmatched-build-idxs? true})))

(defmethod lp/emit-expr :single-join [{:keys [opts] :as join-expr} args]
  (let [{:keys [conditions]} opts]
    (emit-join-expr (-> (emit-join-children join-expr args)
                        (assoc :condition conditions))
                    args
                    {:build-side :right
                     :merge-vec-types-fn (fn [left-vec-types right-vec-types] (merge-with types/merge-types left-vec-types (types/with-nullable-types right-vec-types)))
                     :join-type ::single-join
                     :with-nil-row? true})))


(defn columns [relation]
  (set (keys (:vec-types relation))))

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

(defmethod lp/emit-expr :mega-join [{:keys [opts relations]} args]
  (let [{:keys [conditions]} opts
        conditions-with-cols (->> conditions
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
