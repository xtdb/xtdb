(ns xtdb.operator.group-by
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.expression.map :as emap]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.distinct :as distinct]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (java.io Closeable)
           (java.util ArrayList LinkedList List Spliterator)
           (java.util.stream IntStream IntStream$Builder)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb ICursor)
           (xtdb.arrow RelationReader Vector VectorReader VectorWriter)
           (xtdb.arrow.agg AggregateSpec AggregateSpec$Factory Average Count GroupMapper GroupMapper$Mapper GroupMapper$Null RowCount StdDevPop StdDevSamp Sum VariancePop VarianceSamp)
           (xtdb.expression.map RelationMapBuilder)
           xtdb.operator.distinct.DistinctRelationMap
           xtdb.types.LeastUpperBound))

(s/def ::aggregate-expr
  (s/or :nullary (s/cat :f simple-symbol?)
        :unary (s/cat :f simple-symbol?
                      :from-column ::lp/column)
        :binary (s/cat :f simple-symbol?
                       :left-column ::lp/column
                       :right-column ::lp/column)))

(s/def ::aggregate
  (s/map-of ::lp/column ::aggregate-expr :conform-keys true :count 1))

(s/def ::columns (s/coll-of (s/or :group-by ::lp/column :aggregate ::aggregate), :min-count 1))

(defmethod lp/ra-expr :group-by [_]
  (s/cat :op #{:γ :gamma :group-by}
         :opts (s/keys :req-un [::columns])
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defn ->group-mapper [^BufferAllocator allocator, group-fields]
  (if-let [group-col-names (not-empty (set (keys group-fields)))]
    (GroupMapper$Mapper. allocator
                         (distinct/->relation-map allocator {:build-fields group-fields
                                                             :build-key-col-names (vec group-col-names)
                                                             :nil-keys-equal? true}))
    (GroupMapper$Null. allocator)))

(defmulti ^AggregateSpec$Factory ->aggregate-factory
  #_{:clj-kondo/ignore [:unused-binding]}
  (fn [{:keys [f from-name from-field to-name zero-row?]}]
    (expr/normalise-fn-name f)))

(defmethod ->aggregate-factory :row_count [{:keys [to-name zero-row?]}]
  (RowCount. (str to-name) zero-row?))

(defmethod ->aggregate-factory :count [{:keys [from-name to-name zero-row?]}]
  (Count. (str from-name) (str to-name) zero-row?))

(def ^:private acc-sym (gensym 'acc))
(def ^:private group-idx-sym (gensym 'group_idx))
(def ^:private acc-local (gensym 'acc_local))
(def ^:private val-local (gensym 'val_local))

(def emit-agg
  (-> (fn [{:keys [to-type val-expr step-expr]} input-opts]
        (let [group-mapping-sym (gensym 'group-mapping)
              to-col-type (types/vec-type->col-type to-type)

              acc-expr {:op :variable, :variable acc-sym, :idx group-idx-sym
                        :extract-vec-from-rel? false}
              agg-expr (-> {:op :if-some, :local val-local, :expr val-expr
                            :then {:op :if-some, :local acc-local,
                                   :expr acc-expr
                                   :then step-expr
                                   :else {:op :local, :local val-local}}
                            :else acc-expr}
                           (expr/prepare-expr))

              {:keys [continue] :as emitted-expr} (expr/codegen-expr agg-expr input-opts)]

          {:eval-agg (-> `(fn [~(-> acc-sym (expr/with-tag Vector))
                               ~(-> expr/rel-sym (expr/with-tag RelationReader))
                               ~(-> group-mapping-sym (expr/with-tag VectorReader))]
                            (let [~@(expr/batch-bindings emitted-expr)]
                              (dotimes [~expr/idx-sym (.getRowCount ~expr/rel-sym)]
                                (let [~group-idx-sym (.getInt ~group-mapping-sym ~expr/idx-sym)]
                                  (.ensureCapacity ~acc-sym (inc ~group-idx-sym))

                                  ~(continue (fn [acc-type acc-code]
                                               (expr/set-value-code acc-type acc-sym group-idx-sym acc-code)))))))
                         #_(doto clojure.pprint/pprint) ;; <<no-commit>>
                         eval)}))
      (util/lru-memoize))) ;; <<no-commit>>

(defn- reducing-agg-factory [{:keys [to-name to-type zero-row?] :as agg-opts}]
  (let [to-type (types/->nullable-type to-type)
        agg-opts (assoc agg-opts :to-type to-type)]
    (reify AggregateSpec$Factory
      (getColName [_] (str to-name))
      (getType [_] to-type)

      (build [this al]
        (let [out-vec (Vector/open al (.getField this))]
          (reify
            AggregateSpec
            (aggregate [_ in-rel group-mapping]
              (let [input-opts {:var-types (->> in-rel
                                                (into {acc-sym to-type}
                                                      (map (juxt #(symbol (.getName ^VectorReader %))
                                                                 #(.getType ^VectorReader %)))))}
                    {:keys [eval-agg]} (emit-agg agg-opts input-opts)]
                (eval-agg out-vec in-rel group-mapping)))

            (openFinishedVector [_]
              (when (and zero-row? (zero? (.getValueCount out-vec)))
                (.writeNull out-vec))

              (.openSlice out-vec al))

            Closeable
            (close [_] (.close out-vec))))))))

(defmethod ->aggregate-factory :sum [{:keys [from-name from-field to-name zero-row?]}]
  (Sum. (str from-name) (str to-name) (Sum/outType (types/->type (or from-field :null))) zero-row?))

(defmethod ->aggregate-factory :avg [{:keys [from-name from-field to-name zero-row?]}]
  (Average. (str from-name) (types/->type (or from-field :null)) (str to-name) zero-row?))

(defmethod ->aggregate-factory :var_pop [{:keys [from-name to-name zero-row?]}]
  (VariancePop. (str from-name) (str to-name) zero-row?))

(defmethod ->aggregate-factory :var_samp [{:keys [from-name to-name zero-row?]}]
  (VarianceSamp. (str from-name) (str to-name) zero-row?))

(defmethod ->aggregate-factory :stddev_pop [{:keys [from-name to-name zero-row?]}]
  (StdDevPop. (str from-name) (str to-name) zero-row?))

(defmethod ->aggregate-factory :stddev_samp [{:keys [from-name to-name zero-row?]}]
  (StdDevSamp. (str from-name) (str to-name) zero-row?))

(defn- assert-supported-min-max-type [to-type]
  ;; TODO variable-width types - it's reasonable to want (e.g.) `(min <string-col>)`
  (let [to-col-type (types/vec-type->col-type to-type)
        to-col-type-head (types/col-type-head to-col-type)]
    (when-not (or (isa? types/col-type-hierarchy to-col-type-head :num)
                  (contains? #{:duration :date :timestamp-local :timestamp-tz :time-local :null}
                             to-col-type-head))
      (throw (err/unsupported :xtdb.group-by/unsupported-min-max-type
                              "Unsupported type in min/max aggregate"
                              {:unsupported-type to-col-type})))))

(defn- min-max-factory
  "compare-kw: update the accumulated value if `(compare-kw el acc)`"
  [compare-kw {:keys [from-name from-field] :as agg-opts}]

  (let [to-arrow-type (LeastUpperBound/of [(types/->type from-field)])
        to-type (or (some-> to-arrow-type types/->type)
                    (throw (err/incorrect :xtdb.group-by/incomparable-min-max-types
                                          "Incomparable types in min/max aggregate"
                                          {:from-field from-field})))]
    (assert-supported-min-max-type to-type)

    (reducing-agg-factory (into agg-opts
                                {:to-type to-type
                                 :val-expr {:op :call, :f :cast, :target-type (types/vec-type->col-type to-type)
                                            :args [{:op :variable, :variable from-name}]}
                                 :step-expr {:op :if,
                                             :pred {:op :call, :f compare-kw,
                                                    :args [{:op :local, :local val-local}
                                                           {:op :local, :local acc-local}]}
                                             :then {:op :local, :local val-local}
                                             :else {:op :local, :local acc-local}}}))))

(defmethod ->aggregate-factory :min [agg-opts] (min-max-factory :< agg-opts))
(defmethod ->aggregate-factory :min_all [agg-opts] (min-max-factory :< agg-opts))
(defmethod ->aggregate-factory :min_distinct [agg-opts] (min-max-factory :< agg-opts))
(defmethod ->aggregate-factory :max [agg-opts] (min-max-factory :> agg-opts))
(defmethod ->aggregate-factory :max_all [agg-opts] (min-max-factory :> agg-opts))
(defmethod ->aggregate-factory :max_distinct [agg-opts] (min-max-factory :> agg-opts))

(defn- wrap-distinct [^AggregateSpec$Factory agg-factory, from-name, ^Field from-field]
  (reify AggregateSpec$Factory
    (getColName [_] (.getColName agg-factory))
    (getType [_] (.getType agg-factory))

    (build [_ al]
      (let [agg-spec (.build agg-factory al)
            rel-maps (ArrayList.)]
        (reify
          AggregateSpec
          (aggregate [_ in-rel group-mapping]
            (let [in-vec (.vectorForOrNull in-rel (str from-name))
                  builders (ArrayList. (.size rel-maps))
                  distinct-idxs (IntStream/builder)]
              (dotimes [idx (.getValueCount in-vec)]
                (let [group-idx (.getInt group-mapping idx)]
                  (while (<= (.size rel-maps) group-idx)
                    (.add rel-maps (distinct/->relation-map al {:build-fields {from-name from-field}
                                                                :build-key-col-names [from-name]})))
                  (let [^DistinctRelationMap rel-map (nth rel-maps group-idx)]
                    (while (<= (.size builders) group-idx)
                      (.add builders nil))

                    (let [^RelationMapBuilder
                          builder (or (nth builders group-idx)
                                      (let [builder (.buildFromRelation rel-map (vr/rel-reader [in-vec]))]
                                        (.set builders group-idx builder)
                                        builder))]
                      (when (neg? (.addIfNotPresent builder idx))
                        (.add distinct-idxs idx))))))
              (let [distinct-idxs (.toArray (.build distinct-idxs))]
                (.aggregate agg-spec
                            (.select in-rel distinct-idxs)
                            (.select group-mapping distinct-idxs)))))

          (openFinishedVector [_] (.openFinishedVector agg-spec))

          Closeable
          (close [_]
            (util/close agg-spec)
            (util/close rel-maps)))))))

(defmethod ->aggregate-factory :count_distinct [{:keys [from-name from-field] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :count))
      (wrap-distinct from-name from-field)))

(defmethod ->aggregate-factory :count_all [agg-opts]
  (->aggregate-factory (assoc agg-opts :f :count)))

(defmethod ->aggregate-factory :sum_distinct [{:keys [from-name from-field] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :sum))
      (wrap-distinct from-name from-field)))

(defmethod ->aggregate-factory :sum_all [agg-opts]
  (->aggregate-factory (assoc agg-opts :f :sum)))

(defmethod ->aggregate-factory :avg_distinct [{:keys [from-name from-field] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :avg))
      (wrap-distinct from-name from-field)))

(defmethod ->aggregate-factory :avg_all [agg-opts]
  (->aggregate-factory (assoc agg-opts :f :avg)))

(deftype ArrayAggAggregateSpec [^BufferAllocator allocator
                                from-name ^Field to-field
                                ^VectorWriter acc-col
                                ^:unsynchronized-mutable ^long base-idx
                                ^List group-idxmaps
                                on-empty]
  AggregateSpec
  (aggregate [this in-rel group-mapping]
    (let [in-vec (.vectorForOrNull in-rel (str from-name))
          row-count (.getValueCount in-vec)]
      (.append acc-col in-vec)

      (dotimes [idx row-count]
        (let [group-idx (.getInt group-mapping idx)]
          (while (<= (.size group-idxmaps) group-idx)
            (.add group-idxmaps (IntStream/builder)))
          (.add ^IntStream$Builder (.get group-idxmaps group-idx)
                (+ base-idx idx))))

      (set! (.base-idx this) (+ base-idx row-count))))

  (openFinishedVector [_]
    (util/with-close-on-catch [out-vec (Vector/open allocator to-field)]
      (let [el-writer (.getListElements out-vec)
            row-copier (.rowCopier acc-col el-writer)]
        (doseq [^IntStream$Builder isb group-idxmaps]
          (.forEach (.build isb)
                    (fn [^long idx]
                      (.copyRow row-copier idx)))
          (.endList out-vec))

        (let [value-count (.size group-idxmaps)]
          (when (zero? value-count)
            (case on-empty
              :null (.writeNull out-vec)
              :empty-vec (.endList out-vec)
              nil)))

        out-vec)))

  Closeable
  (close [_]
    (util/close acc-col)))

(defmethod ->aggregate-factory :array_agg [{:keys [from-name from-field to-name zero-row?]}]
  (let [from-type (types/->type from-field)
        to-type (cond-> (types/->type [:list from-type])
                  zero-row? types/->nullable-type)]
    (reify AggregateSpec$Factory
      (getColName [_] (str to-name))
      (getType [_] to-type)

      (build [this al]
        (util/with-close-on-catch [acc-col (Vector/open al (types/->field from-type "$data$"))]
          (ArrayAggAggregateSpec. al from-name (.getField this) acc-col
                                  0 (ArrayList.) (if zero-row? :null :empty-rel)))))))

(defmethod ->aggregate-factory :array_agg_distinct [{:keys [from-name from-field] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :array_agg))
      (wrap-distinct from-name from-field)))

(defmethod ->aggregate-factory :vec_agg [{:keys [from-name from-field to-name zero-row?]}]
  (let [from-type (types/->type from-field)
        to-type (types/->type [:list from-type])]
    (reify AggregateSpec$Factory
      (getColName [_] (str to-name))
      (getType [_] to-type)

      (build [this al]
        (util/with-close-on-catch [acc-col (Vector/open al (types/->field from-type "$data$"))]
          (ArrayAggAggregateSpec. al from-name (.getField this) acc-col
                                  0 (ArrayList.) (if zero-row? :empty-vec :empty-rel)))))))

(defn- bool-agg-factory [step-f-kw {:keys [from-name] :as agg-opts}]
  (reducing-agg-factory (into agg-opts
                              {:to-type #xt/type :bool
                               :val-expr {:op :variable, :variable from-name}
                               :step-expr {:op :call, :f step-f-kw,
                                           :args [{:op :local, :local acc-local}
                                                  {:op :local, :local val-local}]}})))

(defmethod ->aggregate-factory :bool_and [agg-opts] (bool-agg-factory :and agg-opts))
(defmethod ->aggregate-factory :every [agg-opts] (->aggregate-factory (assoc agg-opts :f :bool_and)))
(defmethod ->aggregate-factory :bool_or [agg-opts] (bool-agg-factory :or agg-opts))

(deftype GroupByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^GroupMapper group-mapper
                        ^List aggregate-specs
                        ^:unsynchronized-mutable ^boolean done?]
  ICursor
  (getCursorType [_] "group-by")
  (getChildCursors [_] [in-cursor])

  (tryAdvance [this c]
    (boolean
     (when-not done?
       (set! (.done? this) true)

       (.forEachRemaining in-cursor
                          (fn [^RelationReader in-rel]
                            (let [group-mapping (.groupMapping group-mapper in-rel)]
                              (doseq [^AggregateSpec agg-spec aggregate-specs]
                                (.aggregate agg-spec in-rel group-mapping)))))

       (util/with-open [agg-cols (map #(.openFinishedVector ^AggregateSpec %) aggregate-specs)]
         (let [gm-rel (.finish group-mapper)
               ^RelationReader out-rel (vr/rel-reader (concat (.getVectors gm-rel) agg-cols)
                                                      (.getRowCount gm-rel))]
           (if (pos? (.getRowCount out-rel))
             (do
               (.accept c out-rel)
               true)
             false))))))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE))

  (close [_]
    (util/close aggregate-specs)
    (util/close in-cursor)
    (util/close group-mapper)))

(defmethod lp/emit-expr :group-by [{:keys [opts relation]} args]
  (let [{:keys [columns]} opts
        {group-cols :group-by, aggs :aggregate} (group-by first columns)
        group-cols (mapv second group-cols)]
    (lp/unary-expr (lp/emit-expr relation args)
      (fn [{:keys [vec-types], :as inner-rel}]
        (let [fields (into {} (map (fn [[k v]] [k (types/->field v k)])) vec-types)
              agg-factories (for [[_ agg] aggs]
                              (let [[to-column agg-form] (first agg)]
                                (->aggregate-factory (into {:to-name to-column
                                                            :zero-row? (empty? group-cols)}
                                                           (zmatch agg-form
                                                             [:nullary agg-opts]
                                                             (select-keys agg-opts [:f])

                                                             [:unary agg-opts]
                                                             (let [{:keys [f from-column]} agg-opts
                                                                   from-field (get fields from-column types/null-field)]
                                                               {:f f
                                                                :from-name from-column
                                                                :from-field from-field}))))))]
          (let [out-fields (into (->> group-cols
                                     (into {} (map (juxt identity fields))))
                                (->> agg-factories
                                     (into {} (map (comp (juxt #(symbol (.getName ^Field %)) identity)
                                                         #(.getField ^AggregateSpec$Factory %))))))]
            (let [out-vec-types (update-vals out-fields types/->type)]
              {:op :group-by
               :children [inner-rel]
               :explain {:group-by (mapv str group-cols)
                         :aggregates (->> aggs
                                          (mapv (fn [[_ agg]]
                                                  (let [[to-column agg-form] (first agg)]
                                                    [(str to-column) (pr-str agg-form)]))))}
               :vec-types out-vec-types
               :fields out-fields

             :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span]} in-cursor]
                         (cond-> (util/with-close-on-catch [agg-specs (LinkedList.)]
                                   (doseq [^AggregateSpec$Factory factory agg-factories]
                                     (.add agg-specs (.build factory allocator)))

                                   (GroupByCursor. allocator in-cursor
                                                   (->group-mapper allocator (select-keys fields group-cols))
                                                   (vec agg-specs)
                                                   false))
                           (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))})))))))
