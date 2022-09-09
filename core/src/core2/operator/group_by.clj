(ns core2.operator.group-by
  (:require [clojure.spec.alpha :as s]
            [core2.expression :as expr]
            [core2.expression.map :as emap]
            [core2.logical-plan :as lp]
            [core2.rewrite :refer [zmatch]]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector :as vec]
            [core2.vector.writer :as vw]
            [core2.error :as err])
  (:import (core2 ICursor)
           (core2.expression.map IRelationMap IRelationMapBuilder)
           (core2.vector IIndirectRelation IIndirectVector IVectorWriter)
           (java.io Closeable)
           (java.util ArrayList LinkedList List Spliterator)
           (java.util.function Consumer IntConsumer)
           (java.util.stream IntStream IntStream$Builder)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector Float8Vector IntVector NullVector ValueVector)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector)))

(s/def ::aggregate-expr
  (s/or :nullary (s/cat :f simple-symbol?)
        :unary (s/cat :f simple-symbol?
                      :from-column ::lp/column)
        :binary (s/cat :f simple-symbol?
                       :left-column ::lp/column
                       :right-column ::lp/column)))

(s/def ::aggregate
  (s/map-of ::lp/column ::aggregate-expr :conform-keys true :count 1))

(defmethod lp/ra-expr :group-by [_]
  (s/cat :op #{:γ :gamma :group-by}
         :columns (s/coll-of (s/or :group-by ::lp/column :aggregate ::aggregate), :min-count 1)
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(definterface IGroupMapper
  (^org.apache.arrow.vector.IntVector groupMapping [^core2.vector.IIndirectRelation inRelation])
  (^java.util.List #_<IIndirectVector> finish []))

(deftype NullGroupMapper [^IntVector group-mapping]
  IGroupMapper
  (groupMapping [_ in-rel]
    (.clear group-mapping)
    (let [row-count (.rowCount in-rel)]
      (.setValueCount group-mapping row-count)
      (dotimes [idx row-count]
        (.set group-mapping idx 0))
      group-mapping))

  (finish [_] [])

  Closeable
  (close [_]
    (.close group-mapping)))

(deftype GroupMapper [^List group-col-names
                      ^IRelationMap rel-map
                      ^IntVector group-mapping]
  IGroupMapper
  (groupMapping [_ in-rel]
    (.clear group-mapping)
    (.setValueCount group-mapping (.rowCount in-rel))

    (let [builder (.buildFromRelation rel-map in-rel)]
      (dotimes [idx (.rowCount in-rel)]
        (.set group-mapping idx (emap/inserted-idx (.addIfNotPresent builder idx)))))

    group-mapping)

  (finish [_]
    (seq (.getBuiltRelation rel-map)))

  Closeable
  (close [_]
    (.close group-mapping)
    (util/try-close rel-map)))

(defn- ->group-mapper [^BufferAllocator allocator, group-col-types]
  (let [gm-vec (IntVector. "group-mapping" allocator)]
    (if-let [group-col-names (not-empty (set (keys group-col-types)))]
      (GroupMapper. group-col-names
                    (emap/->relation-map allocator {:build-col-types group-col-types
                                                    :build-key-col-names (vec group-col-names)
                                                    :nil-keys-equal? true})
                    gm-vec)
      (NullGroupMapper. gm-vec))))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IAggregateSpec
  (^void aggregate [^core2.vector.IIndirectRelation inRelation,
                    ^org.apache.arrow.vector.IntVector groupMapping])
  (^core2.vector.IIndirectVector finish []))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IAggregateSpecFactory
  (^clojure.lang.Symbol getToColumnName [])
  (getToColumnType [])
  (^core2.operator.group_by.IAggregateSpec build [^org.apache.arrow.memory.BufferAllocator allocator]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti ^core2.operator.group_by.IAggregateSpecFactory ->aggregate-factory
  (fn [{:keys [f from-name from-type to-name zero-row?]}]
    (keyword (name f))))

(defmethod ->aggregate-factory :count-star [{:keys [to-name zero-row?]}]
  (reify IAggregateSpecFactory
    (getToColumnName [_] to-name)
    (getToColumnType [_] :i64)

    (build [_ al]
      (let [^BigIntVector out-vec (-> (types/col-type->field to-name :i64)
                                      (.createVector al))]
        (reify
          IAggregateSpec
          (aggregate [_ in-rel group-mapping]
            (dotimes [idx (.rowCount in-rel)]
              (let [group-idx (.get group-mapping idx)]
                (when (<= (.getValueCount out-vec) group-idx)
                  (.setValueCount out-vec (inc group-idx))
                  (.set out-vec group-idx 0))

                (.set out-vec group-idx (inc (.get out-vec group-idx))))))

          (finish [_]
            (when (and zero-row? (zero? (.getValueCount out-vec)))
              (.setValueCount out-vec 1)
              (.set out-vec 0 0))
            (iv/->direct-vec out-vec))

          Closeable
          (close [_] (.close out-vec)))))))

(defmethod ->aggregate-factory :count [{:keys [from-name to-name zero-row?]}]
  (reify IAggregateSpecFactory
    (getToColumnName [_] to-name)
    (getToColumnType [_] :i64)

    (build [_ al]
      (let [^BigIntVector out-vec (-> (types/col-type->field to-name :i64)
                                      (.createVector al))]
        (reify
          IAggregateSpec
          (aggregate [_ in-rel group-mapping]
            (let [in-col (.vectorForName in-rel (name from-name))
                  in-vec (.getVector in-col)]
              (dotimes [idx (.rowCount in-rel)]
                (let [group-idx (.get group-mapping idx)]
                  (when (<= (.getValueCount out-vec) group-idx)
                    (.setValueCount out-vec (inc group-idx))
                    (.set out-vec group-idx 0))

                  (let [inner-idx (.getIndex in-col idx)]
                    ;; TODO this logic should probably belong elsewhere...
                    (when-not (if (instance? DenseUnionVector in-vec)
                                (let [^DenseUnionVector in-vec in-vec
                                      type-id (.getTypeId in-vec inner-idx)]
                                  (.isNull (.getVectorByType in-vec type-id)
                                           (.getOffset in-vec inner-idx)))
                                (.isNull in-vec inner-idx))
                      (.set out-vec group-idx (inc (.get out-vec group-idx)))))))))

          (finish [_]
            (when (and zero-row? (zero? (.getValueCount out-vec)))
              (.setValueCount out-vec 1)
              (.set out-vec 0 0))
            (iv/->direct-vec out-vec))

          Closeable
          (close [_] (.close out-vec)))))))

(def ^:private acc-sym (gensym 'acc))
(def ^:private acc-col-sym (gensym 'acc-col))
(def ^:private group-idx-sym (gensym 'group-idx))
(def ^:private acc-local (gensym 'acc-local))
(def ^:private val-local (gensym 'val-local))
(def ^:private acc-writer-sym (gensym 'acc-writer))

(def emit-agg
  (-> (fn [{:keys [to-type val-expr step-expr]} input-opts]
        (let [group-mapping-sym (gensym 'group-mapping)

              return-type [:union (conj #{:null} to-type)]
              ^List ordered-col-types (vec (second return-type))

              acc-expr {:op :variable, :variable acc-col-sym, :idx group-idx-sym
                        :extract-vec-from-rel? false}
              agg-expr (-> {:op :if-some, :local val-local, :expr val-expr
                            :then {:op :if-some, :local acc-local,
                                   :expr acc-expr
                                   :then step-expr
                                   :else {:op :local, :local val-local}}
                            :else acc-expr}
                           (expr/prepare-expr))

              {:keys [continue] :as emitted-expr} (expr/codegen-expr agg-expr input-opts)
              ;; ignore return-type of the codegen because it may be more specific than the acc type

              vec-type (-> (.getType (types/col-type->field return-type))
                           (types/arrow-type->vector-type))]

          {:return-type return-type
           :eval-agg (-> `(fn [~(-> acc-sym (expr/with-tag vec-type))
                               ~(-> expr/rel-sym (expr/with-tag IIndirectRelation))
                               ~(-> group-mapping-sym (expr/with-tag IntVector))]
                            (let [~acc-col-sym (iv/->direct-vec ~acc-sym)
                                  ~acc-writer-sym (vec/->poly-writer ~acc-sym ~ordered-col-types)
                                  ~@(expr/batch-bindings emitted-expr)]
                              (dotimes [~expr/idx-sym (.rowCount ~expr/rel-sym)]
                                (let [~group-idx-sym (.get ~group-mapping-sym ~expr/idx-sym)]
                                  (when (<= (.getValueCount ~acc-sym) ~group-idx-sym)
                                    (.setValueCount ~acc-sym (inc ~group-idx-sym)))

                                  ~(continue (fn [acc-type acc-code]
                                               `(do
                                                  (.setPosition (.writerPosition ~acc-writer-sym) ~group-idx-sym)
                                                  ~(expr/write-value-code acc-type acc-writer-sym
                                                                          (.indexOf ordered-col-types acc-type)
                                                                          acc-code))))))))
                         #_(doto clojure.pprint/pprint)
                         eval)}))
      (util/lru-memoize)))

(defn- reducing-agg-factory [{:keys [to-name to-type zero-row?] :as agg-opts}]
  (let [to-type [:union (conj #{:null} to-type)]
        to-field (types/col-type->field to-name to-type)]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnType [_] to-type)

      (build [_ al]
        (let [^ValueVector out-vec (.createVector to-field al)]
          (reify
            IAggregateSpec
            (aggregate [_ in-rel group-mapping]
              (let [input-opts {:var->col-type (->> (seq in-rel)
                                                    (into {acc-col-sym to-type}
                                                          (map (juxt #(symbol (.getName ^IIndirectVector %))
                                                                     #(-> (.getVector ^IIndirectVector %) .getField types/field->col-type)))))}
                    {:keys [eval-agg]} (emit-agg agg-opts input-opts)]
                (eval-agg out-vec in-rel group-mapping)))

            (finish [_]
              (when (and zero-row? (zero? (.getValueCount out-vec)))
                (.setValueCount out-vec 1))

              (iv/->direct-vec out-vec))

            Closeable
            (close [_] (.close out-vec))))))))

(defmethod ->aggregate-factory :sum [{:keys [from-name from-type] :as agg-opts}]
  (let [to-type (->> (types/flatten-union-types from-type)
                     ;; TODO handle non-num types, if appropriate? (durations?)
                     ;; do we want to runtime error, or treat them as nulls?
                     (filter (comp #(isa? types/col-type-hierarchy % :num) types/col-type-head))
                     (types/least-upper-bound))]
    (reducing-agg-factory (into agg-opts
                                {:to-type to-type
                                 :val-expr {:op :call, :f :cast, :target-type to-type
                                            :args [{:op :variable, :variable from-name}]}
                                 :step-expr {:op :call, :f :+,
                                             :args [{:op :local, :local acc-local}
                                                    {:op :local, :local val-local}]}}))))

(defmethod ->aggregate-factory :avg [{:keys [from-name from-type to-name zero-row?]}]
  (let [sum-agg (->aggregate-factory {:f :sum, :from-name from-name, :from-type from-type,
                                      :to-name 'sum, :zero-row? zero-row?})
        count-agg (->aggregate-factory {:f :count, :from-name from-name, :from-type from-type,
                                        :to-name 'cnt, :zero-row? zero-row?})
        projecter (expr/->expression-projection-spec to-name '(/ (double sum) cnt)
                                                     {:col-types {'sum (.getToColumnType sum-agg)
                                                                  'cnt (.getToColumnType count-agg)}})]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnType [_] (.getColumnType projecter))

      (build [_ al]
        (let [sum-agg (.build sum-agg al)
              count-agg (.build count-agg al)
              res-vec (Float8Vector. (name to-name) al)]
          (reify
            IAggregateSpec
            (aggregate [_ in-rel group-mapping]
              (.aggregate sum-agg in-rel group-mapping)
              (.aggregate count-agg in-rel group-mapping))

            (finish [_]
              (let [sum-ivec (.finish sum-agg)
                    count-ivec (.finish count-agg)
                    out-vec (.project projecter al (iv/->indirect-rel [sum-ivec count-ivec]) {})]
                (if (instance? NullVector (.getVector out-vec))
                  out-vec
                  (do
                    (doto (.makeTransferPair (.getVector out-vec) res-vec)
                      (.transfer))
                    (iv/->direct-vec res-vec)))))

            Closeable
            (close [_]
              (util/try-close res-vec)
              (util/try-close sum-agg)
              (util/try-close count-agg))))))))

(defn- ->variance-agg-factory [variance-op {:keys [from-name from-type to-name zero-row?]}]
  (let [countx-agg (->aggregate-factory {:f :count, :from-name from-name, :from-type from-type
                                         :to-name 'countx, :zero-row? zero-row?})

        sumx-agg (->aggregate-factory {:f :sum, :from-name from-name, :from-type from-type
                                       :to-name 'sumx, :zero-row? zero-row?})

        x2-projecter (expr/->expression-projection-spec 'x2 (list '* from-name from-name)
                                                        {:col-types {from-name from-type}})

        sumx2-agg (->aggregate-factory {:f :sum, :from-name 'x2, :from-type (.getColumnType x2-projecter)
                                        :to-name 'sumx2, :zero-row? zero-row?})

        finish-projecter (expr/->expression-projection-spec to-name (case variance-op
                                                                      :var_pop '(if (> countx 0)
                                                                                  (/ (- sumx2
                                                                                        (/ (* sumx sumx) (double countx)))
                                                                                     (double countx))
                                                                                  nil)
                                                                      :var_samp '(if (> countx 1)
                                                                                   (/ (- sumx2
                                                                                         (/ (* sumx sumx) (double countx)))
                                                                                      (double (- countx 1)))
                                                                                   nil))
                                                            {:col-types {'sumx (.getToColumnType sumx-agg)
                                                                         'sumx2 (.getToColumnType sumx2-agg)
                                                                         'countx (.getToColumnType countx-agg)}})]

    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnType [_] (.getColumnType finish-projecter))

      (build [_ al]
        (let [sumx-agg (.build sumx-agg al)
              sumx2-agg (.build sumx2-agg al)
              countx-agg (.build countx-agg al)
              res-vec (Float8Vector. (name to-name) al)]
          (reify
            IAggregateSpec
            (aggregate [_ in-rel group-mapping]
              (let [in-vec (.vectorForName in-rel (name from-name))]
                (with-open [x2 (.project x2-projecter al (iv/->indirect-rel [in-vec]) {})]
                  (.aggregate sumx-agg in-rel group-mapping)
                  (.aggregate sumx2-agg (iv/->indirect-rel [x2]) group-mapping)
                  (.aggregate countx-agg in-rel group-mapping))))

            (finish [_]
              (let [sumx-ivec (.finish sumx-agg)
                    sumx2-ivec (.finish sumx2-agg)
                    countx-ivec (.finish countx-agg)
                    out-ivec (.project finish-projecter al (iv/->indirect-rel [countx-ivec sumx-ivec sumx2-ivec]) {})]
                (if (instance? NullVector (.getVector out-ivec))
                  out-ivec
                  (do
                    (doto (.makeTransferPair (.getVector out-ivec) res-vec)
                      (.transfer))
                    (iv/->direct-vec res-vec)))))

            Closeable
            (close [_]
              (util/try-close res-vec)
              (util/try-close sumx-agg)
              (util/try-close sumx2-agg)
              (util/try-close countx-agg))))))))

(defmethod ->aggregate-factory :var_pop [agg-opts] (->variance-agg-factory :var_pop agg-opts))
(defmethod ->aggregate-factory :var_samp [agg-opts] (->variance-agg-factory :var_samp agg-opts))

(defn- ->stddev-agg-factory [variance-op {:keys [from-name from-type to-name zero-row?]}]
  (let [variance-agg (->aggregate-factory {:f variance-op, :from-name from-name, :from-type from-type
                                           :to-name 'variance, :zero-row? zero-row?})
        finish-projecter (expr/->expression-projection-spec to-name '(sqrt variance)
                                                            {:col-types {'variance (.getToColumnType variance-agg)}})]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnType [_] (.getColumnType finish-projecter))

      (build [_ al]
        (let [variance-agg (.build variance-agg al)
              res-vec (Float8Vector. (name to-name) al)]
          (reify
            IAggregateSpec
            (aggregate [_ in-rel group-mapping]
              (.aggregate variance-agg in-rel group-mapping))

            (finish [_]
              (let [variance-ivec (.finish variance-agg)
                    out-ivec (.project finish-projecter al (iv/->indirect-rel [variance-ivec]) {})]
                (if (instance? NullVector (.getVector out-ivec))
                  out-ivec
                  (do
                    (doto (.makeTransferPair (.getVector out-ivec) res-vec)
                      (.transfer))
                    (iv/->direct-vec res-vec)))))

            Closeable
            (close [_]
              (util/try-close res-vec)
              (util/try-close variance-agg))))))))

(defmethod ->aggregate-factory :stddev_pop [agg-opts]
  (->stddev-agg-factory :var_pop agg-opts))

(defmethod ->aggregate-factory :stddev_samp [agg-opts]
  (->stddev-agg-factory :var_samp agg-opts))

(defn- assert-supported-min-max-types [from-types to-type]
  ;; TODO variable-width types - it's reasonable to want (e.g.) `(min <string-col>)`
  (when-let [unsupported-types (not-empty (->> from-types
                                               (into #{} (remove (comp (some-fn #(isa? types/col-type-hierarchy % :num)
                                                                                #{:duration :date :timestamp-local :timestamp-tz :time-local})
                                                                       types/col-type-head)))))]
    (throw (err/runtime-err :core2.group-by/unsupported-min-max-types
                            {::err/message "Unsupported types in min/max aggregate"
                             :unsupported-types unsupported-types})))

  (when (= :any to-type)
    (throw (err/runtime-err :core2.group-by/incomparable-min-max-types
                            {::err/message "Incomparable types in min/max aggregate"
                             :types from-types}))))

(defn- min-max-factory
  "compare-kw: update the accumulated value if `(compare-kw el acc)`"
  [compare-kw {:keys [from-name from-type] :as agg-opts}]

  (let [from-types (-> (types/flatten-union-types from-type)
                       (disj :null))
        to-type (types/least-upper-bound from-types)]
    (assert-supported-min-max-types from-types to-type)
    (reducing-agg-factory (into agg-opts
                                {:to-type to-type
                                 :val-expr {:op :call, :f :cast, :target-type to-type
                                            :args [{:op :variable, :variable from-name}]}
                                 :step-expr {:op :if,
                                             :pred {:op :call, :f compare-kw,
                                                    :args [{:op :local, :local val-local}
                                                           {:op :local, :local acc-local}]}
                                             :then {:op :local, :local val-local}
                                             :else {:op :local, :local acc-local}}}))))

(defmethod ->aggregate-factory :min [agg-opts] (min-max-factory :< agg-opts))
(defmethod ->aggregate-factory :min-all [agg-opts] (min-max-factory :< agg-opts))
(defmethod ->aggregate-factory :min-distinct [agg-opts] (min-max-factory :< agg-opts))
(defmethod ->aggregate-factory :max [agg-opts] (min-max-factory :> agg-opts))
(defmethod ->aggregate-factory :max-all [agg-opts] (min-max-factory :> agg-opts))
(defmethod ->aggregate-factory :max-distinct [agg-opts] (min-max-factory :> agg-opts))

(defn- wrap-distinct [^IAggregateSpecFactory agg-factory, from-name, from-type]
  (reify IAggregateSpecFactory
    (getToColumnName [_] (.getToColumnName agg-factory))
    (getToColumnType [_] (.getToColumnType agg-factory))

    (build [_ al]
      (let [agg-spec (.build agg-factory al)
            rel-maps (ArrayList.)]
        (reify
          IAggregateSpec
          (aggregate [_ in-rel group-mapping]
            (let [in-vec (.vectorForName in-rel (name from-name))
                  builders (ArrayList. (.size rel-maps))
                  distinct-idxs (IntStream/builder)]
              (dotimes [idx (.getValueCount in-vec)]
                (let [group-idx (.get group-mapping idx)]
                  (while (<= (.size rel-maps) group-idx)
                    (.add rel-maps (emap/->relation-map al {:build-col-types [from-name from-type]
                                                            :build-key-col-names [from-name]})))
                  (let [^IRelationMap rel-map (nth rel-maps group-idx)]
                    (while (<= (.size builders) group-idx)
                      (.add builders nil))

                    (let [^IRelationMapBuilder
                          builder (or (nth builders group-idx)
                                      (let [builder (.buildFromRelation rel-map (iv/->indirect-rel [in-vec]))]
                                        (.set builders group-idx builder)
                                        builder))]
                      (when (neg? (.addIfNotPresent builder idx))
                        (.add distinct-idxs idx))))))
              (let [distinct-idxs (.toArray (.build distinct-idxs))]
                (with-open [distinct-gm (-> (iv/->direct-vec group-mapping)
                                            (.select distinct-idxs)
                                            (.copy al))]
                  (.aggregate agg-spec
                              (iv/->indirect-rel [(.select in-vec distinct-idxs)])
                              (.getVector distinct-gm))))))

          (finish [_] (.finish agg-spec))

          Closeable
          (close [_]
            (util/try-close agg-spec)
            (run! util/try-close rel-maps)))))))

(defmethod ->aggregate-factory :count-distinct [{:keys [from-name from-type] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :count))
      (wrap-distinct from-name from-type)))

(defmethod ->aggregate-factory :count-all [agg-opts]
  (->aggregate-factory (assoc agg-opts :f :count)))

(defmethod ->aggregate-factory :sum-distinct [{:keys [from-name from-type] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :sum))
      (wrap-distinct from-name from-type)))

(defmethod ->aggregate-factory :sum-all [agg-opts]
  (->aggregate-factory (assoc agg-opts :f :sum)))

(defmethod ->aggregate-factory :avg-distinct [{:keys [from-name from-type] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :avg))
      (wrap-distinct from-name from-type)))

(defmethod ->aggregate-factory :avg-all [agg-opts]
  (->aggregate-factory (assoc agg-opts :f :avg)))

(deftype ArrayAggAggregateSpec [^BufferAllocator allocator
                                from-name to-name
                                ^IVectorWriter acc-col
                                ^:unsynchronized-mutable ^ListVector out-vec
                                ^:unsynchronized-mutable ^long base-idx
                                ^List group-idxmaps]
  IAggregateSpec
  (aggregate [this in-rel group-mapping]
    (let [in-vec (.vectorForName in-rel (name from-name))
          row-count (.getValueCount in-vec)]
      (vw/append-vec acc-col in-vec)

      (dotimes [idx row-count]
        (let [group-idx (.get group-mapping idx)]
          (while (<= (.size group-idxmaps) group-idx)
            (.add group-idxmaps (IntStream/builder)))
          (.add ^IntStream$Builder (.get group-idxmaps group-idx)
                (+ base-idx idx))))

      (set! (.base-idx this) (+ base-idx row-count))))

  (finish [this]
    (let [out-vec (-> (types/->field (name to-name) types/list-type false (.getField (.getVector acc-col)))
                      (.createVector allocator))]
      (set! (.out-vec this) out-vec)

      (let [list-writer (.asList (vw/vec->writer out-vec))
            data-writer (.getDataWriter list-writer)
            row-copier (.rowCopier data-writer (.getVector acc-col))]
        (doseq [^IntStream$Builder isb group-idxmaps]
          (.startValue list-writer)
          (.forEach (.build isb)
                    (reify IntConsumer
                      (accept [_ idx]
                        (.startValue data-writer)
                        (.copyRow row-copier idx)
                        (.endValue data-writer))))
          (.endValue list-writer))

        (.setValueCount out-vec (.size group-idxmaps))
        (iv/->direct-vec out-vec))))

  Closeable
  (close [_]
    (util/try-close acc-col)
    (util/try-close out-vec)))

(defmethod ->aggregate-factory :array-agg [{:keys [from-name from-type to-name]}]
  (let [to-type [:list from-type]]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnType [_] to-type)

      (build [_ al]
        (ArrayAggAggregateSpec. al from-name to-name
                                (vw/->vec-writer al (name to-name))
                                nil 0 (ArrayList.))))))

(defmethod ->aggregate-factory :array-agg-distinct [{:keys [from-name from-type] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :array-agg))
      (wrap-distinct from-name from-type)))

(defn- bool-agg-factory [step-f-kw {:keys [from-name] :as agg-opts}]
  (reducing-agg-factory (into agg-opts
                              {:to-type :bool
                               :val-expr {:op :variable, :variable from-name}
                               :step-expr {:op :call, :f step-f-kw,
                                           :args [{:op :local, :local acc-local}
                                                  {:op :local, :local val-local}]}})))

(defmethod ->aggregate-factory :all [agg-opts] (bool-agg-factory :and agg-opts))
(defmethod ->aggregate-factory :every [agg-opts] (->aggregate-factory (assoc agg-opts :f :all)))
(defmethod ->aggregate-factory :any [agg-opts] (bool-agg-factory :or agg-opts))
(defmethod ->aggregate-factory :some [agg-opts] (->aggregate-factory (assoc agg-opts :f :any)))

(deftype GroupByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^IGroupMapper group-mapper
                        ^List aggregate-specs
                        ^:unsynchronized-mutable ^boolean done?]
  ICursor
  (tryAdvance [this c]
    (boolean
     (when-not done?
       (set! (.done? this) true)

       (try
         (.forEachRemaining in-cursor
                            (reify Consumer
                              (accept [_ in-rel]
                                (with-open [group-mapping (.groupMapping group-mapper in-rel)]
                                  (doseq [^IAggregateSpec agg-spec aggregate-specs]
                                    (.aggregate agg-spec in-rel group-mapping))))))

         (let [out-rel (iv/->indirect-rel (concat (.finish group-mapper)
                                                  (map #(.finish ^IAggregateSpec %) aggregate-specs)))]
           (if (pos? (.rowCount out-rel))
             (do
               (.accept c out-rel)
               true)
             false))
         (finally
           (util/try-close group-mapper)
           (run! util/try-close aggregate-specs))))))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE))

  (close [_]
    (run! util/try-close aggregate-specs)
    (util/try-close in-cursor)
    (util/try-close group-mapper)))

(defmethod lp/emit-expr :group-by [{:keys [columns relation]} args]
  (let [{group-cols :group-by, aggs :aggregate} (group-by first columns)
        group-cols (mapv second group-cols)]
    (lp/unary-expr relation args
      (fn [col-types]
        (let [agg-factories (for [[_ agg] aggs]
                              (let [[to-column agg-form] (first agg)]
                                (->aggregate-factory (into {:to-name to-column
                                                            :zero-row? (empty? group-cols)}
                                                           (zmatch agg-form
                                                             [:nullary agg-opts]
                                                             (select-keys agg-opts [:f])

                                                             [:unary agg-opts]
                                                             (let [{:keys [f from-column]} agg-opts]
                                                               {:f f
                                                                :from-name from-column
                                                                :from-type (get col-types from-column :null)}))))))]
          {:col-types (-> (into (->> group-cols
                                     (into {} (map (juxt identity col-types))))
                                (->> agg-factories
                                     (into {} (map (juxt #(.getToColumnName ^IAggregateSpecFactory %)
                                                         #(.getToColumnType ^IAggregateSpecFactory %)))))))

           :->cursor (fn [{:keys [allocator]} in-cursor]
                       (let [agg-specs (LinkedList.)]
                         (try
                           (doseq [^IAggregateSpecFactory factory agg-factories]
                             (.add agg-specs (.build factory allocator)))

                           (GroupByCursor. allocator in-cursor
                                           (->group-mapper allocator (select-keys col-types group-cols))
                                           (vec agg-specs)
                                           false)

                           (catch Exception e
                             (run! util/try-close agg-specs)
                             (throw e)))))})))))
