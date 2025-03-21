(ns xtdb.operator.group-by
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.expression.map :as emap]
            [xtdb.logical-plan :as lp]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.io Closeable)
           (java.util ArrayList LinkedList List Spliterator)
           (java.util.function Consumer IntConsumer)
           (java.util.stream IntStream IntStream$Builder)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector Float8Vector IntVector ValueVector)
           (org.apache.arrow.vector.complex ListVector)
           org.apache.arrow.vector.types.pojo.FieldType
           (xtdb ICursor)
           (xtdb.arrow RelationReader VectorReader)
           (xtdb.expression.map IRelationMap IRelationMapBuilder)
           (xtdb.vector IVectorWriter)))

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

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IGroupMapper
  (^org.apache.arrow.vector.IntVector groupMapping [^xtdb.vector.RelationReader inRelation])
  (^java.util.List #_<IVectorReader> finish []))

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
    (let [row-count (.rowCount in-rel)]
      (.allocateNew group-mapping row-count)

      (let [builder (.buildFromRelation rel-map in-rel)]
        (dotimes [idx row-count]
          (.set group-mapping idx (emap/inserted-idx (.addIfNotPresent builder idx)))))

      (.setValueCount group-mapping row-count)

      group-mapping))

  (finish [_]
    (seq (.getBuiltRelation rel-map)))

  Closeable
  (close [_]
    (.close group-mapping)
    (util/try-close rel-map)))

(defn ->group-mapper [^BufferAllocator allocator, group-fields]
  (let [gm-vec (.createVector #xt.arrow/field ["group-mapping" #xt.arrow/field-type [#xt.arrow/type :i32 false]]
                              allocator)]
    (if-let [group-col-names (not-empty (set (keys group-fields)))]
      (GroupMapper. group-col-names
                    (emap/->relation-map allocator {:build-fields group-fields
                                                    :build-key-col-names (vec group-col-names)
                                                    :nil-keys-equal? true})
                    gm-vec)
      (NullGroupMapper. gm-vec))))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IAggregateSpec
  (^void aggregate [^xtdb.vector.RelationReader inRelation,
                    ^org.apache.arrow.vector.IntVector groupMapping])
  (^xtdb.vector.IVectorReader finish []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IAggregateSpecFactory
  (^clojure.lang.Symbol getToColumnName [])
  (getToColumnField [])
  (^xtdb.operator.group_by.IAggregateSpec build [^org.apache.arrow.memory.BufferAllocator allocator]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti ^xtdb.operator.group_by.IAggregateSpecFactory ->aggregate-factory
  (fn [{:keys [f from-name from-type to-name zero-row?]}]
    (expr/normalise-fn-name f)))

(defmethod ->aggregate-factory :row_count [{:keys [to-name zero-row?]}]
  (reify IAggregateSpecFactory
    (getToColumnName [_] to-name)
    (getToColumnField [_] (types/col-type->field :i64))

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
            (vr/vec->reader out-vec))

          Closeable
          (close [_] (.close out-vec)))))))

(defmethod ->aggregate-factory :count [{:keys [from-name to-name zero-row?]}]
  (reify IAggregateSpecFactory
    (getToColumnName [_] to-name)
    (getToColumnField [_] (types/col-type->field :i64))

    (build [_ al]
      (let [^BigIntVector out-vec (-> (types/col-type->field to-name :i64)
                                      (.createVector al))]
        (reify
          IAggregateSpec
          (aggregate [_ in-rel group-mapping]
            (let [in-col (.readerForName in-rel (str from-name))]
              (dotimes [idx (.rowCount in-rel)]
                (let [group-idx (.get group-mapping idx)]
                  (when (<= (.getValueCount out-vec) group-idx)
                    (.setValueCount out-vec (inc group-idx))
                    (.set out-vec group-idx 0))

                  (when-not (.isNull in-col idx)
                    (.set out-vec group-idx (inc (.get out-vec group-idx))))))))

          (finish [_]
            (when (and zero-row? (zero? (.getValueCount out-vec)))
              (.setValueCount out-vec 1)
              (.set out-vec 0 0))
            (vr/vec->reader out-vec))

          Closeable
          (close [_] (.close out-vec)))))))

(def ^:private acc-sym (gensym 'acc))
(def ^:private acc-col-sym (gensym 'acc_col))
(def ^:private group-idx-sym (gensym 'group_idx))
(def ^:private acc-local (gensym 'acc_local))
(def ^:private val-local (gensym 'val_local))
(def ^:private acc-writer-sym (gensym 'acc_writer))

(def emit-agg
  (-> (fn [{:keys [to-type val-expr step-expr]} input-opts]
        (let [group-mapping-sym (gensym 'group-mapping)

              return-type [:union (conj #{:null} to-type)]

              acc-expr {:op :variable, :variable acc-col-sym, :idx group-idx-sym
                        :extract-vec-from-rel? false}
              agg-expr (-> {:op :if-some, :local val-local, :expr val-expr
                            :then {:op :if-some, :local acc-local,
                                   :expr acc-expr
                                   :then step-expr
                                   :else {:op :local, :local val-local}}
                            :else acc-expr}
                           (expr/prepare-expr))

              ;; ignore return-type of the codegen because it may be more specific than the acc type
              {:keys [continue] :as emitted-expr} (expr/codegen-expr agg-expr input-opts)]

          {:return-type return-type
           :eval-agg (-> `(fn [~(-> acc-sym (expr/with-tag ValueVector))
                               ~(-> expr/rel-sym (expr/with-tag RelationReader))
                               ~(-> group-mapping-sym (expr/with-tag IntVector))]
                            (let [~acc-col-sym (vr/vec->reader ~acc-sym)
                                  ~acc-writer-sym (vw/->writer ~acc-sym)
                                  ~@(expr/batch-bindings emitted-expr)]
                              (dotimes [~expr/idx-sym (.getRowCount ~expr/rel-sym)]
                                (let [~group-idx-sym (.get ~group-mapping-sym ~expr/idx-sym)]
                                  (when (<= (.getValueCount ~acc-sym) ~group-idx-sym)
                                    (.setValueCount ~acc-sym (inc ~group-idx-sym)))

                                  ~(continue (fn [acc-type acc-code]
                                               `(do
                                                  (.setPosition (.writerPosition ~acc-writer-sym) ~group-idx-sym)
                                                  ~(expr/write-value-code acc-type acc-writer-sym acc-code))))))))
                         #_(doto clojure.pprint/pprint) ;; <<no-commit>>
                         eval)}))
      (util/lru-memoize))) ;; <<no-commit>>


(defn- reducing-agg-factory [{:keys [to-name to-type zero-row?] :as agg-opts}]
  (let [to-type [:union (conj #{:null} to-type)]
        to-field (types/col-type->field to-name to-type)]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnField [_] (types/col-type->field to-type))

      (build [_ al]
        (let [^ValueVector out-vec (.createVector to-field al)]
          (reify
            IAggregateSpec
            (aggregate [_ in-rel group-mapping]
              (let [in-rel (RelationReader/from in-rel)
                    input-opts {:var->col-type (->> (seq in-rel)
                                                    (into {acc-col-sym to-type}
                                                          (map (juxt #(symbol (.getName ^VectorReader %))
                                                                     #(-> (.getField ^VectorReader %) types/field->col-type)))))}
                    {:keys [eval-agg]} (emit-agg agg-opts input-opts)]
                (eval-agg out-vec in-rel group-mapping)))

            (finish [_]
              (when (and zero-row? (zero? (.getValueCount out-vec)))
                (.setValueCount out-vec 1))

              (vr/vec->reader out-vec))

            Closeable
            (close [_] (.close out-vec))))))))

(defn- ->projector ^xtdb.operator.ProjectionSpec [col-name form input-types]
  (expr/->expression-projection-spec col-name (expr/form->expr form input-types) input-types))

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
        input-types {:col-types {'sum (types/field->col-type (.getToColumnField sum-agg))
                                 'cnt (types/field->col-type (.getToColumnField count-agg))}}
        projecter (->projector to-name '(/ (double sum) cnt) input-types)]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnField [_] (types/col-type->field (.getColumnType projecter)))

      (build [_ al]
        (let [sum-agg (.build sum-agg al)
              count-agg (.build count-agg al)]
          (reify
            IAggregateSpec
            (aggregate [_ in-rel group-mapping]
              (.aggregate sum-agg in-rel group-mapping)
              (.aggregate count-agg in-rel group-mapping))

            (finish [_]
              (with-open [sum (.finish sum-agg)
                          count (.finish count-agg)]
                (.project projecter al (vr/rel-reader [sum count]) {} vw/empty-args)))

            Closeable
            (close [_]
              (util/close sum-agg)
              (util/close count-agg))))))))

(defn- ->variance-agg-factory [variance-op {:keys [from-name from-type to-name zero-row?]}]
  (let [countx-agg (->aggregate-factory {:f :count, :from-name from-name, :from-type from-type
                                         :to-name 'countx, :zero-row? zero-row?})

        sumx-agg (->aggregate-factory {:f :sum, :from-name from-name, :from-type from-type
                                       :to-name 'sumx, :zero-row? zero-row?})

        x2-projecter (->projector 'x2 (list '* from-name from-name)
                                  {:col-types {from-name from-type}})

        sumx2-agg (->aggregate-factory {:f :sum, :from-name 'x2, :from-type (.getColumnType x2-projecter)
                                        :to-name 'sumx2, :zero-row? zero-row?})

        finish-projecter (->projector to-name (case variance-op
                                                :var-pop '(if (> countx 0)
                                                            (/ (- sumx2
                                                                  (/ (* sumx sumx) (double countx)))
                                                               (double countx))
                                                            nil)
                                                :var-samp '(if (> countx 1)
                                                             (/ (- sumx2
                                                                   (/ (* sumx sumx) (double countx)))
                                                                (double (- countx 1)))
                                                             nil))
                                      {:col-types {'sumx (types/field->col-type (.getToColumnField sumx-agg))
                                                   'sumx2 (types/field->col-type (.getToColumnField sumx2-agg))
                                                   'countx (types/field->col-type (.getToColumnField countx-agg))}})]

    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnField [_] (types/col-type->field (.getColumnType finish-projecter)))

      (build [_ al]
        (let [sumx-agg (.build sumx-agg al)
              sumx2-agg (.build sumx2-agg al)
              countx-agg (.build countx-agg al)]
          (reify
            IAggregateSpec
            (aggregate [_ in-rel group-mapping]
              (let [in-vec (.readerForName in-rel (str from-name))]
                (with-open [x2 (.project x2-projecter al (vr/rel-reader [in-vec]) {} vw/empty-args)]
                  (.aggregate sumx-agg in-rel group-mapping)
                  (.aggregate sumx2-agg (vr/rel-reader [x2]) group-mapping)
                  (.aggregate countx-agg in-rel group-mapping))))

            (finish [_]
              (with-open [sumx (.finish sumx-agg)
                          sumx2 (.finish sumx2-agg)
                          countx (.finish countx-agg)]
                (.project finish-projecter al
                          (vr/rel-reader [sumx sumx2 countx])
                          {}
                          vw/empty-args)))

            Closeable
            (close [_]
              (util/try-close sumx-agg)
              (util/try-close sumx2-agg)
              (util/try-close countx-agg))))))))

(defmethod ->aggregate-factory :var_pop [agg-opts] (->variance-agg-factory :var-pop agg-opts))
(defmethod ->aggregate-factory :var_samp [agg-opts] (->variance-agg-factory :var-samp agg-opts))

(defn- ->stddev-agg-factory [variance-op {:keys [from-name from-type to-name zero-row?]}]
  (let [variance-agg (->aggregate-factory {:f variance-op, :from-name from-name, :from-type from-type
                                           :to-name 'variance, :zero-row? zero-row?})
        finish-projecter (->projector to-name '(sqrt variance)
                                      {:col-types {'variance (types/field->col-type (.getToColumnField variance-agg))}})]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnField [_] (types/col-type->field (.getColumnType finish-projecter)))

      (build [_ al]
        (let [variance-agg (.build variance-agg al)
              res-vec (Float8Vector. (str to-name) al)]
          (reify
            IAggregateSpec
            (aggregate [_ in-rel group-mapping]
              (.aggregate variance-agg in-rel group-mapping))

            (finish [_]
              (with-open [variance (.finish variance-agg)]
                (.project finish-projecter al (vr/rel-reader [variance]) {} vw/empty-args)))

            Closeable
            (close [_]
              (util/try-close res-vec)
              (util/try-close variance-agg))))))))

(defmethod ->aggregate-factory :stddev_pop [agg-opts]
  (->stddev-agg-factory :var-pop agg-opts))

(defmethod ->aggregate-factory :stddev_samp [agg-opts]
  (->stddev-agg-factory :var-samp agg-opts))

(defn- assert-supported-min-max-types [from-types to-type]
  ;; TODO variable-width types - it's reasonable to want (e.g.) `(min <string-col>)`
  (when-let [unsupported-types (not-empty (->> from-types
                                               (into #{} (remove (comp (some-fn #(isa? types/col-type-hierarchy % :num)
                                                                                #{:duration :date :timestamp-local :timestamp-tz :time-local})
                                                                       types/col-type-head)))))]
    (throw (err/runtime-err :xtdb.group-by/unsupported-min-max-types
                            {::err/message "Unsupported types in min/max aggregate"
                             :unsupported-types unsupported-types})))

  (when (= :any to-type)
    (throw (err/runtime-err :xtdb.group-by/incomparable-min-max-types
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
(defmethod ->aggregate-factory :min_all [agg-opts] (min-max-factory :< agg-opts))
(defmethod ->aggregate-factory :min_distinct [agg-opts] (min-max-factory :< agg-opts))
(defmethod ->aggregate-factory :max [agg-opts] (min-max-factory :> agg-opts))
(defmethod ->aggregate-factory :max_all [agg-opts] (min-max-factory :> agg-opts))
(defmethod ->aggregate-factory :max_distinct [agg-opts] (min-max-factory :> agg-opts))

(defn- wrap-distinct [^IAggregateSpecFactory agg-factory, from-name, from-type]
  (reify IAggregateSpecFactory
    (getToColumnName [_] (.getToColumnName agg-factory))
    (getToColumnField [_] (.getToColumnField agg-factory))

    (build [_ al]
      (let [agg-spec (.build agg-factory al)
            rel-maps (ArrayList.)]
        (reify
          IAggregateSpec
          (aggregate [_ in-rel group-mapping]
            (let [in-vec (.readerForName in-rel (str from-name))
                  builders (ArrayList. (.size rel-maps))
                  distinct-idxs (IntStream/builder)]
              (dotimes [idx (.getValueCount in-vec)]
                (let [group-idx (.get group-mapping idx)]
                  (while (<= (.size rel-maps) group-idx)
                    (.add rel-maps (emap/->relation-map al {:build-fields {from-name (types/col-type->field from-type)}
                                                            :build-key-col-names [from-name]})))
                  (let [^IRelationMap rel-map (nth rel-maps group-idx)]
                    (while (<= (.size builders) group-idx)
                      (.add builders nil))

                    (let [^IRelationMapBuilder
                          builder (or (nth builders group-idx)
                                      (let [builder (.buildFromRelation rel-map (vr/rel-reader [in-vec]))]
                                        (.set builders group-idx builder)
                                        builder))]
                      (when (neg? (.addIfNotPresent builder idx))
                        (.add distinct-idxs idx))))))
              (let [distinct-idxs (.toArray (.build distinct-idxs))]
                (with-open [distinct-gm (-> (.getField group-mapping)
                                            (.createVector al))]
                  (-> (vr/vec->reader group-mapping)
                      (.select distinct-idxs)
                      (.copyTo distinct-gm))
                  (.aggregate agg-spec
                              (vr/rel-reader [(.select in-vec distinct-idxs)])
                              distinct-gm)))))

          (finish [_] (.finish agg-spec))

          Closeable
          (close [_]
            (util/try-close agg-spec)
            (run! util/try-close rel-maps)))))))

(defmethod ->aggregate-factory :count_distinct [{:keys [from-name from-type] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :count))
      (wrap-distinct from-name from-type)))

(defmethod ->aggregate-factory :count_all [agg-opts]
  (->aggregate-factory (assoc agg-opts :f :count)))

(defmethod ->aggregate-factory :sum_distinct [{:keys [from-name from-type] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :sum))
      (wrap-distinct from-name from-type)))

(defmethod ->aggregate-factory :sum_all [agg-opts]
  (->aggregate-factory (assoc agg-opts :f :sum)))

(defmethod ->aggregate-factory :avg_distinct [{:keys [from-name from-type] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :avg))
      (wrap-distinct from-name from-type)))

(defmethod ->aggregate-factory :avg_all [agg-opts]
  (->aggregate-factory (assoc agg-opts :f :avg)))

(deftype ArrayAggAggregateSpec [^BufferAllocator allocator
                                from-name to-name to-type
                                ^IVectorWriter acc-col
                                ^:unsynchronized-mutable ^ListVector out-vec
                                ^:unsynchronized-mutable ^long base-idx
                                ^List group-idxmaps
                                on-empty]
  IAggregateSpec
  (aggregate [this in-rel group-mapping]
    (let [in-vec (.readerForName in-rel (str from-name))
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
    (let [out-vec (-> (types/col-type->field to-name to-type)
                      (.createVector allocator))]
      (set! (.out-vec this) out-vec)

      (let [list-writer (vw/->writer out-vec)
            el-writer (.listElementWriter list-writer)
            row-copier (.rowCopier (vw/vec-wtr->rdr acc-col) el-writer)]
        (doseq [^IntStream$Builder isb group-idxmaps]
          (.forEach (.build isb)
                    (reify IntConsumer
                      (accept [_ idx]
                        (.copyRow row-copier idx))))
          (.endList list-writer))

        (let [value-count (.size group-idxmaps)]
          (when (and (zero? value-count) (= :empty-vec on-empty))
            (.endList list-writer))
          (.setValueCount out-vec (if (zero? value-count)
                                    (case on-empty
                                     :null 1
                                     :empty-rel 0
                                     :empty-vec 1)
                                    value-count)))
        (vr/vec->reader out-vec))))

  Closeable
  (close [_]
    (util/try-close acc-col)
    (util/try-close out-vec)))

(defmethod ->aggregate-factory :array_agg [{:keys [from-name from-type to-name zero-row?]}]
  (let [to-type [:list from-type]]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnField [_] (types/col-type->field to-type))

      (build [_ al]
        (ArrayAggAggregateSpec. al from-name to-name to-type
                                (vw/->vec-writer al (str to-name) (FieldType/notNullable #xt.arrow/type :union))
                                nil 0 (ArrayList.) (if zero-row? :null :empty-rel))))))

(defmethod ->aggregate-factory :array_agg_distinct [{:keys [from-name from-type] :as agg-opts}]
  (-> (->aggregate-factory (assoc agg-opts :f :array_agg))
      (wrap-distinct from-name from-type)))

(defmethod ->aggregate-factory :vec_agg [{:keys [from-name from-type to-name zero-row?]}]
  (let [to-type [:list from-type]]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnField [_] (types/col-type->field to-type))

      (build [_ al]
        (ArrayAggAggregateSpec. al from-name to-name to-type
                                (vw/->vec-writer al (str to-name) (FieldType/notNullable #xt.arrow/type :union))
                                nil 0 (ArrayList.) (if zero-row? :empty-vec :empty-rel))))))

(defn- bool-agg-factory [step-f-kw {:keys [from-name] :as agg-opts}]
  (reducing-agg-factory (into agg-opts
                              {:to-type :bool
                               :val-expr {:op :variable, :variable from-name}
                               :step-expr {:op :call, :f step-f-kw,
                                           :args [{:op :local, :local acc-local}
                                                  {:op :local, :local val-local}]}})))

(defmethod ->aggregate-factory :bool_and [agg-opts] (bool-agg-factory :and agg-opts))
(defmethod ->aggregate-factory :every [agg-opts] (->aggregate-factory (assoc agg-opts :f :bool_and)))
(defmethod ->aggregate-factory :bool_or [agg-opts] (bool-agg-factory :or agg-opts))

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

         (util/with-open [agg-cols (map #(.finish ^IAggregateSpec %) aggregate-specs)]
           (let [out-rel (vr/rel-reader (concat (.finish group-mapper) agg-cols))]
             (if (pos? (.rowCount out-rel))
               (do
                 (.accept c out-rel)
                 true)
               false)))
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
    (lp/unary-expr (lp/emit-expr relation args)
      (fn [fields]
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
                                                                :from-type (-> (get fields from-column types/null-field)
                                                                               types/field->col-type)}))))))]
          {:fields (-> (into (->> group-cols
                                  (into {} (map (juxt identity fields))))
                             (->> agg-factories
                                  (into {} (map (juxt #(.getToColumnName ^IAggregateSpecFactory %)
                                                      #(.getToColumnField ^IAggregateSpecFactory %)))))))

           :->cursor (fn [{:keys [allocator]} in-cursor]
                       (let [agg-specs (LinkedList.)]
                         (try
                           (doseq [^IAggregateSpecFactory factory agg-factories]
                             (.add agg-specs (.build factory allocator)))

                           (GroupByCursor. allocator in-cursor
                                           (->group-mapper allocator (select-keys fields group-cols))
                                           (vec agg-specs)
                                           false)

                           (catch Exception e
                             (run! util/try-close agg-specs)
                             (throw e)))))})))))
