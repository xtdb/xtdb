(ns core2.operator.group-by
  (:require [core2.expression :as expr]
            [core2.expression.map :as emap]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import (core2 ICursor)
           (core2.expression.map IRelationMap IRelationMapBuilder)
           (core2.vector IIndirectRelation IIndirectVector IVectorWriter)
           (java.io Closeable)
           (java.util ArrayList LinkedList List HashMap Spliterator)
           (java.util.function Consumer IntConsumer)
           (java.util.stream IntStream IntStream$Builder)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector Float8Vector IntVector NullVector ValueVector)
           (org.apache.arrow.vector.complex ListVector)
           (org.apache.arrow.vector.types.pojo ArrowType$FloatingPoint ArrowType$Int FieldType)))

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

(defn- ->group-mapper [^BufferAllocator allocator, group-col-names]
  (let [gm-vec (IntVector. "group-mapping" allocator)]
    (if (seq group-col-names)
      (GroupMapper. group-col-names
                    (emap/->relation-map allocator {:key-col-names group-col-names,
                                                    :store-col-names #{}
                                                    :nil-keys-equal? true})
                    gm-vec)
      (NullGroupMapper. gm-vec))))

(definterface IAggregateSpec
  (^String getFromColumnName [])
  (^void aggregate [^core2.vector.IIndirectVector inVector,
                    ^org.apache.arrow.vector.IntVector groupMapping])
  (^core2.vector.IIndirectVector finish []))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IAggregateSpecFactory
  (^String getToColumnName [])
  (^core2.operator.group_by.IAggregateSpec build [^org.apache.arrow.memory.BufferAllocator allocator]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti ^core2.operator.group_by.IAggregateSpecFactory ->aggregate-factory
  (fn [f from-name to-name]
    (keyword (name f))))

(def emit-agg
  (-> (fn [from-var from-val-types, emit-init-group, emit-step]
        (let [acc-sym (gensym 'acc)
              group-mapping-sym (gensym 'group-mapping)
              group-idx-sym (gensym 'group-idx)
              boxes (HashMap.)
              {continue-var :continue} (expr/codegen-expr (expr/form->expr from-var {:col-names #{from-var}})
                                                          {:var->types {from-var from-val-types}
                                                           :return-boxes boxes})]
          (eval
           `(fn [~(-> acc-sym (expr/with-tag ValueVector))
                 ~(-> from-var (expr/with-tag IIndirectVector))
                 ~(-> group-mapping-sym (expr/with-tag IntVector))]
              (let [~@(expr/box-bindings (vals boxes))]
                (dotimes [~expr/idx-sym (.getValueCount ~from-var)]
                  (let [~group-idx-sym (.get ~group-mapping-sym ~expr/idx-sym)]
                    (when (<= (.getValueCount ~acc-sym) ~group-idx-sym)
                      (.setValueCount ~acc-sym (inc ~group-idx-sym))
                      ~(emit-init-group acc-sym group-idx-sym))
                    ~(continue-var (fn [var-type val-code]
                                     (emit-step var-type acc-sym group-idx-sym val-code))))))))))
      (memoize)))

(defn- monomorphic-agg-factory {:style/indent 2} [^String from-name, ^String to-name, ->out-vec, emit-init-group, emit-step]
  (let [from-var (symbol from-name)]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)

      (build [_ al]
        (let [^ValueVector out-vec (->out-vec al)]
          (reify
            IAggregateSpec
            (getFromColumnName [_] from-name)

            (aggregate [_ in-vec group-mapping]
              (let [from-val-types (expr/field->value-types (.getField (.getVector in-vec)))
                    f (emit-agg from-var from-val-types emit-init-group emit-step)]
                (f out-vec in-vec group-mapping)))

            (finish [_] (iv/->direct-vec out-vec))

            Closeable
            (close [_] (.close out-vec))))))))

(defmethod ->aggregate-factory :count [_ ^String from-name, ^String to-name]
  (monomorphic-agg-factory from-name to-name #(BigIntVector. to-name ^BufferAllocator %)
    (fn emit-init-count-group [acc-sym group-idx-sym]
      `(let [~(-> acc-sym (expr/with-tag BigIntVector)) ~acc-sym]
         (.setIndexDefined ~acc-sym ~group-idx-sym)
         (.set ~acc-sym ~group-idx-sym 0)))

    (fn emit-count-step [var-type acc-sym group-idx-sym _val-code]
      `(let [~(-> acc-sym (expr/with-tag BigIntVector)) ~acc-sym]
         (when-not ~(= var-type types/null-type)
           (.set ~acc-sym ~group-idx-sym
                 (inc (.get ~acc-sym ~group-idx-sym))))))))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IPromotableVector
  (^org.apache.arrow.vector.ValueVector getVector [])
  (^org.apache.arrow.vector.ValueVector maybePromote [valueTypes]))

(def ^:private emit-copy-vec
  (let [in-vec-sym (gensym 'in)
        out-vec-sym (gensym 'out)]
    (-> (fn [in-type out-type]
          (eval
           `(fn [~(-> in-vec-sym (expr/with-tag (types/arrow-type->vector-type in-type)))
                 ~(-> out-vec-sym (expr/with-tag (types/arrow-type->vector-type out-type)))]
              (let [row-count# (.getValueCount ~in-vec-sym)]
                (.setValueCount ~out-vec-sym row-count#)
                (dotimes [idx# row-count#]
                  (.set ~out-vec-sym idx# (~(expr/type->cast out-type) (.get ~in-vec-sym idx#))))
                ~out-vec-sym))))
        (memoize))))

(deftype PromotableVector [^BufferAllocator allocator,
                           ^:unsynchronized-mutable ^ValueVector v]
  Closeable
  (close [this] (util/try-close (.getVector this)))

  IPromotableVector
  (getVector [_] v)

  (maybePromote [this from-val-types]
    (let [^ValueVector cur-vec (.getVector this)
          cur-type (.getType (.getField cur-vec))

          new-type (types/least-upper-bound (->> (cond-> from-val-types
                                                   (not (coll? from-val-types)) vector)
                                                 (map #(.getType ^FieldType %))
                                                 (cons cur-type)
                                                 (into [] (filter #(isa? types/arrow-type-hierarchy (class %) ::types/Number)))))

          new-vec (if (= cur-type new-type)
                    cur-vec

                    (let [new-vec (.createVector (types/->field (.getName cur-vec) new-type false) allocator)]
                      (try
                        (when-not (= types/null-type cur-type)
                          (let [copy-vec (emit-copy-vec cur-type new-type)]
                            (copy-vec cur-vec new-vec)))
                        new-vec

                        (catch Throwable e
                          (.close new-vec)
                          (throw e))

                        (finally
                          (.close cur-vec)))))]

      (set! (.v this) new-vec)

      new-vec)))

(defn- promotable-agg-factory {:style/indent 2} [^String from-name, ^String to-name, emit-init-group, emit-step]
  (let [from-var (symbol from-name)
        emit-init-group (memoize (fn [acc-type] (partial emit-init-group acc-type)))
        emit-step (memoize (fn [acc-type] (partial emit-step acc-type)))]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)

      (build [_ al]
        (let [out-pvec (PromotableVector. al (NullVector. to-name))]
          (reify
            IAggregateSpec
            (getFromColumnName [_] from-name)

            (aggregate [_ in-vec group-mapping]
              (when (pos? (.getValueCount in-vec))
                (let [from-val-types (expr/field->value-types (.getField (.getVector in-vec)))
                      out-vec (.maybePromote out-pvec from-val-types)
                      acc-type (.getType (.getField out-vec))
                      f (emit-agg from-var from-val-types
                                  (emit-init-group acc-type)
                                  (emit-step acc-type))]
                  (f out-vec in-vec group-mapping))))

            (finish [_] (iv/->direct-vec (.getVector out-pvec)))

            Closeable
            (close [_] (util/try-close out-pvec))))))))

(defmethod ->aggregate-factory :sum [_ ^String from-name, ^String to-name]
  (promotable-agg-factory from-name to-name
    (fn emit-sum-init [acc-type acc-sym group-idx-sym]
      (let [vec-type (types/arrow-type->vector-type acc-type)]
        `(let [~(-> acc-sym (expr/with-tag vec-type)) ~acc-sym]
           (.setIndexDefined ~acc-sym ~group-idx-sym)
           (.set ~acc-sym ~group-idx-sym
                 ~(cond
                    (instance? ArrowType$Int acc-type) 0
                    (instance? ArrowType$FloatingPoint acc-type) 0.0)))))

    (fn emit-sum-step [acc-type var-type acc-sym group-idx-sym val-code]
      ;; TODO `DoubleSummaryStatistics` uses 'Kahan's summation algorithm'
      ;; to compensate for rounding errors - should we?
      (let [{:keys [continue]} (expr/codegen-call*
                                {:op :call, :f :+,
                                 :emitted-args [{:return-types #{acc-type}
                                                 :continue (fn [f]
                                                             (f acc-type (expr/get-value-form var-type acc-sym group-idx-sym)))}
                                                {:return-types #{var-type}
                                                 :continue (fn [f]
                                                             (f var-type val-code))}]})]
        `(let [~(-> acc-sym (expr/with-tag (types/arrow-type->vector-type acc-type))) ~acc-sym]
           ~(continue (fn [arrow-type res-code]
                        (expr/set-value-form arrow-type acc-sym group-idx-sym res-code))))))))

(defmethod ->aggregate-factory :avg [_ ^String from-name, ^String to-name]
  (let [sum-agg (->aggregate-factory :sum from-name "sum")
        count-agg (->aggregate-factory :count from-name "cnt")
        projecter (expr/->expression-projection-spec to-name '(/ (double sum) cnt) '#{sum cnt} {})]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)

      (build [_ al]
        (let [sum-agg (.build sum-agg al)
              count-agg (.build count-agg al)
              res-vec (Float8Vector. to-name al)]
          (reify
            IAggregateSpec
            (getFromColumnName [_] from-name)

            (aggregate [_ in-vec group-mapping]
              (.aggregate sum-agg in-vec group-mapping)
              (.aggregate count-agg in-vec group-mapping))

            (finish [_]
              (let [sum-ivec (.finish sum-agg)
                    count-ivec (.finish count-agg)
                    out-vec (.project projecter al (iv/->indirect-rel [sum-ivec count-ivec]))]
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

(defmethod ->aggregate-factory :variance [_ ^String from-name, ^String to-name]
  (let [avgx-agg (->aggregate-factory :avg from-name "avgx")
        avgx2-agg (->aggregate-factory :avg "x2" "avgx2")
        from-var (symbol from-name)
        x2-projecter (expr/->expression-projection-spec "x2" (list '* from-var from-var) #{from-var} {})
        finish-projecter (expr/->expression-projection-spec to-name '(- avgx2 (* avgx avgx)) '#{avgx2 avgx} {})]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)

      (build [_ al]
        (let [avgx-agg (.build avgx-agg al)
              avgx2-agg (.build avgx2-agg al)
              res-vec (Float8Vector. to-name al)]
          (reify
            IAggregateSpec
            (getFromColumnName [_] from-name)

            (aggregate [_ in-vec group-mapping]
              (with-open [x2 (.project x2-projecter al (iv/->indirect-rel [in-vec]))]
                (.aggregate avgx-agg in-vec group-mapping)
                (.aggregate avgx2-agg x2 group-mapping)))

            (finish [_]
              (let [avgx-ivec (.finish avgx-agg)
                    avgx2-ivec (.finish avgx2-agg)
                    out-ivec (.project finish-projecter al (iv/->indirect-rel [avgx-ivec avgx2-ivec]))]
                (if (instance? NullVector (.getVector out-ivec))
                  out-ivec
                  (do
                    (doto (.makeTransferPair (.getVector out-ivec) res-vec)
                      (.transfer))
                    (iv/->direct-vec res-vec)))))

            Closeable
            (close [_]
              (util/try-close res-vec)
              (util/try-close avgx-agg)
              (util/try-close avgx2-agg))))))))

(defmethod ->aggregate-factory :std-dev [_ ^String from-name, ^String to-name]
  (let [variance-agg (->aggregate-factory :variance from-name "variance")
        finish-projecter (expr/->expression-projection-spec to-name '(sqrt variance) '#{variance} {})]
    (reify IAggregateSpecFactory
      (getToColumnName [_] to-name)

      (build [_ al]
        (let [variance-agg (.build variance-agg al)
              res-vec (Float8Vector. to-name al)]
          (reify
            IAggregateSpec
            (getFromColumnName [_] from-name)

            (aggregate [_ in-vec group-mapping]
              (.aggregate variance-agg in-vec group-mapping))

            (finish [_]
              (let [variance-ivec (.finish variance-agg)
                    out-ivec (.project finish-projecter al (iv/->indirect-rel [variance-ivec]))]
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

(defn- min-max-factory
  "update-if-f-kw: update the accumulated value if `(f el acc)`"
  [from-name to-name update-if-f-kw]

  ;; TODO: this still only works for fixed-width values, but it's reasonable to want (e.g.) `(min <string-col>)`
  (promotable-agg-factory from-name to-name
    (fn emit-min-max-init [_acc-type _acc-sym _group-idx-sym]
      ;; no init here
      )

    (fn emit-min-max-step [acc-type var-type acc-sym group-idx-sym val-code]
      (let [{:keys [continue-call]} (expr/codegen-call {:op :call, :f update-if-f-kw,
                                                        :arg-types [var-type acc-type]})
            val-sym (gensym 'val)]
        `(let [~(-> acc-sym (expr/with-tag (types/arrow-type->vector-type acc-type))) ~acc-sym
               ~val-sym ~val-code]
           (if (.isNull ~acc-sym ~group-idx-sym)
             ~(expr/set-value-form acc-type acc-sym group-idx-sym val-sym)

             ~(continue-call (fn [_arrow-type res-code]
                               `(when ~res-code
                                  ~(expr/set-value-form acc-type acc-sym group-idx-sym val-sym)))
                             [val-sym
                              (expr/get-value-form var-type acc-sym group-idx-sym)])))))))

(defmethod ->aggregate-factory :min [_ from-name to-name] (min-max-factory from-name to-name :<))
(defmethod ->aggregate-factory :max [_ from-name to-name] (min-max-factory from-name to-name :>))

(defn- wrap-distinct [^IAggregateSpecFactory agg-factory]
  (reify IAggregateSpecFactory
    (getToColumnName [_] (.getToColumnName agg-factory))

    (build [_ al]
      (let [agg-spec (.build agg-factory al)
            rel-maps (ArrayList.)
            from-name (.getFromColumnName agg-spec)]
        (reify
          IAggregateSpec
          (getFromColumnName [_] from-name)

          (aggregate [_ in-vec group-mapping]
            (let [builders (ArrayList. (.size rel-maps))
                  distinct-idxs (IntStream/builder)]
              (dotimes [idx (.getValueCount in-vec)]
                (let [group-idx (.get group-mapping idx)]
                  (while (<= (.size rel-maps) group-idx)
                    (.add rel-maps (emap/->relation-map al {:key-col-names [from-name]})))
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
                              (.select in-vec distinct-idxs)
                              (.getVector distinct-gm))))))

          (finish [_] (.finish agg-spec))

          Closeable
          (close [_]
            (util/try-close agg-spec)
            (run! util/try-close rel-maps)))))))

(defmethod ->aggregate-factory :count-distinct [_ from-name to-name]
  (-> (->aggregate-factory :count from-name to-name)
      (wrap-distinct)))

(defmethod ->aggregate-factory :sum-distinct [_ from-name to-name]
  (-> (->aggregate-factory :sum from-name to-name)
      (wrap-distinct)))

(defmethod ->aggregate-factory :avg-distinct [_ from-name to-name]
  (-> (->aggregate-factory :avg from-name to-name)
      (wrap-distinct)))

(deftype ArrayAggAggregateSpec [^BufferAllocator allocator
                                ^String from-name, ^String to-name
                                ^IVectorWriter acc-col
                                ^:unsynchronized-mutable ^ListVector out-vec
                                ^:unsynchronized-mutable ^long base-idx
                                ^List group-idxmaps]
  IAggregateSpec
  (getFromColumnName [_] from-name)

  (aggregate [this in-vec group-mapping]
    (let [row-count (.getValueCount in-vec)]
      (vw/append-vec acc-col in-vec)

      (dotimes [idx row-count]
        (let [group-idx (.get group-mapping idx)]
          (while (<= (.size group-idxmaps) group-idx)
            (.add group-idxmaps (IntStream/builder)))
          (.add ^IntStream$Builder (.get group-idxmaps group-idx)
                (+ base-idx idx))))

      (set! (.base-idx this) (+ base-idx row-count))))

  (finish [this]
    (let [out-vec (-> (types/->field to-name types/list-type false (.getField (.getVector acc-col)))
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

(defmethod ->aggregate-factory :array-agg [_ ^String from-name, ^String to-name]
  (reify IAggregateSpecFactory
    (getToColumnName [_] to-name)

    (build [_ al]
      (ArrayAggAggregateSpec. al from-name to-name
                              (vw/->vec-writer al to-name)
                              nil 0 (ArrayList.)))))

(defmethod ->aggregate-factory :array-agg-distinct [_ from-name to-name]
  (-> (->aggregate-factory :array-agg from-name to-name)
      (wrap-distinct)))

(defn- bool-agg-factory [^String from-name, ^String to-name, zero-value step-f-kw]
  ;; TODO this could be a nullable input?
  (monomorphic-agg-factory from-name to-name #(BitVector. to-name ^BufferAllocator %)
    (fn emit-bool-agg-init [acc-sym group-idx-sym]
      `(let [~(-> acc-sym (expr/with-tag BitVector)) ~acc-sym]
         (.setIndexDefined ~acc-sym ~group-idx-sym)
         ~(expr/set-value-form types/bool-type acc-sym group-idx-sym zero-value)))

    (fn emit-bool-agg-step [var-type acc-sym group-idx-sym val-code]
      (let [{:keys [continue]} (expr/codegen-call* {:op :call, :f step-f-kw
                                                    :emitted-args [{:return-types #{types/null-type types/bool-type}
                                                                    :continue (fn [f]
                                                                                `(if (.isNull ~acc-sym ~group-idx-sym)
                                                                                   ~(f types/null-type nil)
                                                                                   ~(f types/bool-type (expr/get-value-form types/bool-type acc-sym group-idx-sym))))}
                                                                   {:return-types #{var-type}
                                                                    :continue (fn [f]
                                                                                (f var-type val-code))}]})]

        `(let [~(-> acc-sym (expr/with-tag BitVector)) ~acc-sym]
           ~(continue (fn [arrow-type res-code]
                        (if (= arrow-type types/null-type)
                          `(.setNull ~acc-sym ~group-idx-sym)
                          `(do
                             (.setIndexDefined ~acc-sym ~group-idx-sym)
                             ~(expr/set-value-form arrow-type acc-sym group-idx-sym res-code))))))))))

(defmethod ->aggregate-factory :all [_ ^String from-name, ^String to-name] (bool-agg-factory from-name to-name true :and))
(defmethod ->aggregate-factory :every [_ ^String from-name, ^String to-name] (->aggregate-factory :all from-name to-name))
(defmethod ->aggregate-factory :any [_ ^String from-name, ^String to-name] (bool-agg-factory from-name to-name false :or))
(defmethod ->aggregate-factory :some [_ ^String from-name, ^String to-name] (->aggregate-factory :any from-name to-name))

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
                                (let [^IIndirectRelation in-rel in-rel]
                                  (with-open [group-mapping (.groupMapping group-mapper in-rel)]
                                    (doseq [^IAggregateSpec agg-spec aggregate-specs
                                            :let [in-vec (.vectorForName in-rel (.getFromColumnName agg-spec))]]
                                      (.aggregate agg-spec in-vec group-mapping)))))))

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

(defn ->group-by-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor,
                                        ^List #_<String> group-col-names
                                        ^List #_<IAggregateSpecFactory> agg-factories]
  (let [agg-specs (LinkedList.)]
    (try
      (doseq [^IAggregateSpecFactory factory agg-factories]
        (.add agg-specs (.build factory allocator)))

      (GroupByCursor. allocator in-cursor
                      (->group-mapper allocator group-col-names)
                      (vec agg-specs)
                      false)

      (catch Exception e
        (run! util/try-close agg-specs)
        (throw e)))))
