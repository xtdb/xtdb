(ns core2.expression.metadata
  (:require [core2.bloom :as bloom]
            [core2.expression :as expr]
            [core2.expression.macro :as emacro]
            [core2.expression.walk :as ewalk]
            [core2.metadata :as meta]
            [core2.types :as types]
            [core2.vector.indirect :as iv])
  (:import core2.metadata.IMetadataIndices
           core2.vector.IIndirectVector
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector VarBinaryVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex StructVector]
           [org.apache.arrow.vector.types.pojo FieldType]
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn- simplify-and-or-expr [{:keys [f args] :as expr}]
  (let [args (filterv some? args)]
    (case (count args)
      0 {:op :literal, :literal (case f and true, or false)}
      1 (first args)
      (-> expr (assoc :args args)))))

(defn- meta-fallback-expr [{:keys [op] :as expr}]
  (case op
    (:literal :param :let) nil
    :variable {:op :metadata-field-present, :field (:variable expr)}
    :if (let [{:keys [pred then else]} expr]
          {:op :if,
           :pred (meta-fallback-expr pred)
           :then (meta-fallback-expr then)
           :else (meta-fallback-expr else)})
    :call (let [{:keys [f args]} expr]
            (-> {:op :call
                 :f (if (= f 'or) 'or 'and)
                 :args (map meta-fallback-expr args)}
                simplify-and-or-expr))))

(declare meta-expr)

(defn call-meta-expr [{:keys [f args] :as expr}]
  (letfn [(var-param-expr [f meta-value field {:keys [param-type] :as param-expr}]
            (simplify-and-or-expr
             {:op :call
              :f 'or
              ;; TODO this seems like it could make better use
              ;; of the polymorphic expr patterns?
              :args (vec (for [col-type (if (isa? types/col-type-hierarchy param-type :num)
                                          [:i64 :f64]
                                          [param-type])]
                           (into {:op :metadata-vp-call,
                                  :f f
                                  :meta-value meta-value
                                  :col-type col-type
                                  :field field,
                                  :param-expr param-expr
                                  :bloom-hash-sym (when (= meta-value :bloom-filter)
                                                    (gensym 'bloom-hashes))})))}))

          (bool-expr [var-param-f var-param-meta-fn
                      param-var-f param-var-meta-fn]
            (let [[{x-op :op, :as x-arg} {y-op :op, :as y-arg}] args]
              (case [x-op y-op]
                [:param :param] expr
                [:variable :param] (var-param-expr var-param-f var-param-meta-fn
                                                   (:variable x-arg) y-arg)
                [:param :variable] (var-param-expr param-var-f param-var-meta-fn
                                                   (:variable y-arg) x-arg)
                nil)))]

    (or (case f
          and (-> {:op :call, :f 'and, :args (map meta-expr args)}
                  simplify-and-or-expr)
          or (-> {:op :call, :f 'or, :args (map meta-expr args)}
                 simplify-and-or-expr)
          < (bool-expr '< :min, '> :max)
          <= (bool-expr '<= :min, '>= :max)
          > (bool-expr '> :max, '< :min)
          >= (bool-expr '>= :max, '<= :min)
          = (-> {:op :call
                 :f 'and
                 :args (->> [(meta-expr {:op :call,
                                         :f 'and,
                                         :args [{:op :call, :f '<=, :args args}
                                                {:op :call, :f '>=, :args args}]})

                             (bool-expr nil :bloom-filter, nil :bloom-filter)]
                            (filterv some?))}
                simplify-and-or-expr)
          nil)

        (meta-fallback-expr expr))))

(defn meta-expr [{:keys [op] :as expr}]
  (case op
    (:literal :param :let) nil
    :variable (meta-fallback-expr expr)
    :if {:op :call
         :f 'and
         :args [(meta-fallback-expr (:pred expr))
                (-> {:op :call
                     :f 'or
                     :args [(meta-expr (:then expr))
                            (meta-expr (:else expr))]}
                    simplify-and-or-expr)]}
    :call (call-meta-expr expr)))

(defn- ->bloom-hashes [expr params]
  (with-open [allocator (RootAllocator.)]
    (vec
     (for [{:keys [param-expr]} (->> (ewalk/expr-seq expr)
                                     (filter :bloom-hash-sym))]
       (bloom/literal-hashes allocator
                             (some-> (or (find param-expr :literal)
                                         (find params (get param-expr :param)))
                                     val))))))

(def ^:private metadata-root-sym (gensym "metadata-root"))
(def ^:private metadata-idxs-sym (gensym "metadata-idxs"))
(def ^:private block-idx-sym (gensym "block-idx"))
(def ^:private types-vec-sym (gensym "types-vec"))
(def ^:private bloom-vec-sym (gensym "bloom-vec"))

(def ^:private metadata-vec-syms
  {:min (gensym "min-vec")
   :max (gensym "max-vec")})

(defmethod expr/codegen-expr :metadata-field-present [{:keys [field]} _]
  (let [field-name (str field)]
    {:return-type :bool
     :continue (fn [f]
                 (f :bool
                    `(boolean
                      (if ~block-idx-sym
                        (.blockIndex ~metadata-idxs-sym ~field-name ~block-idx-sym)
                        (.columnIndex ~metadata-idxs-sym ~field-name)))))}))

(defmethod expr/codegen-expr :metadata-vp-call [{:keys [f meta-value field param-expr col-type bloom-hash-sym]} opts]
  (let [field-name (str field)]
    {:return-type :bool
     :continue
     (fn [cont]
       (cont :bool
             `(boolean
               (when-let [~expr/idx-sym (if ~block-idx-sym
                                          (.blockIndex ~metadata-idxs-sym ~field-name ~block-idx-sym)
                                          (.columnIndex ~metadata-idxs-sym ~field-name))]
                 ~(if (= meta-value :bloom-filter)
                    `(bloom/bloom-contains? ~bloom-vec-sym ~expr/idx-sym ~bloom-hash-sym)

                    (let [vec-sym (get metadata-vec-syms meta-value)
                          col-sym (gensym 'meta-col)
                          col-field (types/col-type->field col-type)
                          arrow-type (.getType col-field)]
                      `(when-let [~(-> vec-sym (expr/with-tag (types/arrow-type->vector-type arrow-type)))
                                  (some-> ^StructVector (.getChild ~types-vec-sym ~(.getName col-field))
                                          (.getChild ~(name meta-value)))]
                         (when-not (.isNull ~vec-sym ~expr/idx-sym)
                           (let [~(-> col-sym (expr/with-tag IIndirectVector)) (iv/->direct-vec ~vec-sym)]
                             ~((:continue (expr/codegen-expr
                                           {:op :call
                                            :f f
                                            :args [{:op :variable, :variable col-sym}
                                                   param-expr]}
                                           (-> opts
                                               ;; HACK remove :var->types (eventually)
                                               (assoc-in [:var->types col-sym] (FieldType/notNullable arrow-type))
                                               (assoc-in [:var->col-type col-sym] col-type))))
                               (fn [_ code] code)))))))))))}))

(defmethod ewalk/walk-expr :metadata-vp-call [inner outer expr]
  (outer (-> expr (update :param-expr inner))))

(defmethod ewalk/direct-child-exprs :metadata-vp-call [{:keys [param-expr]}] #{param-expr})

(defn check-meta [chunk-idx ^IMetadataIndices metadata-idxs check-meta-f]
  (when (check-meta-f nil)
    (let [block-idxs (RoaringBitmap.)]
      (dotimes [block-idx (.blockCount metadata-idxs)]
        (when (check-meta-f block-idx)
          (.add block-idxs block-idx)))

      (when-not (.isEmpty block-idxs)
        (meta/->ChunkMatch chunk-idx block-idxs)))))

(def ^:private compile-meta-expr
  (-> (fn [expr opts]
        (let [expr (->> expr
                        (emacro/macroexpand-all)
                        (ewalk/postwalk-expr (comp #(expr/with-batch-bindings % opts) expr/lit->param))
                        (meta-expr))]
          {:expr expr
           :f (-> `(fn [chunk-idx#
                        ~(-> metadata-root-sym (expr/with-tag VectorSchemaRoot))
                        ~expr/params-sym
                        [~@(keep :bloom-hash-sym (ewalk/expr-seq expr))]]
                     (let [~(-> metadata-idxs-sym (expr/with-tag IMetadataIndices)) (meta/->metadata-idxs ~metadata-root-sym)

                           ~(-> types-vec-sym (expr/with-tag StructVector)) (.getVector ~metadata-root-sym "types")
                           ~(-> bloom-vec-sym (expr/with-tag VarBinaryVector)) (.getVector ~metadata-root-sym "bloom")

                           ~@(expr/batch-bindings expr)]
                       (check-meta chunk-idx# ~metadata-idxs-sym
                                   (fn check-meta# [~block-idx-sym]
                                     ~(let [{:keys [continue]} (expr/codegen-expr expr opts)]
                                        (continue (fn [_ code] code)))))))
                  #_(doto clojure.pprint/pprint)
                  (eval))}))

      ;; TODO passing gensym'd bloom-hash-syms into the memoized fn means we're unlikely to get cache hits
      ;; pass values instead?
      memoize))

(defn ->metadata-selector [form col-names params]
  (let [{:keys [expr f]} (compile-meta-expr (expr/form->expr form {:param-types (expr/->param-types params), :col-names col-names})
                                            (expr/param-opts params))]
    (fn [chunk-idx metadata-root]
      (f chunk-idx metadata-root params (->bloom-hashes expr params)))))
