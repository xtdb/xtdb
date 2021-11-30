(ns core2.expression.metadata
  (:require [core2.bloom :as bloom]
            [core2.expression :as expr]
            [core2.metadata :as meta]
            [core2.types :as types]
            [core2.vector.indirect :as iv])
  (:import clojure.lang.MapEntry
           core2.metadata.IMetadataIndices
           core2.vector.IIndirectVector
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector VarBinaryVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex ListVector StructVector]
           org.apache.arrow.vector.types.pojo.FieldType
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
    (:literal :param) nil
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

(defn call-meta-expr [{:keys [f args] :as expr} param-types]
  (letfn [(var-param-expr [f meta-value field param]
            (simplify-and-or-expr
             {:op :call
              :f 'or
              :args (vec (let [arrow-type (get param-types param)]
                           ;; TODO this seems like it could make better use
                           ;; of the polymorphic expr patterns?
                           (for [arrow-type (if (isa? types/arrow-type-hierarchy arrow-type ::types/Number)
                                              [types/bigint-type types/float8-type]
                                              [arrow-type])]
                             {:op :metadata-vp-call,
                              :f f
                              :meta-value meta-value
                              :arrow-type arrow-type
                              :field field,
                              :param param
                              :bloom-hash-sym (when (= meta-value :bloom-filter)
                                                (gensym 'bloom-hashes))})))}))

          (bool-expr [var-param-f var-param-meta-fn
                      param-var-f param-var-meta-fn]
            (let [[{x-op :op, :as x-arg} {y-op :op, :as y-arg}] args]
              (case [x-op y-op]
                [:param :param] expr
                [:variable :param] (var-param-expr var-param-f var-param-meta-fn
                                                   (:variable x-arg) (:param y-arg))
                [:param :variable] (var-param-expr param-var-f param-var-meta-fn
                                                   (:variable y-arg) (:param x-arg))
                nil)))]

    (or (case f
          and (-> {:op :call, :f 'and, :args (map #(meta-expr % param-types) args)}
                  simplify-and-or-expr)
          or (-> {:op :call, :f 'or, :args (map #(meta-expr % param-types) args)}
                 simplify-and-or-expr)
          < (bool-expr '< :min, '> :max)
          <= (bool-expr '<= :min, '>= :max)
          > (bool-expr '> :max, '< :min)
          >= (bool-expr '>= :max, '<= :min)
          = {:op :call
             :f 'and
             :args [(meta-expr {:op :call,
                                :f 'and,
                                :args [{:op :call, :f '<=, :args args}
                                       {:op :call, :f '>=, :args args}]}
                               param-types)

                    (bool-expr nil :bloom-filter, nil :bloom-filter)]}
          nil)

        (meta-fallback-expr expr))))

(defn- meta-expr [{:keys [op] :as expr} param-types]
  (case op
    (:literal :param) nil
    :variable (meta-fallback-expr expr)
    :if {:op :call
         :f 'and
         :args [(meta-fallback-expr (:pred expr))
                (-> {:op :call
                     :f 'or
                     :args [(meta-expr (:then expr) param-types)
                            (meta-expr (:else expr) param-types)]}
                    simplify-and-or-expr)]}
    :call (call-meta-expr expr param-types)))

(defn- ->bloom-hashes [expr params]
  (with-open [allocator (RootAllocator.)]
    (let [bloom-hashes (for [{:keys [bloom-hash-sym param]} (expr/expr-seq expr)
                             :when bloom-hash-sym]
                         (MapEntry/create bloom-hash-sym
                                          (bloom/literal-hashes allocator
                                                                (get params param))))]
      {:bloom-hash-syms (map key bloom-hashes)
       :bloom-hashes (map val bloom-hashes)})))

(def ^:private metadata-root-sym (gensym "metadata-root"))
(def ^:private metadata-idxs-sym (gensym "metadata-idxs"))
(def ^:private block-idx-sym (gensym "block-idx"))

(def ^:private metadata-vec-syms
  {:min (gensym "min-vec")
   :max (gensym "max-vec")
   :bloom (gensym "bloom-vec")})

(defmethod expr/codegen-expr :metadata-field-present [{:keys [field]} _]
  (let [field-name (str field)]
    {:continue (fn [f]
                 (f types/bool-type
                    `(boolean
                      (if ~block-idx-sym
                        (.blockIndex ~metadata-idxs-sym ~field-name ~block-idx-sym)
                        (.columnIndex ~metadata-idxs-sym ~field-name)))))
     :return-types #{types/bool-type}}))

(defmethod expr/codegen-expr :metadata-vp-call [{:keys [f meta-value field param arrow-type bloom-hash-sym]} opts]
  (let [field-name (str field)]
    {:return-types #{types/bool-type}
     :continue
     (fn [cont]
       (cont types/bool-type
             `(boolean
               (when-let [~expr/idx-sym (if ~block-idx-sym
                                          (.blockIndex ~metadata-idxs-sym ~field-name ~block-idx-sym)
                                          (.columnIndex ~metadata-idxs-sym ~field-name))]
                 ~(if (= meta-value :bloom-filter)
                    `(bloom/bloom-contains? ~(:bloom metadata-vec-syms) ~expr/idx-sym ~bloom-hash-sym)

                    (let [vec-sym (get metadata-vec-syms meta-value)
                          col-sym (gensym 'meta-col)]
                      `(when-let [~(-> vec-sym (expr/with-tag (types/arrow-type->vector-type arrow-type)))
                                  (.getChild ~vec-sym ~(types/type->field-name arrow-type))]
                         (when-not (.isNull ~vec-sym ~expr/idx-sym)
                           (let [~(-> col-sym (expr/with-tag IIndirectVector)) (iv/->direct-vec ~vec-sym)]
                             ~((:continue (expr/codegen-expr
                                           {:op :call
                                            :f f
                                            :args [{:op :variable, :variable col-sym}
                                                   {:op :param, :param param}]}
                                           (-> opts
                                               (assoc-in [:var->types col-sym] (FieldType. false arrow-type nil)))))
                               (fn [_ code] code)))))))))))}))

(defn check-meta [chunk-idx ^VectorSchemaRoot metadata-root ^IMetadataIndices metadata-idxs check-meta-f]
  (when (check-meta-f (.getVector metadata-root "min")
                      (.getVector metadata-root "max")
                      (.getVector metadata-root "bloom")
                      nil)
    (let [block-idxs (RoaringBitmap.)
          ^ListVector blocks-vec (.getVector metadata-root "blocks")
          ^StructVector blocks-data-vec (.getDataVector blocks-vec)

          blocks-min-vec (.getChild blocks-data-vec "min")
          blocks-max-vec (.getChild blocks-data-vec "max")
          blocks-bloom-vec (.getChild blocks-data-vec "bloom")]

      (dotimes [block-idx (.blockCount metadata-idxs)]
        (when (check-meta-f blocks-min-vec blocks-max-vec blocks-bloom-vec block-idx)
          (.add block-idxs block-idx)))

      (when-not (.isEmpty block-idxs)
        (meta/->ChunkMatch chunk-idx block-idxs)))))

(def ^:private compile-meta-expr
  (-> (fn [expr param-types bloom-hash-syms]
        (eval
         `(fn [chunk-idx#
               ~(-> metadata-root-sym (expr/with-tag VectorSchemaRoot))
               [~@(map key param-types)]
               [~@bloom-hash-syms]]
            (let [~(-> metadata-idxs-sym (expr/with-tag IMetadataIndices)) (meta/->metadata-idxs ~metadata-root-sym)]
              (check-meta chunk-idx# ~metadata-root-sym ~metadata-idxs-sym
                          (fn check-meta# [~(-> (:min metadata-vec-syms) (expr/with-tag StructVector))
                                           ~(-> (:max metadata-vec-syms) (expr/with-tag StructVector))
                                           ~(-> (:bloom metadata-vec-syms) (expr/with-tag VarBinaryVector))
                                           ~block-idx-sym]
                            ~(let [{:keys [continue]} (expr/codegen-expr expr {:param->type param-types})]
                               (continue (fn [_ code] code)))))))))

      ;; TODO passing gensym'd bloom-hash-syms into the memoized fn means we're unlikely to get cache hits
      ;; pass values instead?
      memoize))

(defn ->metadata-selector [form params]
  (let [{:keys [expr param-types params emitted-params]} (-> (expr/form->expr form {:params params})
                                                             (expr/normalise-params params))
        meta-expr (meta-expr expr (into {} param-types))
        {:keys [bloom-hash-syms bloom-hashes]} (->bloom-hashes meta-expr params)
        f (compile-meta-expr meta-expr (into {} param-types) bloom-hash-syms)]
    (fn [chunk-idx metadata-root]
      (f chunk-idx metadata-root (vals emitted-params) bloom-hashes))))
