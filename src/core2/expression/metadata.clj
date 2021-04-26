(ns core2.expression.metadata
  (:require [core2.bloom :as bloom]
            [core2.expression :as expr]
            [core2.metadata :as meta]
            [core2.types :as types])
  (:import core2.metadata.IMetadataIndices
           java.util.HashMap
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector VarBinaryVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex ListVector StructVector]
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
    :literal nil
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
  (letfn [(var-lit-expr [f meta-value field literal-arg]
            (simplify-and-or-expr
             {:op :call
              :f 'or
              :args (vec (let [{literal-code :code, field-type :return-type} (expr/codegen-expr literal-arg {})]
                           (for [field-type (if (.isAssignableFrom Number field-type)
                                              expr/numeric-types
                                              [field-type])]
                             {:op :metadata-var-lit-call,
                              :f f
                              :meta-value meta-value
                              :field-type field-type
                              :field field,
                              :literal literal-arg})))}))

          (bool-expr [var-lit-f var-lit-meta-fn
                      lit-var-f lit-var-meta-fn]
            (let [[{x-op :op, :as x-arg} {y-op :op, :as y-arg}] args]
              (case [x-op y-op]
                [:literal :literal] expr
                [:variable :literal] (var-lit-expr var-lit-f var-lit-meta-fn
                                                   (:variable x-arg) y-arg)
                [:literal :variable] (var-lit-expr lit-var-f lit-var-meta-fn
                                                   (:variable y-arg) x-arg)
                nil)))]

    (or (case f
          and (-> {:op :call, :f 'and, :args (map meta-expr args)} simplify-and-or-expr)
          or (-> {:op :call, :f 'or, :args (map meta-expr args)} simplify-and-or-expr)
          < (bool-expr '< :min, '> :max)
          <= (bool-expr '<= :min, '>= :max)
          > (bool-expr '> :max, '< :min)
          >= (bool-expr '>= :max, '<= :min)
          = {:op :call
             :f 'and
             :args [(meta-expr {:op :call,
                                :f 'and,
                                :args [{:op :call, :f '<=, :args args}
                                       {:op :call, :f '>=, :args args}]})

                    (bool-expr nil :bloom-filter, nil :bloom-filter)]}
          nil)

        (meta-fallback-expr expr))))

(defn- meta-expr [{:keys [op] :as expr}]
  (expr/expand-variadics
   (case op
     :literal nil
     :variable (meta-fallback-expr expr)
     :if {:op :call
          :f 'and
          :args [(meta-fallback-expr (:pred expr))
                 (-> {:op :call
                      :f 'or
                      :args [(meta-expr (:then expr))
                             (meta-expr (:else expr))]}
                     simplify-and-or-expr)]}
     :call (call-meta-expr expr))))

(def ^:private metadata-root-sym (gensym "metadata-root"))
(def ^:private metadata-idxs-sym (gensym "metadata-idxs"))
(def ^:private block-idx-sym (gensym "block-idx"))

(def ^:private metadata-vec-syms
  {:min (gensym "min-vec")
   :max (gensym "max-vec")
   :bloom (gensym "bloom-vec")})

(defmethod expr/codegen-expr :metadata-field-present [{:keys [field]} _]
  (let [field-name (str field)]
    {:code `(boolean
             (if ~block-idx-sym
               (.blockIndex ~metadata-idxs-sym ~field-name ~block-idx-sym)
               (.columnIndex ~metadata-idxs-sym ~field-name)))
     :return-type Boolean}))

(defmethod expr/codegen-expr :metadata-var-lit-call [{:keys [f meta-value field literal field-type]} {:keys [allocator] :as opts}]
  (let [field-name (str field)]
    {:code `(boolean
             (when-let [~expr/idx-sym (if ~block-idx-sym
                                        (.blockIndex ~metadata-idxs-sym ~field-name ~block-idx-sym)
                                        (.columnIndex ~metadata-idxs-sym ~field-name))]
               ~(if (= meta-value :bloom-filter)
                  `(bloom/bloom-contains? ~(:bloom metadata-vec-syms)
                                          ~expr/idx-sym
                                          ~(let [^ints hashes (bloom/literal-hashes allocator (:literal literal))]
                                             `(doto (int-array ~(alength hashes))
                                                ~@(for [[idx h] (map-indexed vector hashes)]
                                                    `(aset ~idx ~h)))))

                  (let [arrow-type (types/->arrow-type field-type)
                        vec-sym (get metadata-vec-syms meta-value)
                        variable-code (expr/codegen-expr
                                       {:op :variable, :variable vec-sym}
                                       {:var->type {vec-sym field-type}})]
                    `(let [~(-> vec-sym (expr/with-tag (types/arrow-type->vector-type arrow-type)))
                           (.getChild ~vec-sym ~(meta/type->field-name arrow-type))]
                       (when-not (.isNull ~vec-sym ~expr/idx-sym)
                         ~(:code (expr/codegen-expr
                                  {:op :call
                                   :f f
                                   :args [(if-let [cast (expr/type->cast field-type)]
                                            {:code (list cast (:code variable-code)),
                                             :return-type field-type}
                                            variable-code)
                                          (expr/codegen-expr literal nil)]}
                                  opts))))))))
     :return-type Boolean}))

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

(defn meta-expr->code [expr]
  (with-open [allocator (RootAllocator.)]
    `(fn [chunk-idx# ~(-> metadata-root-sym (expr/with-tag VectorSchemaRoot))]
       (let [~@(expr/init-expressions expr)
             ~(-> metadata-idxs-sym (expr/with-tag IMetadataIndices)) (meta/->metadata-idxs ~metadata-root-sym)]
         (check-meta chunk-idx# ~metadata-root-sym ~metadata-idxs-sym
                     (fn check-meta# [~(-> (:min metadata-vec-syms) (expr/with-tag StructVector))
                                      ~(-> (:max metadata-vec-syms) (expr/with-tag StructVector))
                                      ~(-> (:bloom metadata-vec-syms) (expr/with-tag VarBinaryVector))
                                      ~block-idx-sym]
                       ~(:code (expr/postwalk-expr #(expr/codegen-expr % {:allocator allocator}) expr))))))))

(def memo-meta-expr->code
  (memoize meta-expr->code))

(def ^:private memo-eval (memoize eval))

(defn ->metadata-selector [expr]
  (-> expr
      (meta-expr)
      (memo-meta-expr->code)
      (memo-eval)))
