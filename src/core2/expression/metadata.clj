(ns core2.expression.metadata
  (:require [core2.bloom :as bloom]
            [core2.expression :as expr]
            [core2.types :as types])
  (:import org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector]
           org.apache.arrow.vector.VectorSchemaRoot
           org.apache.arrow.vector.BitVector))

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
                              :literal literal-code})))}))

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

(def ^:private metadata-vec-syms
  {:min (gensym "min-vec")
   :max (gensym "max-vec")
   :bloom (gensym "bloom-vec")
   :bloom-bits (gensym "bloom-bits-vec")})

(defn field-present? [metadata-root field]
  ;; TODO this goes to a nested select but we could also find a way
  ;; to inline the generated code
  (not (.isEmpty (.select (expr/->expression-root-selector (expr/form->expr (list '= 'field (str field))))
                          metadata-root))))

(defmethod expr/codegen-expr :metadata-field-present [{:keys [field]} _]
  {:code `(field-present? ~metadata-root-sym '~field)
   :return-type Boolean})

(defn metadata-field-idx [metadata-root column field type-id]
  (let [bitmap (.select (expr/->expression-root-selector
                         (expr/form->expr
                          (list 'and
                                (list '= 'column (str column))
                                (list '= 'field (str field))
                                (list '= 'type-id type-id))))
                        metadata-root)]
    (when-not (.isEmpty bitmap)
      (.first bitmap))))

(defmethod expr/codegen-expr :metadata-var-lit-call [{:keys [f meta-value field literal field-type]} {:keys [allocator] :as opts}]
  (let [arrow-type (types/->arrow-type field-type)]
    (if (= meta-value :bloom-filter)
      {:code `(boolean
               (when-let [~expr/idx-sym (metadata-field-idx ~metadata-root-sym '~field '~field
                                                            ~(types/arrow-type->type-id arrow-type))]
                 (bloom/bloom-contains? ~(:bloom metadata-vec-syms)
                                        ~(:bloom-bits metadata-vec-syms)
                                        ~expr/idx-sym
                                        ~(bloom/literal-hashes allocator literal))))
       :return-type Boolean}

      (let [vl-sym (gensym 'vl)
            vec-sym (get metadata-vec-syms meta-value)]
        {:code `(boolean
                 (when-let [duv-idx# (metadata-field-idx ~metadata-root-sym '~field '~field
                                                         ~(types/arrow-type->type-id arrow-type))]
                   (let [~expr/idx-sym (.getOffset ~vec-sym duv-idx#)

                         ~(-> vec-sym (expr/with-tag (types/arrow-type->vector-type arrow-type)))
                         (.getVectorByType ~vec-sym ~(types/arrow-type->type-id arrow-type))]
                     (when-let [~vl-sym ~(:code (expr/codegen-expr
                                                 {:op :variable, :variable vec-sym}
                                                 {:var->type {vec-sym field-type}}))]
                       ~(:code (expr/codegen-expr
                                {:op :call
                                 :f f
                                 :args [{:code (list (expr/type->cast field-type) vl-sym),
                                         :return-type field-type}
                                        (expr/codegen-expr {:op :literal, :literal literal} nil)]}
                                opts))))))
         :return-type Boolean}))))

(defn meta-expr->code [expr]
  (with-open [allocator (RootAllocator.)]
    `(fn [~(-> metadata-root-sym (expr/with-tag VectorSchemaRoot))]
       (let [~(-> (:min metadata-vec-syms) (expr/with-tag DenseUnionVector))
             (.getVector ~metadata-root-sym "min")

             ~(-> (:max metadata-vec-syms) (expr/with-tag DenseUnionVector))
             (.getVector ~metadata-root-sym "max")

             ~(-> (:bloom metadata-vec-syms) (expr/with-tag FixedSizeListVector))
             (.getVector ~metadata-root-sym "bloom")

             ~(-> (:bloom-bits metadata-vec-syms) (expr/with-tag BitVector))
             (.getDataVector ~(:bloom metadata-vec-syms))]
         ~(:code (expr/postwalk-expr #(expr/codegen-expr % {:allocator allocator}) expr))))))

(def memo-meta-expr->code
  (memoize meta-expr->code))

(def ^:private memo-eval (memoize eval))

(defn ->metadata-selector [expr]
  (-> expr
      (meta-expr)
      (memo-meta-expr->code)
      (memo-eval)))
