(ns xtdb.expression.metadata
  (:require [xtdb.bloom :as bloom]
            [xtdb.expression :as expr]
            [xtdb.expression.walk :as ewalk]
            [xtdb.metadata :as meta]
            [xtdb.util :as util]
            [xtdb.types :as types]
            [xtdb.vector.indirect :as iv])
  (:import (xtdb.metadata IMetadataPredicate ITableMetadata)
           (xtdb.vector IIndirectRelation IIndirectVector)
           java.util.function.IntPredicate
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector VarBinaryVector]
           [org.apache.arrow.vector.complex StructVector]))

(set! *unchecked-math* :warn-on-boxed)

(defn- simplify-and-or-expr [{:keys [f args] :as expr}]
  (let [args (filterv some? args)]
    (case (count args)
      0 {:op :literal, :literal (case f :and true, :or false)}
      1 (first args)
      (-> expr (assoc :args args)))))

(defn- meta-fallback-expr [{:keys [op] :as expr}]
  (case op
    (:literal :param :local) nil ;; expected to be filtered out by the caller, using simplify-and-or-expr
    :variable {:op :metadata-field-present, :field (:variable expr)}

    :if (let [{:keys [pred then else]} expr]
          (-> {:op :call, :f :and
               :args [(meta-fallback-expr pred)
                      (-> {:op :call, :f :or
                           :args [(meta-fallback-expr then)
                                  (meta-fallback-expr else)]}
                          simplify-and-or-expr)]}
              simplify-and-or-expr))

    :if-some (let [{:keys [local expr then else]} expr]
               (recur {:op :let, :local local, :expr expr,
                       :body {:op :if, :pred {:op :call, :f :nil?
                                              :args [{:op :local, :local local}]}
                              :then else
                              :else then}}))

    :let (let [{:keys [expr body]} expr]
           (-> {:op :call, :f :and
                :args [(meta-fallback-expr expr)
                       (meta-fallback-expr body)]}
               simplify-and-or-expr))

    :call (let [{:keys [f args]} expr]
            (-> {:op :call
                 :f (if (= f :or) :or :and)
                 :args (map meta-fallback-expr args)}
                simplify-and-or-expr))))

(declare meta-expr)

(defn call-meta-expr [{:keys [f args] :as expr}]
  (letfn [(var-param-expr [f meta-value field {:keys [param-type] :as param-expr}]
            (simplify-and-or-expr
             {:op :call
              :f :or
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
          :and (-> {:op :call, :f :and, :args (map meta-expr args)}
                   simplify-and-or-expr)
          :or (-> {:op :call, :f :or, :args (map meta-expr args)}
                  simplify-and-or-expr)
          :< (bool-expr :< :min, :> :max)
          :<= (bool-expr :<= :min, :>= :max)
          :> (bool-expr :> :max, :< :min)
          :>= (bool-expr :>= :max, :<= :min)
          := (-> {:op :call
                  :f :and
                  :args (->> [(meta-expr {:op :call,
                                          :f :and,
                                          :args [{:op :call, :f :<=, :args args}
                                                 {:op :call, :f :>=, :args args}]})

                              (bool-expr nil :bloom-filter, nil :bloom-filter)]
                             (filterv some?))}
                 simplify-and-or-expr)
          nil)

        (meta-fallback-expr expr))))

(defn meta-expr [{:keys [op] :as expr}]
  (case op
    (:literal :param :let) nil ;; expected to be filtered out by the caller, using simplify-and-or-expr
    :variable (meta-fallback-expr expr)
    :if (-> {:op :call
             :f :and
             :args [(meta-fallback-expr (:pred expr))
                    (-> {:op :call
                         :f :or
                         :args [(meta-expr (:then expr))
                                (meta-expr (:else expr))]}
                        simplify-and-or-expr)]}
            simplify-and-or-expr)
    :call (call-meta-expr expr)))

(defn- ->bloom-hashes [expr ^IIndirectRelation params]
  (with-open [allocator (RootAllocator.)]
    (vec
     (for [{:keys [param-expr]} (->> (ewalk/expr-seq expr)
                                     (filter :bloom-hash-sym))]
       (bloom/literal-hashes allocator
                             (if-let [[_ literal] (find param-expr :literal)]
                               literal
                               (when-let [param-col (.vectorForName params (name (get param-expr :param)))]
                                 (types/get-object (.getVector param-col) (.getIndex param-col 0)))))))))

(def ^:private table-metadata-sym (gensym "table-metadata"))
(def ^:private metadata-root-sym (gensym "metadata-root"))
(def ^:private block-idx-sym (gensym "block-idx"))
(def ^:private types-vec-sym (gensym "types-vec"))
(def ^:private bloom-vec-sym (gensym "bloom-vec"))

(defmethod expr/codegen-expr :metadata-field-present [{:keys [field]} _]
  (let [field-name (str field)]
    {:return-type :bool
     :continue (fn [f]
                 (f :bool
                    `(boolean
                      (.rowIndex ~table-metadata-sym ~field-name ~block-idx-sym))))}))

(defmethod expr/codegen-expr :metadata-vp-call [{:keys [f meta-value field param-expr col-type bloom-hash-sym]} opts]
  (let [field-name (str field)

        idx-code `(.rowIndex ~table-metadata-sym ~field-name ~block-idx-sym)]

    (if (= meta-value :bloom-filter)
      {:return-type :bool
       :continue (fn [cont]
                   (cont :bool
                         `(boolean
                           (when-let [~expr/idx-sym ~idx-code]
                             (bloom/bloom-contains? ~bloom-vec-sym ~expr/idx-sym ~bloom-hash-sym)))))}

      (let [col-sym (gensym 'meta-col)
            col-field (types/col-type->field col-type)

            val-sym (gensym 'val)

            {:keys [continue] :as emitted-expr}
            (expr/codegen-expr {:op :call, :f :boolean
                                :args [{:op :if-some, :local val-sym, :expr {:op :variable, :variable col-sym}
                                        :then {:op :call, :f f
                                               :args [{:op :local, :local val-sym}, param-expr]}
                                        :else {:op :literal, :literal false}}]}
                               (-> opts
                                   (assoc-in [:var->col-type col-sym] (types/merge-col-types col-type :null))))]
        {:return-type :bool
         :batch-bindings [[(-> col-sym (expr/with-tag IIndirectVector))
                           `(some-> ^StructVector (.getChild ~types-vec-sym ~(.getName col-field))
                                    (.getChild ~(name meta-value))
                                    iv/->direct-vec)]]
         :children [emitted-expr]
         :continue (fn [cont]
                     (cont :bool
                           `(when ~col-sym
                              (when-let [~expr/idx-sym ~idx-code]
                                ~(continue (fn [_ code] code))))))}))))

(defmethod ewalk/walk-expr :metadata-vp-call [inner outer expr]
  (outer (-> expr (update :param-expr inner))))

(defmethod ewalk/direct-child-exprs :metadata-vp-call [{:keys [param-expr]}] #{param-expr})

(def ^:private compile-meta-expr
  (-> (fn [expr opts]
        (let [expr (or (meta-expr (expr/prepare-expr expr))
                       (expr/prepare-expr {:op :literal, :literal true}))
              {:keys [continue] :as emitted-expr} (expr/codegen-expr expr opts)]
          {:expr expr
           :f (-> `(fn [~(-> table-metadata-sym (expr/with-tag ITableMetadata))
                        ~(-> expr/params-sym (expr/with-tag IIndirectRelation))
                        [~@(keep :bloom-hash-sym (ewalk/expr-seq expr))]]
                     (let [~metadata-root-sym (.metadataRoot ~table-metadata-sym)
                           ~(-> types-vec-sym (expr/with-tag StructVector)) (.getVector ~metadata-root-sym "types")
                           ~(-> bloom-vec-sym (expr/with-tag VarBinaryVector)) (.getVector ~metadata-root-sym "bloom")

                           ~@(expr/batch-bindings emitted-expr)]
                       (reify IntPredicate
                         (~'test [_ ~block-idx-sym]
                          (boolean ~(continue (fn [_ code] code)))))))
                  #_(doto clojure.pprint/pprint)
                  (eval))}))

      ;; TODO passing gensym'd bloom-hash-syms into the memoized fn means we're unlikely to get cache hits
      ;; pass values instead?
      (util/lru-memoize)))

(defn ->metadata-selector [form col-types params]
  (let [param-types (expr/->param-types params)
        {:keys [expr f]} (compile-meta-expr (expr/form->expr form {:param-types param-types,
                                                                   :col-types col-types})
                                            {:param-types param-types
                                             :extract-vecs-from-rel? false})
        bloom-hashes (->bloom-hashes expr params)]
    (reify IMetadataPredicate
      (build [_ table-metadata]
        (f table-metadata params bloom-hashes)))))
