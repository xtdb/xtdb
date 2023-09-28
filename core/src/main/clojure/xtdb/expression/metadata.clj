(ns xtdb.expression.metadata
  (:require [xtdb.bloom :as bloom]
            [xtdb.expression :as expr]
            [xtdb.expression.walk :as ewalk]
            [xtdb.metadata :as meta]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import java.util.function.IntPredicate
           (xtdb.metadata IMetadataPredicate ITableMetadata)
           (xtdb.vector IVectorReader RelationReader)))

(set! *unchecked-math* :warn-on-boxed)

(defn- simplify-and-or-expr [{:keys [f args] :as expr}]
  (let [args (filterv some? args)]
    (case (count args)
      0 {:op :literal, :literal (case f :and true, :or false)}
      1 (first args)
      (-> expr (assoc :args args)))))

(declare meta-expr)

(def ^:private bool-metadata-types #{:null :bool :fixed-size-binary :clj-form})

(defn call-meta-expr [{:keys [f args] :as expr} {:keys [col-types] :as opts}]
  (letfn [(var-param-expr [f meta-value field {:keys [param-type] :as param-expr}]
            ;; TODO adapt for boolean metadata writer
            (when-not (contains? bool-metadata-types param-type)
              (let [base-col-types (-> (get col-types field)
                                       types/flatten-union-types)]
                (simplify-and-or-expr
                 {:op :call
                  :f :or
                  ;; TODO this seems like it could make better use
                  ;; of the polymorphic expr patterns?
                  :args (vec
                         (for [col-type (cond
                                          (isa? types/col-type-hierarchy param-type :num)
                                          (filterv types/num-types base-col-types)

                                          (and (vector? param-type) (isa? types/col-type-hierarchy (first param-type) :date-time))
                                          (filterv (comp types/date-time-types first) base-col-types)

                                          (contains? base-col-types param-type)
                                          [param-type])]

                           {:op :metadata-vp-call,
                            :f f
                            :meta-value meta-value
                            :col-type col-type
                            :field field,
                            :param-expr param-expr
                            :bloom-hash-sym (when (= meta-value :bloom-filter)
                                              (gensym 'bloom-hashes))}))}))))

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
          :and (-> {:op :call, :f :and, :args (map #(meta-expr % opts) args)}
                   simplify-and-or-expr)
          :or (-> {:op :call, :f :or, :args (map #(meta-expr % opts) args)}
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
                                                 {:op :call, :f :>=, :args args}]}
                                         opts)

                              (bool-expr nil :bloom-filter, nil :bloom-filter)]
                             (filterv some?))}
                 simplify-and-or-expr)
          nil)

        ;; we can't check this call at the metadata level, have to pull the block and look.
        {:op :literal, :literal true})))

(defn meta-expr [{:keys [op] :as expr} opts]
  (case op
    (:literal :param :let) nil ;; expected to be filtered out by the caller, using simplify-and-or-expr
    :variable {:op :literal, :literal true}
    :if (-> {:op :call
             :f :or
             :args [(meta-expr (:then expr) opts)
                    (meta-expr (:else expr) opts)]}
            simplify-and-or-expr)
    :call (call-meta-expr expr opts)))

(defn- ->bloom-hashes [expr ^RelationReader params]
  (vec
    (for [{:keys [param-expr col-type]} (->> (ewalk/expr-seq expr)
                                             (filter :bloom-hash-sym))]
      (bloom/literal-hashes params param-expr col-type))))

(def ^:private table-metadata-sym (gensym "table-metadata"))
(def ^:private metadata-rdr-sym (gensym "metadata-rdr"))
(def ^:private cols-rdr-sym (gensym "cols-rdr"))
(def ^:private col-rdr-sym (gensym "col-rdr"))
(def ^:private page-idx-sym (gensym "page-idx"))
(def ^:private types-rdr-sym (gensym "types-rdr"))
(def ^:private bloom-rdr-sym (gensym "bloom-rdr"))


(defmethod expr/codegen-expr :metadata-vp-call [{:keys [f meta-value field param-expr col-type bloom-hash-sym]} opts]
  (let [field-name (util/str->normal-form-str (str field))

        idx-code `(.rowIndex ~table-metadata-sym ~field-name ~page-idx-sym)]

    (if (= meta-value :bloom-filter)
      {:return-type :bool
       :continue (fn [cont]
                   (cont :bool
                         `(boolean
                           (when-let [~expr/idx-sym ~idx-code]
                             (bloom/bloom-contains? ~bloom-rdr-sym ~expr/idx-sym ~bloom-hash-sym)))))}

      (let [col-sym (gensym 'meta_col)
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
         :batch-bindings [[(-> col-sym (expr/with-tag IVectorReader))
                           `(some-> (.structKeyReader ~types-rdr-sym ~(.getName col-field))
                                    (.structKeyReader ~(name meta-value)))]]
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
        (let [expr (or (-> expr (expr/prepare-expr) (meta-expr opts) (expr/prepare-expr))
                       (expr/prepare-expr {:op :literal, :literal true}))
              {:keys [continue] :as emitted-expr} (expr/codegen-expr expr opts)]
          {:expr expr
           :f (-> `(fn [~(-> table-metadata-sym (expr/with-tag ITableMetadata))
                        ~(-> expr/params-sym (expr/with-tag RelationReader))
                        [~@(keep :bloom-hash-sym (ewalk/expr-seq expr))]]
                     (let [~metadata-rdr-sym (.metadataReader ~table-metadata-sym)
                           ~(-> cols-rdr-sym (expr/with-tag IVectorReader)) (.structKeyReader ~metadata-rdr-sym "columns")
                           ~(-> col-rdr-sym (expr/with-tag IVectorReader)) (.listElementReader ~cols-rdr-sym)
                           ~(-> types-rdr-sym (expr/with-tag IVectorReader)) (.structKeyReader ~col-rdr-sym "types")
                           ~(-> bloom-rdr-sym (expr/with-tag IVectorReader)) (.structKeyReader ~col-rdr-sym "bloom")

                           ~@(expr/batch-bindings emitted-expr)]
                       (reify IntPredicate
                         (~'test [_ ~page-idx-sym]
                          (boolean ~(continue (fn [_ code] code)))))))
                  ;; (doto clojure.pprint/pprint)
                  (eval))}))

      (util/lru-memoize)))

(defn ->metadata-selector ^xtdb.metadata.IMetadataPredicate [form col-types params]
  (let [param-types (expr/->param-types params)
        {:keys [expr f]} (compile-meta-expr (expr/form->expr form {:param-types param-types,
                                                                   :col-types col-types})
                                            {:param-types param-types
                                             :col-types col-types
                                             :extract-vecs-from-rel? false})
        bloom-hashes (->bloom-hashes expr params)]
    (reify IMetadataPredicate
      (build [_ table-metadata]
        (f table-metadata params bloom-hashes)))))
