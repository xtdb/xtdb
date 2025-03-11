(ns xtdb.expression.metadata
  (:require [clojure.set :as set]
            [xtdb.bloom :as bloom]
            [xtdb.expression :as expr]
            [xtdb.expression.walk :as ewalk]
            [xtdb.metadata :as meta]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import java.util.function.IntPredicate
           (xtdb.arrow RelationReader VectorReader)
           (xtdb.metadata MetadataPredicate PageMetadata)
           (xtdb.bloom BloomUtils)))

(set! *unchecked-math* :warn-on-boxed)

(defn- simplify-and-or-expr [{:keys [f args] :as expr}]
  (let [args (filterv some? args)]
    (case (count args)
      0 {:op :literal, :literal (case f :and true, :or false)}
      1 (first args)
      (-> expr (assoc :args args)))))

(declare meta-expr)

(def ^:private bool-metadata-types #{:null :bool :fixed-size-binary :transit})

(defn call-meta-expr [{:keys [f args] :as expr} {:keys [col-types] :as opts}]
  (letfn [(var-value-expr [f meta-value field value-type value-expr]
            ;; TODO adapt for boolean metadata writer
            (when-not (contains? bool-metadata-types value-type)
              (let [base-col-types (->> (get col-types field)
                                        types/flatten-union-types)]

                (if (= meta-value :bloom-filter)
                  (let [base-col-types (->> (map types/col-type-head base-col-types) set)]
                    ;; values that are equal according to the EE should hash to the same value
                    ;; TODO add missing col-type hierarchy bits
                    (when (or (and (isa? types/col-type-hierarchy value-type :num)
                                   (seq (set/intersection types/num-types base-col-types)))

                              (and (vector? value-type) (isa? types/col-type-hierarchy (first value-type) :date-time)
                                   (seq (set/intersection types/date-time-types base-col-types)))

                              (contains? base-col-types value-type))
                      {:op :test-metadata,
                       :f f
                       :meta-value meta-value
                       :col-type value-type
                       :field field,
                       :value-expr value-expr
                       :bloom-hash-sym (gensym 'bloom-hashes)}))

                  (simplify-and-or-expr
                   {:op :call
                    :f :or
                    ;; TODO this seems like it could make better use
                    ;; of the polymorphic expr patterns?
                    :args (vec
                           (for [col-type (cond
                                            (isa? types/col-type-hierarchy value-type :num)
                                            (filterv types/num-types base-col-types)

                                            (and (vector? value-type) (isa? types/col-type-hierarchy (first value-type) :date-time))
                                            (filterv (comp types/date-time-types types/col-type-head) base-col-types)

                                            (contains? base-col-types value-type)
                                            [value-type])]

                             {:op :test-metadata,
                              :f f
                              :meta-value meta-value
                              :col-type col-type
                              :field field,
                              :value-expr value-expr}))})))))

          (bool-expr [var-value-f var-value-meta-fn
                      value-var-f value-var-meta-fn]
            (let [[{x-op :op, :as x-arg} {y-op :op, :as y-arg}] args]
              (case [x-op y-op]
                ([:param :param]
                 [:literal :literal]
                 [:literal :param]
                 [:param :literal]) expr

                [:variable :literal] (var-value-expr var-value-f var-value-meta-fn (:variable x-arg)
                                                     (vw/value->col-type (:literal y-arg)) y-arg)

                [:variable :param] (var-value-expr var-value-f var-value-meta-fn (:variable x-arg)
                                                   (:param-type y-arg) y-arg)

                [:literal :variable] (var-value-expr var-value-f var-value-meta-fn (:variable x-arg)
                                                     (vw/value->col-type (:literal y-arg)) y-arg)

                [:param :variable] (var-value-expr value-var-f value-var-meta-fn (:variable y-arg)
                                                   (:param-type x-arg) x-arg)

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
    (for [{:keys [value-expr col-type]} (->> (ewalk/expr-seq expr)
                                             (filter :bloom-hash-sym))]
      (bloom/literal-hashes params value-expr col-type))))

(def ^:private table-metadata-sym (gensym "table-metadata"))
(def ^:private metadata-rdr-sym (gensym "metadata-rdr"))
(def ^:private cols-rdr-sym (gensym "cols-rdr"))
(def ^:private col-rdr-sym (gensym "col-rdr"))
(def ^:private page-idx-sym (gensym "page-idx"))
(def ^:private types-rdr-sym (gensym "types-rdr"))
(def ^:private bloom-rdr-sym (gensym "bloom-rdr"))

(def ^:private content-metadata-present-sym (gensym "content-metadata-present?"))

(defmethod expr/codegen-expr :test-metadata [{:keys [f meta-value field value-expr col-type bloom-hash-sym]} opts]
  (let [field-name (str field)

        idx-code `(.rowIndex ~table-metadata-sym ~field-name ~page-idx-sym)]

    (if (= meta-value :bloom-filter)
      {:return-type :bool
       :continue (fn [cont]
                   (cont :bool
                         `(boolean
                           (let [~expr/idx-sym ~idx-code]
                             (if (>= ~expr/idx-sym 0)
                               (BloomUtils/bloomContains ~bloom-rdr-sym ~expr/idx-sym ~bloom-hash-sym)
                               (not ~content-metadata-present-sym))))))}

      (let [col-sym (gensym 'meta_col)
            col-field (types/col-type->field col-type)

            val-sym (gensym 'val)

            {:keys [continue] :as emitted-expr}
            (expr/codegen-expr {:op :call, :f :boolean
                                :args [{:op :if-some, :local val-sym, :expr {:op :variable, :variable col-sym}
                                        :then {:op :call, :f f
                                               :args [{:op :local, :local val-sym}, value-expr]}
                                        :else {:op :literal, :literal false}}]}
                               (-> opts
                                   (assoc-in [:var->col-type col-sym] (types/merge-col-types col-type :null))))]
        {:return-type :bool
         :batch-bindings [[(-> col-sym (expr/with-tag VectorReader))
                           `(some-> (.keyReader ~types-rdr-sym ~(.getName col-field))
                                    (.keyReader ~(name meta-value)))]]
         :children [emitted-expr]
         :continue (fn [cont]
                     (cont :bool
                           `(if (and ~col-sym ~content-metadata-present-sym)
                              (let [~expr/idx-sym ~idx-code]
                                (when (>= ~expr/idx-sym 0)
                                  ~(continue (fn [_ code] code))))
                              ;; no content-metadata, we can't filter the pages
                              true)))}))))

(defmethod ewalk/walk-expr :test-metadata [inner outer expr]
  (outer (-> expr (update :value-expr inner))))

(defmethod ewalk/direct-child-exprs :test-metadata [{:keys [value-expr]}] #{value-expr})

(def ^:private compile-meta-expr
  (-> (fn [expr opts]
        (let [expr (or (-> expr (expr/prepare-expr) (meta-expr opts) (expr/prepare-expr))
                       (expr/prepare-expr {:op :literal, :literal true}))
              {:keys [continue] :as emitted-expr} (expr/codegen-expr expr opts)]
          {:expr expr
           :f (-> `(fn [~(-> table-metadata-sym (expr/with-tag PageMetadata))
                        ~(-> expr/args-sym (expr/with-tag RelationReader))
                        [~@(keep :bloom-hash-sym (ewalk/expr-seq expr))]]
                     (let [~metadata-rdr-sym (VectorReader/from (.getMetadataLeafReader ~table-metadata-sym))
                           ~cols-rdr-sym (.keyReader ~metadata-rdr-sym "columns")
                           ~col-rdr-sym (.elementReader ~cols-rdr-sym)
                           ~types-rdr-sym (.keyReader ~col-rdr-sym "types")
                           ~bloom-rdr-sym (.keyReader ~col-rdr-sym "bloom")
                           ~content-metadata-present-sym (contains? (.getColumnNames ~table-metadata-sym) "_id")

                           ~@(expr/batch-bindings emitted-expr)]
                       (reify IntPredicate
                         (~'test [_ ~page-idx-sym]
                          ~(continue (fn [_ code] code))))))
                  #_(doto clojure.pprint/pprint)
                  (eval))}))

      (util/lru-memoize)))

(defn ->metadata-selector ^xtdb.metadata.MetadataPredicate [form col-types params]
  (let [params (RelationReader/from params)
        param-types (expr/->param-types params)
        {:keys [expr f]} (compile-meta-expr (expr/form->expr form {:param-types param-types,
                                                                   :col-types col-types})
                                            {:param-types param-types
                                             :col-types col-types
                                             :extract-vecs-from-rel? false})
        bloom-hashes (->bloom-hashes expr params)]
    (reify MetadataPredicate
      (build [_ table-metadata]
        (f table-metadata params bloom-hashes)))))
