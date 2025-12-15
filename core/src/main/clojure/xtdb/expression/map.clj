(ns xtdb.expression.map
  (:require [xtdb.expression :as expr]
            [xtdb.expression.walk :as ewalk]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (java.util Map)
           java.util.function.IntBinaryOperator
           (xtdb.arrow RelationReader VectorReader)))

(def ^:private left-rel (gensym 'left-rel))
(def ^:private left-vec (gensym 'left-vec))
(def ^:private left-idx (gensym 'left-idx))

(def ^:private right-rel (gensym 'right-rel))
(def ^:private right-vec (gensym 'right-vec))
(def ^:private right-idx (gensym 'right-idx))

(def build-comparator
  (-> (fn [expr input-opts]
        (let [{:keys [vec-fields param-fields]} input-opts
              input-opts {:var->col-type (update-vals vec-fields types/field->col-type)
                          :param-types (update-vals param-fields types/->type)}
                          
              {:keys [continue], :as emitted-expr}
              (expr/codegen-expr expr input-opts)]

          (-> `(fn [~(expr/with-tag left-rel RelationReader)
                    ~(expr/with-tag right-rel RelationReader)
                    ~(-> expr/schema-sym (expr/with-tag Map))
                    ~(-> expr/args-sym (expr/with-tag RelationReader))]
                 (let [~@(expr/batch-bindings emitted-expr)]
                   (reify IntBinaryOperator
                     (~'applyAsInt [_# ~left-idx ~right-idx]
                      ~(continue (fn [res-type code]
                                   (case res-type
                                     :null 0
                                     :bool `(if ~code 1 -1))))))))

              #_(doto clojure.pprint/pprint)
              (eval))))
      (util/lru-memoize)))

(def ^:private pg-class-schema-hack
  {"pg_catalog/pg_class" #{}})

(defn ->equi-comparator [^VectorReader left-col, ^VectorReader right-col, params
                         {:keys [nil-keys-equal? param-fields]}]
  (let [f (build-comparator {:op :call, :f (if nil-keys-equal? :null-eq :==)
                             :args [{:op :variable, :variable left-vec, :rel left-rel, :idx left-idx}
                                    {:op :variable, :variable right-vec, :rel right-rel, :idx right-idx}]}
                            {:vec-fields {left-vec (.getField left-col)
                                          right-vec (.getField right-col)}
                             :param-fields param-fields})]
    (f (vr/rel-reader [(.withName left-col (str left-vec))])
       (vr/rel-reader [(.withName right-col (str right-vec))])
       pg-class-schema-hack
       params)))

(defn ->theta-comparator [build-rel probe-rel theta-expr params {:keys [build-fields probe-fields param-fields]}]
  (let [vec-fields (merge build-fields probe-fields)
        f (build-comparator (->> (expr/form->expr theta-expr {:vec-fields vec-fields, :param-fields param-fields})
                                 (expr/prepare-expr)
                                 (ewalk/postwalk-expr (fn [{:keys [op] :as expr}]
                                                        (cond-> expr
                                                          (= op :variable)
                                                          (into (let [{:keys [variable]} expr]
                                                                  (if (contains? probe-fields variable)
                                                                    {:rel right-rel, :idx right-idx}
                                                                    {:rel left-rel, :idx left-idx})))))))
                            {:vec-fields vec-fields, :param-fields param-fields})]
    (f build-rel
       probe-rel
       pg-class-schema-hack
       params)))
