(ns xtdb.expression.map
  (:require [xtdb.expression :as expr]
            [xtdb.expression.walk :as ewalk]
            [xtdb.util :as util])
  (:import (java.util Map)
           java.util.function.IntBinaryOperator
           (xtdb.arrow RelationReader)))

(def ^:private left-rel (gensym 'left-rel))
(def ^:private left-idx (gensym 'left-idx))

(def ^:private right-rel (gensym 'right-rel))
(def ^:private right-idx (gensym 'right-idx))

(def build-comparator
  (-> (fn [expr input-opts]
        (let [{:keys [continue], :as emitted-expr} (expr/codegen-expr expr input-opts)]
          (-> `(fn [~(expr/with-tag left-rel RelationReader)
                    ~(expr/with-tag right-rel RelationReader)
                    ~(-> expr/schema-sym (expr/with-tag Map))
                    ~(-> expr/args-sym (expr/with-tag RelationReader))]
                 (let [~@(expr/batch-bindings emitted-expr)]
                   (reify IntBinaryOperator
                     (~'applyAsInt [_# ~left-idx ~right-idx]
                      ~(continue (fn [res-type code]
                                   (condp = res-type
                                     #xt/type :null 0
                                     #xt/type :bool `(if ~code 1 -1))))))))

              #_(doto clojure.pprint/pprint)
              (eval))))
      (util/lru-memoize)))

(def ^:private pg-class-schema-hack
  {"pg_catalog/pg_class" #{}})

(defn ->theta-comparator [build-rel probe-rel theta-expr params {:keys [build-vec-types probe-vec-types param-types]}]
  (let [var-types (merge build-vec-types probe-vec-types)
        f (build-comparator (->> (expr/form->expr theta-expr {:var-types var-types, :param-types param-types})
                                 (expr/prepare-expr)
                                 (ewalk/postwalk-expr (fn [{:keys [op] :as expr}]
                                                        (cond-> expr
                                                          (= op :variable)
                                                          (into (let [{:keys [variable]} expr]
                                                                  (if (contains? probe-vec-types variable)
                                                                    {:rel right-rel, :idx right-idx}
                                                                    {:rel left-rel, :idx left-idx})))))))
                            {:var-types var-types, :param-types param-types})]
    (f build-rel
       probe-rel
       pg-class-schema-hack
       params)))
