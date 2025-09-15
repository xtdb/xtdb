(ns xtdb.expression.map
  (:require [xtdb.expression :as expr]
            [xtdb.expression.walk :as ewalk]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.util Map)
           java.util.function.IntBinaryOperator
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector NullVector)
           (org.apache.arrow.vector.types.pojo Schema)
           (xtdb.arrow RelationReader VectorReader)))

(def ^:private left-rel (gensym 'left-rel))
(def ^:private left-vec (gensym 'left-vec))
(def ^:private left-idx (gensym 'left-idx))

(def ^:private right-rel (gensym 'right-rel))
(def ^:private right-vec (gensym 'right-vec))
(def ^:private right-idx (gensym 'right-idx))

(def build-comparator
  (-> (fn [expr input-opts]
        (let [{:keys [continue], :as emitted-expr}
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

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->equi-comparator [^VectorReader left-col, ^VectorReader right-col, params
                         {:keys [nil-keys-equal? param-types]}]
  (let [f (build-comparator {:op :call, :f (if nil-keys-equal? :null-eq :=)
                             :args [{:op :variable, :variable left-vec, :rel left-rel, :idx left-idx}
                                    {:op :variable, :variable right-vec, :rel right-rel, :idx right-idx}]}
                            {:var->col-type {left-vec (types/field->col-type (.getField left-col))
                                             right-vec (types/field->col-type (.getField right-col))}
                             :param-types param-types})]
    (f (vr/rel-reader [(.withName left-col (str left-vec))])
       (vr/rel-reader [(.withName right-col (str right-vec))])
       pg-class-schema-hack
       params)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->theta-comparator [build-rel probe-rel theta-expr params {:keys [build-fields probe-fields param-types]}]
  (let [col-types (update-vals (merge build-fields probe-fields) types/field->col-type)
        f (build-comparator (->> (expr/form->expr theta-expr {:col-types col-types, :param-types param-types})
                                 (expr/prepare-expr)
                                 (ewalk/postwalk-expr (fn [{:keys [op] :as expr}]
                                                        (cond-> expr
                                                          (= op :variable)
                                                          (into (let [{:keys [variable]} expr]
                                                                  (if (contains? probe-fields variable)
                                                                    {:rel right-rel, :idx right-idx}
                                                                    {:rel left-rel, :idx left-idx})))))))
                            {:var->col-type col-types, :param-types param-types})]
    (f build-rel
       probe-rel
       pg-class-schema-hack
       params)))

(defn ->nil-rel
  "Returns a single row relation where all columns are nil. (Useful for outer joins)."
  ^xtdb.arrow.RelationReader [col-names]
  (vr/rel-reader (for [col-name col-names]
                   (vr/vec->reader (doto (NullVector. (str col-name))
                                     (.setValueCount 1))))))

(defn ->nillable-rel-writer
  "Returns a relation with a single row where all columns are nil, but the schema is nillable."
  ^xtdb.arrow.RelationWriter [^BufferAllocator allocator fields]
  (let [schema (Schema. (mapv (fn [[field-name field]]
                                (-> field
                                    (types/field-with-name (str field-name))
                                    (types/->nullable-field)))
                              fields))]
    (util/with-close-on-catch [rel-writer (vw/->rel-writer allocator schema)]
      (doto (.rowCopier (->nil-rel (keys fields)) rel-writer)
        (.copyRow 0))
      rel-writer)))

(def nil-row-idx 0)

