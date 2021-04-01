(ns core2.expression
  (:require [clojure.set :as set]
            [clojure.walk :as w]
            [core2.types :as types]
            [core2.util :as util])
  (:import core2.operator.project.ProjectionSpec
           org.apache.arrow.memory.RootAllocator
           org.apache.arrow.vector.types.Types$MinorType
           [org.apache.arrow.vector BigIntVector BitVector FieldVector Float8Vector NullVector
            TimeStampMilliVector VarBinaryVector VarCharVector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector))

(set! *unchecked-math* :warn-on-boxed)

(defn variables [expr]
  (filter symbol? (tree-seq sequential? rest expr)))

(def ^:private arrow-type->vector-type
  {(.getType Types$MinorType/NULL) NullVector
   (.getType Types$MinorType/BIGINT) BigIntVector
   (.getType Types$MinorType/FLOAT8) Float8Vector
   (.getType Types$MinorType/VARBINARY) VarBinaryVector
   (.getType Types$MinorType/VARCHAR) VarCharVector
   (.getType Types$MinorType/TIMESTAMPMILLI) TimeStampMilliVector
   (.getType Types$MinorType/BIT) BitVector})

(def ^:private compare-op? '#{< <= > >= = !=})

(def ^:private logic-op? '#{and or not})

(defn- adjust-compare-op [op args types]
  (if (set/subset? types #{(.getType Types$MinorType/BIGINT)
                           (.getType Types$MinorType/FLOAT8)
                           (.getType Types$MinorType/TIMESTAMPMILLI)})
    (cons op args)
    (let [comp (if (= types #{(.getType Types$MinorType/VARBINARY)})
                 `java.util.Arrays/equals
                 `compare)]
      (case op
        < `(neg? (~comp ~@args))
        <= `(not (pos? (~comp ~@args)))
        >  `(pos? (~comp ~@args))
        >= `(not (neg? (~comp ~@args)))
        = `(zero? (compare ~@args))
        != `(not (zero? (~comp ~@args)))))))

(defn- infer-return-type [var->type expression]
  (cond
    (symbol? expression)
    [expression (get var->type expression)]
    (not (sequential? expression))
    [expression (types/->arrow-type (class expression))]
    :else
    (let [[op & args] expression
          arg+types (for [expression args]
                      (infer-return-type var->type expression))
          types (set (map second arg+types))
          args (map first arg+types)]
      (cond
        (compare-op? op)
        [(adjust-compare-op op args types)
         (.getType Types$MinorType/BIT)]

        (logic-op? op)
        [`(boolean ~(cons op args))
         (.getType Types$MinorType/BIT)]

        :else
        [(cons op args)
         (cond
           (= #{(.getType Types$MinorType/FLOAT8)} types) (.getType Types$MinorType/FLOAT8)
           (= #{(.getType Types$MinorType/BIGINT)
                (.getType Types$MinorType/FLOAT8)} types) (.getType Types$MinorType/FLOAT8)
           (= #{(.getType Types$MinorType/BIGINT)} types) (.getType Types$MinorType/BIGINT)
           (= #{(.getType Types$MinorType/BIT)} types) (.getType Types$MinorType/BIT)
           :else
           (first types))]))))

(defn- primitive-vector-type? [^Class type]
  (= "org.apache.arrow.vector" (.getPackageName type)))

(defn- normalize-expression [expression]
  (w/postwalk #(cond
                 (vector? %)
                 (seq %)
                 (keyword? %)
                 (symbol (name %))
                 :else
                 %)
              expression))

(defn- generate-code [types expression]
  (let [vars (variables expression)
        var->type (zipmap vars types)
        expression (normalize-expression expression)
        [expression arrow-return-type] (infer-return-type var->type expression)
        ^Class vector-return-type (get arrow-type->vector-type arrow-return-type)
        inner-acc-sym (with-meta (gensym 'inner-acc) {:tag (symbol (.getName vector-return-type))})
        return-type-id (types/arrow-type->type-id arrow-return-type)
        idx-sym (gensym 'idx)
        expanded-expression (w/postwalk
                             #(if-let [type (get var->type %)]
                                (cond
                                  (= BitVector type)
                                  `(= 1 (.get ~% ~idx-sym))
                                  (primitive-vector-type? (get arrow-type->vector-type type))
                                  `(.get ~% ~idx-sym)
                                  :else
                                  `(.getObject ~% ~idx-sym))
                                %)
                             expression)
        expanded-expression (if (= BitVector vector-return-type)
                              `(if ~expanded-expression 1 0)
                              expanded-expression)]
    `(fn [[~@(for [[k v] (map vector vars types)]
               (with-meta k {:tag (symbol (.getName ^Class (get arrow-type->vector-type v)))}))]
          ^DenseUnionVector acc#
          ^long row-count#]
       (let [~inner-acc-sym (.getVectorByType acc# ~return-type-id)]
         (dotimes [~idx-sym row-count#]
           (let [offset# (util/write-type-id acc# ~idx-sym ~return-type-id)]
             (.set ~inner-acc-sym offset# ~expanded-expression))))
       acc#)))

(def ^:private memo-generate-code (memoize generate-code))
(def ^:private memo-eval (memoize eval))

(defn ->expression-projection-spec ^core2.operator.project.ProjectionSpec [col-name expression]
  (reify ProjectionSpec
    (project [_ in allocator]
      (let [in-vecs (vec (for [var (variables expression)]
                           (util/maybe-single-child-dense-union (.getVector in (name var)))))
            types (mapv #(.getType (.getFieldType (.getField ^ValueVector %))) in-vecs)
            inner-expr-code (memo-generate-code types expression)
            inner-expr-fn (memo-eval inner-expr-code)
            ^DenseUnionVector acc (.createVector (types/->primitive-dense-union-field col-name) allocator)]
        (inner-expr-fn in-vecs acc (.getRowCount in))))))
