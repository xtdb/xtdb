(ns core2.expression
  (:require [clojure.set :as set]
            [clojure.walk :as w]
            [core2.operator.project]
            [core2.types :as types]
            [core2.util :as util])
  (:import core2.operator.project.ProjectionSpec
           core2.select.IVectorSchemaRootSelector
           java.lang.reflect.Method
           java.util.Date
           java.time.LocalDateTime
           org.apache.arrow.memory.RootAllocator
           org.apache.arrow.vector.types.Types$MinorType
           [org.apache.arrow.vector BigIntVector BitVector FieldVector FixedWidthVector Float8Vector NullVector
            TimeStampMilliVector VarBinaryVector VarCharVector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           org.apache.arrow.vector.util.Text
           org.roaringbitmap.RoaringBitmap))

;; TODO:

;; Try to change to postwalk enriching literals and variables first
;; and calls typed op multimethod like core2.compute to compile ops.

;; Normalise constants and get methods for
;; Text/bytes/Dates/Intervals. Support ArrowBufPointers?

;; Add tests for things beyond numbers.

;; Add support for IVectorPredicate, currently used by scan, but these
;; should be replaced by per-vector versions: add and use
;; IVectorSelector instead.

;; Figure out how to use this for metadata.

;; Pick proper math methods and use their actual return types in
;; inference. This way of calling real typed Java methods will be used
;; for other built in ops later.

;; Example of other built-in ops needed are things related to strings,
;; dates, casts and temporal intervals.

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
   (.getType Types$MinorType/BIT) BitVector
   (.getType Types$MinorType/UNION) DenseUnionVector})

(def ^:private arrow-type->cast
  {(.getType Types$MinorType/BIGINT) 'long
   (.getType Types$MinorType/FLOAT8) 'double
   (.getType Types$MinorType/VARBINARY) 'bytes
   (.getType Types$MinorType/VARCHAR) 'str
   (.getType Types$MinorType/TIMESTAMPMILLI) 'long
   (.getType Types$MinorType/BIT) 'boolean})

(def ^:private idx-sym (gensym "idx"))

(defn normalize-union-value [v]
  (cond
    (instance? LocalDateTime v)
    (.getTime (util/local-date-time->date v))
    (instance? Text v)
    (str v)
    :else
    v))

(defmulti compile-expression (fn [var->type expression]
                               (cond
                                 (symbol? expression)
                                 ::variable
                                 (sequential? expression)
                                 (keyword (name (first expression)))
                                 :else
                                 ::literal)))

(defmethod compile-expression ::variable [var->type expression]
  (let [type (get var->type expression)]
    [(cond
       (= (.getType Types$MinorType/BIT) type)
       `(== 1 (.get ~expression ~idx-sym))
       (.isAssignableFrom FixedWidthVector (get arrow-type->vector-type type))
       `(.get ~expression ~idx-sym)
       (= (.getType Types$MinorType/VARCHAR) type)
       `(str (.getObject ~expression ~idx-sym))
       :else
       `(normalize-union-value (.getObject ~expression ~idx-sym)))
     type]))

(defmethod compile-expression ::literal [var->type expression]
  [(if (instance? Date expression)
     (.getTime ^Date expression)
     expression)
   (types/->arrow-type (class expression))])

(defmethod compile-expression :default [_ [op & args :as expression]]
  (throw (UnsupportedOperationException. (str "unknown op: " op))))

(defn- compile-sub-expressions [var->type args]
  (let [arg+types (for [expression args]
                    (compile-expression var->type expression))]
    [(map first arg+types) (map second arg+types)]))

(defn- widen-numeric-types [types]
  (let [types (set types)]
    (cond
      (contains? types (.getType Types$MinorType/TIMESTAMPMILLI))
      (.getType Types$MinorType/TIMESTAMPMILLI)
      (contains? types (.getType Types$MinorType/FLOAT8))
      (.getType Types$MinorType/FLOAT8)
      (contains? types (.getType Types$MinorType/UNION))
      (.getType Types$MinorType/UNION)
      (= types #{(.getType Types$MinorType/BIGINT)})
      (.getType Types$MinorType/BIGINT))))

(defn- numeric-compare? [types]
  (set/subset? (set types) #{(.getType Types$MinorType/BIGINT)
                             (.getType Types$MinorType/FLOAT8)
                             (.getType Types$MinorType/TIMESTAMPMILLI)}))

(defn- compare-fn [types]
  (if (= types #{(.getType Types$MinorType/VARBINARY)})
    `java.util.Arrays/equals
    `compare))

(defmethod compile-expression := [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [(if (numeric-compare? types)
       `(== ~@args)
       `(= ~@args))
     (.getType Types$MinorType/BIT)]))

(defmethod compile-expression :!= [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [`(not= ~@args)
     (.getType Types$MinorType/BIT)]))

(defmethod compile-expression :< [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [(if (numeric-compare? types)
       `(~op ~@args)
       `(neg? (~(compare-fn types) ~@args)))
     (.getType Types$MinorType/BIT)]))

(defmethod compile-expression :<= [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [(if (numeric-compare? types)
       `(~op ~@args)
       `(not (pos? (~(compare-fn types) ~@args))))
     (.getType Types$MinorType/BIT)]))

(defmethod compile-expression :> [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [(if (numeric-compare? types)
       `(~op ~@args)
       `(pos? (~(compare-fn types) ~@args)))
     (.getType Types$MinorType/BIT)]))

(defmethod compile-expression :>= [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [(if (numeric-compare? types)
       `(~op ~@args)
       `(not (neg? (~(compare-fn types) ~@args))))
     (.getType Types$MinorType/BIT)]))

(defmethod compile-expression :and [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [`(boolean (~op args)) (.getType Types$MinorType/BIT)]))

(defmethod compile-expression :or [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [`(boolean (~op args)) (.getType Types$MinorType/BIT)]))

(defmethod compile-expression :not [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [`(boolean (~op ~@args)) (.getType Types$MinorType/BIT)]))

(defmethod compile-expression :+ [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [`(~op ~@args) (widen-numeric-types types)]))

(defmethod compile-expression :- [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [`(~op ~@args) (widen-numeric-types types)]))

(defmethod compile-expression :* [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [`(~op ~@args) (widen-numeric-types types)]))

(defmethod compile-expression :% [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)]
    [`(mod ~@args) (widen-numeric-types types)]))

(defmethod compile-expression :/ [var->type [op & args :as expression]]
  (let [[args types] (compile-sub-expressions var->type args)
        return-type (widen-numeric-types types)]
    [(if (= return-type (.getType Types$MinorType/BIGINT))
       `(quot ~@args)
       `(~op ~@args))
     return-type]))

(doseq [math-op (distinct (for [^Method m (.getDeclaredMethods Math)]
                            (.getName m)))]
  (defmethod compile-expression (keyword math-op) [var->type [_ & args]]
    (let [[args types] (compile-sub-expressions var->type args)
          return-type (widen-numeric-types types)]
      [`(~(symbol "Math" math-op) ~@args)
       return-type])))

(defmethod compile-expression :if [var->type [op & args :as expression]]
  (assert (= 3 (count args)))
  (let [[args types] (compile-sub-expressions var->type args)
        types (rest types)
        return-type (if (= 1 (count types))
                      (first types)
                      (or (widen-numeric-types types)
                          (.getType Types$MinorType/UNION)))
        cast (get arrow-type->cast return-type)]
    [(cond->> (cons op args)
       cast (list cast))
     return-type]))

(defn- normalize-expression [expression]
  (w/postwalk #(cond
                 (vector? %)
                 (seq %)
                 (keyword? %)
                 (symbol (name %))
                 :else
                 %)
              expression))

(defn- generate-code [types expression expression-type]
  (let [vars (variables expression)
        var->type (zipmap vars types)
        expression (normalize-expression expression)
        [expression arrow-return-type] (compile-expression var->type expression)
        args (for [[k v] (map vector vars types)]
               (with-meta k {:tag (symbol (.getName ^Class (get arrow-type->vector-type v)))}))]
    (case expression-type
      ::project
      (if (= (.getType Types$MinorType/UNION) arrow-return-type)
        `(fn [[~@args] ^DenseUnionVector acc# ^long row-count#]
           (dotimes [~idx-sym row-count#]
             (let [value# ~expression
                   type-id# (types/arrow-type->type-id (types/->arrow-type (class value#)))
                   offset# (util/write-type-id acc# ~idx-sym type-id#)]
               (types/set-safe! (.getVectorByType acc# type-id#) offset# value#)))
           acc#)
        (let [^Class vector-return-type (get arrow-type->vector-type arrow-return-type)
              return-type-id (types/arrow-type->type-id arrow-return-type)
              inner-acc-sym (with-meta (gensym "inner-acc") {:tag (symbol (.getName vector-return-type))})]
          `(fn [[~@args] ^DenseUnionVector acc# ^long row-count#]
             (let [~inner-acc-sym (.getVectorByType acc# ~return-type-id)]
               (dotimes [~idx-sym row-count#]
                 (let [offset# (util/write-type-id acc# ~idx-sym ~return-type-id)]
                   (.set ~inner-acc-sym offset# ~(cond
                                                   (= BitVector vector-return-type)
                                                   `(if ~expression 1 0)
                                                   (= VarCharVector vector-return-type)
                                                   `(Text. ~expression)
                                                   :else
                                                   expression))))
               acc#))))

      ::select
      (do (assert (= (.getType Types$MinorType/BIT) arrow-return-type))
          `(fn [[~@args] ^RoaringBitmap acc# ^long row-count#]
             (dotimes [~idx-sym row-count#]
               (try
                 (when ~expression
                   (.add acc# ~idx-sym))
                 (catch ClassCastException e#)))
             acc#)))))

(def ^:private memo-generate-code (memoize generate-code))
(def ^:private memo-eval (memoize eval))

(defn- expression-in-vectors [^VectorSchemaRoot in expression]
  (vec (for [var (variables expression)]
         (util/maybe-single-child-dense-union (.getVector in (name var))))))

(defn- vector->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [^ValueVector v]
  (.getType (.getFieldType (.getField v))))

(defn ->expression-projection-spec ^core2.operator.project.ProjectionSpec [col-name expression]
  (reify ProjectionSpec
    (project [_ in allocator]
      (let [in-vecs (expression-in-vectors in expression)
            types (mapv vector->arrow-type in-vecs)
            expr-code (memo-generate-code types expression ::project)
            expr-fn (memo-eval expr-code)
            ^DenseUnionVector acc (.createVector (types/->primitive-dense-union-field col-name) allocator)]
        (expr-fn in-vecs acc (.getRowCount in))))))

(defn ->expression-selector ^core2.select.IVectorSchemaRootSelector [expression]
  (reify IVectorSchemaRootSelector
    (select [_ in]
      (let [in-vecs (expression-in-vectors in expression)
            types (mapv vector->arrow-type in-vecs)
            expr-code (memo-generate-code types expression ::select)
            expr-fn (memo-eval expr-code)
            acc (RoaringBitmap.)]
        (expr-fn in-vecs acc (.getRowCount in))))))
