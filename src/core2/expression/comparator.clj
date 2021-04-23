(ns core2.expression.comparator
  (:require [core2.expression :as expr]
            [core2.types :as types])
  (:import java.util.Date
           org.apache.arrow.vector.types.pojo.ArrowType))

(set! *unchecked-math* :warn-on-boxed)

(definterface FieldVecComparator
  (^int compareIdx [^org.apache.arrow.vector.FieldVector left-vec, ^int left-idx
                    ^org.apache.arrow.vector.FieldVector right-vec, ^int right-idx]))

(defmethod expr/codegen-call [:compare Long Long] [{:keys [emitted-args]}]
  {:code `(Long/compare ~@emitted-args)
   :return-type Long})

(defmethod expr/codegen-call [:compare Double Double] [{:keys [emitted-args]}]
  {:code `(Double/compare ~@emitted-args)
   :return-type Long})

(defmethod expr/codegen-call [:compare Number Number] [{:keys [emitted-args]}]
  {:code `(Double/compare ~@emitted-args)
   :return-type Long})

(defmethod expr/codegen-call [:compare Date Date] [{:keys [emitted-args]}]
  {:code `(Long/compare ~@emitted-args)
   :return-type Long})

(defmethod expr/codegen-call [:compare expr/byte-array-class expr/byte-array-class] [{:keys [emitted-args]}]
  {:code `(expr/compare-nio-buffers-unsigned ~@emitted-args)
   :return-type Long})

(defmethod expr/codegen-call [:compare Comparable Comparable] [{:keys [emitted-args]}]
  {:code `(.compareTo ~@emitted-args)
   :return-type Long})

(defmethod expr/codegen-call [:compare String String] [{:keys [emitted-args]}]
  {:code `(expr/compare-nio-buffers-unsigned ~@emitted-args)
   :return-type Long})

(prefer-method expr/codegen-call [:compare Date Date] [:compare Comparable Comparable])
(prefer-method expr/codegen-call [:compare Number Number] [:compare Comparable Comparable])
(prefer-method expr/codegen-call [:compare String String] [:compare Comparable Comparable])

(defn- comparator-code [^ArrowType arrow-type]
  (let [left-vec-sym (gensym 'left-vec)
        left-idx-sym (gensym 'left-idx)
        right-vec-sym (gensym 'right-vec)
        right-idx-sym (gensym 'right-idx)
        el-type (get types/arrow-type->java-type arrow-type Comparable)
        vec-type (types/arrow-type->vector-type arrow-type)
        codegen-opts {:var->type {left-vec-sym el-type, right-vec-sym el-type}}]
    `(reify FieldVecComparator
       (compareIdx [_# ~left-vec-sym ~left-idx-sym ~right-vec-sym ~right-idx-sym]
         (let [~(-> left-vec-sym (expr/with-tag vec-type)) ~left-vec-sym
               ~(-> right-vec-sym (expr/with-tag vec-type)) ~right-vec-sym]
           ~(:code (expr/codegen-expr
                    {:op :call,
                     :f 'compare,
                     :args [{:code `(let [~expr/idx-sym ~left-idx-sym]
                                      ~(:code (expr/codegen-expr
                                               {:op :variable, :variable left-vec-sym}
                                               codegen-opts)))
                             :return-type el-type}
                            {:code `(let [~expr/idx-sym ~right-idx-sym]
                                      ~(:code (expr/codegen-expr
                                               {:op :variable, :variable right-vec-sym}
                                               codegen-opts)))
                             :return-type el-type}]}
                    codegen-opts)))))))

(def ^:private memo-comparator-code
  (memoize comparator-code))

(def ^:private memo-eval (memoize eval))

(defn ->comparator ^core2.expression.comparator.FieldVecComparator [^ArrowType arrow-type]
  (-> (memo-comparator-code arrow-type)
      (memo-eval)))
