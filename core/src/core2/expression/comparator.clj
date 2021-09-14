(ns core2.expression.comparator
  (:require [core2.expression :as expr]
            [core2.types :as types])
  (:import core2.relation.IReadColumn
           java.util.Date
           org.apache.arrow.vector.types.pojo.ArrowType))

(set! *unchecked-math* :warn-on-boxed)

(definterface ColumnComparator
  (^int compareIdx [^org.apache.arrow.vector.ValueVector left-vec, ^int left-idx,
                    ^org.apache.arrow.vector.ValueVector right-vec, ^int right-idx]))

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

(defmethod expr/codegen-call [:compare types/byte-array-class types/byte-array-class] [{:keys [emitted-args]}]
  {:code `(expr/compare-nio-buffers-unsigned ~@emitted-args)
   :return-type Long})

(defmethod expr/codegen-call [:compare Boolean Boolean] [{:keys [emitted-args]}]
  {:code `(Boolean/compare ~@emitted-args)
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
        el-type (get types/arrow-type->java-type arrow-type Comparable)]
    `(reify ColumnComparator
       (compareIdx [_# ~left-vec-sym ~left-idx-sym ~right-vec-sym ~right-idx-sym]
         ~(:code (expr/codegen-call {:f :compare
                                     :arg-types [el-type el-type]
                                     :emitted-args [(expr/get-value-form arrow-type left-vec-sym left-idx-sym)
                                                    (expr/get-value-form arrow-type right-vec-sym right-idx-sym)]}))))))

(def ^:private memo-comparator-code (memoize comparator-code))
(def ^:private memo-eval (memoize eval))

(defn ->comparator ^core2.expression.comparator.ColumnComparator [^ArrowType arrow-type]
  (-> (memo-comparator-code arrow-type)
      (memo-eval)))
