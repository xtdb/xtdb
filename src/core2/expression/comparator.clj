(ns core2.expression.comparator
  (:require [core2.expression :as expr]
            [core2.types :as types])
  (:import java.util.Date
           org.apache.arrow.vector.types.pojo.ArrowType))

(set! *unchecked-math* :warn-on-boxed)

(definterface ColumnComparator
  (^int compareIdx [^core2.relation.IReadColumn left-col, ^int left-idx
                    ^core2.relation.IReadColumn right-col, ^int right-idx]))

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
  (let [left-col-sym (gensym 'left-col)
        left-idx-sym (gensym 'left-idx)
        right-col-sym (gensym 'right-col)
        right-idx-sym (gensym 'right-idx)
        el-type (get types/arrow-type->java-type arrow-type Comparable)
        codegen-opts {:var->types {left-col-sym #{arrow-type}, right-col-sym #{arrow-type}}}]
    `(reify ColumnComparator
       (compareIdx [_# ~left-col-sym ~left-idx-sym ~right-col-sym ~right-idx-sym]
         ~(:code (expr/codegen-expr
                  {:op :call,
                   :f 'compare,
                   :args [{:code `(let [~expr/idx-sym ~left-idx-sym]
                                    ~(:code (expr/codegen-expr
                                             {:op :variable, :variable left-col-sym}
                                             codegen-opts)))
                           :return-type el-type}
                          {:code `(let [~expr/idx-sym ~right-idx-sym]
                                    ~(:code (expr/codegen-expr
                                             {:op :variable, :variable right-col-sym}
                                             codegen-opts)))
                           :return-type el-type}]}
                  codegen-opts))))))

(def ^:private memo-comparator-code (memoize comparator-code))
(def ^:private memo-eval (memoize eval))

(defn ->comparator ^core2.expression.comparator.ColumnComparator [^ArrowType arrow-type]
  (-> (memo-comparator-code arrow-type)
      (memo-eval)))
