(ns core2.expression.comparator
  (:require [core2.expression :as expr]
            [core2.types :as types])
  (:import [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Int ArrowType$Timestamp ArrowType$Utf8]
           org.apache.arrow.vector.types.Types$MinorType))

(set! *unchecked-math* :warn-on-boxed)

(definterface ColumnComparator
  (^int compareIdx [^org.apache.arrow.vector.ValueVector left-vec, ^int left-idx,
                    ^org.apache.arrow.vector.ValueVector right-vec, ^int right-idx]))

(def ^:private compare-return-type (.getType Types$MinorType/INT))

(defmethod expr/codegen-call [:compare ArrowType$Int ArrowType$Int] [{:keys [emitted-args]}]
  {:code `(Long/compare ~@emitted-args)
   :return-type compare-return-type})

(defmethod expr/codegen-call [:compare ::types/Number ::types/Number] [{:keys [emitted-args]}]
  {:code `(Double/compare ~@emitted-args)
   :return-type compare-return-type})

(defmethod expr/codegen-call [:compare ArrowType$Timestamp ArrowType$Timestamp] [{:keys [emitted-args]}]
  {:code `(Long/compare ~@emitted-args)
   :return-type compare-return-type})

(defmethod expr/codegen-call [:compare ArrowType$Binary ArrowType$Binary] [{:keys [emitted-args]}]
  {:code `(expr/compare-nio-buffers-unsigned ~@emitted-args)
   :return-type compare-return-type})

(defmethod expr/codegen-call [:compare ArrowType$Bool ArrowType$Bool] [{:keys [emitted-args]}]
  {:code `(Boolean/compare ~@emitted-args)
   :return-type compare-return-type})

(defmethod expr/codegen-call [:compare ::types/Object ::types/Object] [{:keys [emitted-args]}]
  {:code `(.compareTo ~@(map #(expr/with-tag % Comparable) emitted-args))
   :return-type compare-return-type})

(defmethod expr/codegen-call [:compare ArrowType$Utf8 ArrowType$Utf8] [{:keys [emitted-args]}]
  {:code `(expr/compare-nio-buffers-unsigned ~@emitted-args)
   :return-type compare-return-type})

(prefer-method expr/codegen-call [:compare ArrowType$Timestamp ArrowType$Timestamp] [:compare ::types/Object ::types/Object])
(prefer-method expr/codegen-call [:compare ::types/Number ::types/Number] [:compare ::types/Object ::types/Object])
(prefer-method expr/codegen-call [:compare ArrowType$Utf8 ArrowType$Utf8] [:compare ::types/Object ::types/Object])

(defn- comparator-code [^ArrowType arrow-type]
  (let [left-vec-sym (gensym 'left-vec)
        left-idx-sym (gensym 'left-idx)
        right-vec-sym (gensym 'right-vec)
        right-idx-sym (gensym 'right-idx)
        vec-type (types/arrow-type->vector-type arrow-type)]
    `(reify ColumnComparator
       (compareIdx [_# ~left-vec-sym ~left-idx-sym ~right-vec-sym ~right-idx-sym]
         ~(:code (expr/codegen-call {:f :compare
                                     :arg-types [arrow-type arrow-type]
                                     :emitted-args [(expr/get-value-form arrow-type (-> left-vec-sym (expr/with-tag vec-type)) left-idx-sym)
                                                    (expr/get-value-form arrow-type (-> right-vec-sym (expr/with-tag vec-type)) right-idx-sym)]}))))))

(def ^:private memo-comparator-code (memoize comparator-code))
(def ^:private memo-eval (memoize eval))

(defn ->comparator ^core2.expression.comparator.ColumnComparator [^ArrowType arrow-type]
  (-> (memo-comparator-code arrow-type)
      (memo-eval)))
