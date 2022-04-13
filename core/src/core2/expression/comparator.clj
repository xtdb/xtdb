(ns core2.expression.comparator
  (:require [core2.expression :as expr]
            [core2.types :as types]
            [core2.util :as util])
  (:import [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Int ArrowType$Timestamp ArrowType$Utf8 ArrowType$Date]))

(set! *unchecked-math* :warn-on-boxed)

(definterface ColumnComparator
  (^int compareIdx [^org.apache.arrow.vector.ValueVector left-vec, ^int left-idx,
                    ^org.apache.arrow.vector.ValueVector right-vec, ^int right-idx]))

(defmethod expr/codegen-call [:compare ArrowType$Int ArrowType$Int] [_]
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(Long/compare ~@emitted-args)))
   :return-types #{types/int-type}})

(defmethod expr/codegen-call [:compare ::types/Number ::types/Number] [_]
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(Double/compare ~@emitted-args)))
   :return-types #{types/int-type}})

(defmethod expr/codegen-call [:compare ArrowType$Timestamp ArrowType$Timestamp] [_]
  ;; TODO different scales
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(Long/compare ~@emitted-args)))
   :return-types #{types/int-type}})

(defmethod expr/codegen-call [:compare ArrowType$Binary ArrowType$Binary] [_]
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(util/compare-nio-buffers-unsigned ~@emitted-args)))
   :return-types #{types/int-type}})

(defmethod expr/codegen-call [:compare ArrowType$Bool ArrowType$Bool] [_]
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(Boolean/compare ~@emitted-args)))
   :return-types #{types/int-type}})

(defmethod expr/codegen-call [:compare ::types/Object ::types/Object] [_]
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(.compareTo ~@(map #(expr/with-tag % Comparable) emitted-args))))
   :return-types #{types/int-type}})

(defmethod expr/codegen-call [:compare ArrowType$Utf8 ArrowType$Utf8] [_]
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(util/compare-nio-buffers-unsigned ~@emitted-args)))
   :return-types #{types/int-type}})

(def ^core2.expression.comparator.ColumnComparator ->comparator
  (-> (fn [^ArrowType arrow-type]
        (let [left-vec-sym (gensym 'left-vec)
              left-idx-sym (gensym 'left-idx)
              right-vec-sym (gensym 'right-vec)
              right-idx-sym (gensym 'right-idx)
              vec-type (types/arrow-type->vector-type arrow-type)]
          (eval
           `(reify ColumnComparator
              (compareIdx [_# ~left-vec-sym ~left-idx-sym ~right-vec-sym ~right-idx-sym]
                ~(let [{:keys [continue-call]} (expr/codegen-call {:f :compare
                                                                   :arg-types [arrow-type arrow-type]})]
                   (continue-call (fn [_ code] code)
                                  [(expr/get-value-form arrow-type (-> left-vec-sym (expr/with-tag vec-type)) left-idx-sym)
                                   (expr/get-value-form arrow-type (-> right-vec-sym (expr/with-tag vec-type)) right-idx-sym)])))))))

      (memoize)))
