(ns xtdb.expression.comparator
  (:require [xtdb.expression :as expr]
            [xtdb.util :as util]))

(set! *unchecked-math* :warn-on-boxed)

;; null-eq is an internal function used in situations where two nulls should compare equal,
;; e.g when grouping rows in group-by.
(defmethod expr/codegen-call [:null_eq :any :any] [call]
  (expr/codegen-call (assoc call :f :==)))

(defmethod expr/codegen-call [:null_eq :null :any] [_]
  {:return-type #xt/type :bool, :->call-code (constantly false)})

(defmethod expr/codegen-call [:null_eq :any :null] [_]
  {:return-type #xt/type :bool, :->call-code (constantly false)})

(defmethod expr/codegen-call [:null_eq :null :null] [_]
  {:return-type #xt/type :bool, :->call-code (constantly true)})

(defmethod expr/codegen-call [:<> :any :any] [expr]
  (let [{:keys [->call-code continue-call] :as eq-call} (expr/codegen-call (assoc expr :f :==))]
    (cond-> eq-call
      ->call-code (assoc :->call-code (comp #(do `(not ~%)) ->call-code))
      continue-call (assoc :continue-call
                           (fn [f emitted-args]
                             (continue-call (fn [return-type code]
                                              (if (= #xt/type :null return-type)
                                                (f return-type code)
                                                (f return-type `(not ~code))))
                                            emitted-args))))))

;; distinct_from is the SQL standard NULL-safe inequality operator (IS DISTINCT FROM).
;; NULL IS DISTINCT FROM NULL → false (they're not distinct)
;; NULL IS DISTINCT FROM <value> → true (they are distinct)
;; <value> IS DISTINCT FROM <value> → delegates to <>
(defmethod expr/codegen-call [:distinct_from :null :null] [_]
  {:return-type #xt/type :bool, :->call-code (constantly false)})

(defmethod expr/codegen-call [:distinct_from :null :any] [_]
  {:return-type #xt/type :bool, :->call-code (constantly true)})

(defmethod expr/codegen-call [:distinct_from :any :null] [_]
  {:return-type #xt/type :bool, :->call-code (constantly true)})

(defmethod expr/codegen-call [:distinct_from :any :any] [expr]
  (expr/codegen-call (assoc expr :f :<>)))

(defmethod expr/codegen-call [:compare :bool :bool] [_]
  {:return-type #xt/type :i32
   :->call-code (fn [emitted-args]
                  `(Boolean/compare ~@emitted-args))})

(defmethod expr/codegen-call [:compare :int :int] [_]
  {:return-type #xt/type :i32
   :->call-code (fn [emitted-args]
                  `(Long/compare ~@emitted-args))})

(defmethod expr/codegen-call [:compare :num :num] [_]
  {:return-type #xt/type :i32
   :->call-code (fn [emitted-args]
                  `(Double/compare ~@emitted-args))})

;; NOTE UUID compares according to bytes rather than Java `compare` - https://bugs.openjdk.org/browse/JDK-7025832
(doseq [col-type #{:varbinary :fixed-size-binary :utf8 :uri :keyword :uuid}]
  (defmethod expr/codegen-call [:compare col-type col-type] [_]
    {:return-type #xt/type :i32
     :->call-code (fn [emitted-args]
                    `(util/compare-nio-buffers-unsigned ~@emitted-args))}))

(doseq [[f left-type right-type res] [[:compare_nulls_first :null :null 0]
                                      [:compare_nulls_first :null :any -1]
                                      [:compare_nulls_first :any :null 1]

                                      [:compare_nulls_last :null :null 0]
                                      [:compare_nulls_last :null :any 1]
                                      [:compare_nulls_last :any :null -1]]]
  (defmethod expr/codegen-call [f left-type right-type] [_]
    {:return-type #xt/type :i32
     :->call-code (constantly res)}))

(doseq [f [:compare_nulls_first :compare_nulls_last]]
  (defmethod expr/codegen-call [f :any :any] [expr]
    (expr/codegen-call (assoc expr :f :compare))))
