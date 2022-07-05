(ns core2.expression.comparator
  (:require [core2.expression :as expr]
            [core2.types :as types]
            [core2.util :as util])
  (:import (core2.vector IIndirectVector)
           java.util.HashMap
           java.util.function.IntBinaryOperator))

(set! *unchecked-math* :warn-on-boxed)

(defmethod expr/codegen-mono-call [:compare :bool :bool] [_]
  {:return-type :i32
   :->call-code (fn [emitted-args]
                  `(Boolean/compare ~@emitted-args))})

(defmethod expr/codegen-mono-call [:compare :int :int] [_]
  {:return-type :i32
   :->call-code (fn [emitted-args]
                  `(Long/compare ~@emitted-args))})

(defmethod expr/codegen-mono-call [:compare :num :num] [_]
  {:return-type :i32
   :->call-code (fn [emitted-args]
                  `(Double/compare ~@emitted-args))})

(defmethod expr/codegen-mono-call [:compare :date :date] [_]
  ;; TODO different scales
  {:return-type :i32
   :->call-code (fn [emitted-args]
                  `(Long/compare ~@emitted-args))})

(defmethod expr/codegen-mono-call [:compare :timestamp-tz :timestamp-tz] [_]
  ;; TODO different scales
  {:return-type :i32
   :->call-code (fn [emitted-args]
                  `(Long/compare ~@emitted-args))})

(doseq [col-type #{:varbinary :utf8}]
  (defmethod expr/codegen-mono-call [:compare col-type col-type] [_]
    {:return-type :i32
     :->call-code (fn [emitted-args]
                    `(util/compare-nio-buffers-unsigned ~@emitted-args))}))

(doseq [col-type #{[:extension :keyword ""] [:extension :uuid ""]}]
  (defmethod expr/codegen-mono-call [:compare col-type col-type] [_]
    {:return-type :i32
     :->call-code (fn [emitted-args]
                    `(.compareTo ~@(map #(expr/with-tag % Comparable) emitted-args)))}))

(doseq [[f left-type right-type res] [[:compare-nulls-first :null :null 0]
                                      [:compare-nulls-first :null :any -1]
                                      [:compare-nulls-first :any :null 1]
                                      [:compare-nulls-last :null :null 0]
                                      [:compare-nulls-last :null :any 1]
                                      [:compare-nulls-last :any :null -1]]]
  (defmethod expr/codegen-mono-call [f left-type right-type] [_]
    {:return-type :i32
     :->call-code (constantly res)}))

(doseq [f [:compare-nulls-first :compare-nulls-last]]
  (defmethod expr/codegen-mono-call [f :any :any] [expr]
    (expr/codegen-mono-call (assoc expr :f :compare))))

(def ^:private build-comparator
  (-> (fn [left-col-type right-col-type null-ordering]
        (let [left-idx-sym (gensym 'left-idx)
              right-idx-sym (gensym 'right-idx)
              left-col-sym (gensym 'left-col)
              right-col-sym (gensym 'right-col)

              {cont :continue, :as emitted-expr}
              (expr/codegen-expr {:op :call,
                                  :f (case null-ordering
                                       :nulls-first :compare-nulls-first
                                       :nulls-last :compare-nulls-last)
                                  :args [{:op :variable, :variable left-col-sym, :idx left-idx-sym}
                                         {:op :variable, :variable right-col-sym, :idx right-idx-sym}]}

                                 {:var->col-type {left-col-sym left-col-type, right-col-sym right-col-type}
                                  :extract-vecs-from-rel? false})]

          (-> `(fn [~(-> left-col-sym (expr/with-tag IIndirectVector))
                    ~(-> right-col-sym (expr/with-tag IIndirectVector))]
                 (let [~@(expr/batch-bindings emitted-expr)]
                   (reify IntBinaryOperator
                     (~'applyAsInt [_# ~left-idx-sym ~right-idx-sym]
                      ~(cont (fn [_ code] code))))))
              #_(doto clojure.pprint/pprint)
              (eval))))
      (util/lru-memoize)))

(defn ->comparator ^java.util.function.IntBinaryOperator [^IIndirectVector left-col, ^IIndirectVector right-col, null-ordering]
  (let [left-field (.getField (.getVector left-col))
        right-field (.getField (.getVector right-col))
        f (build-comparator (types/field->col-type left-field)
                            (types/field->col-type right-field)
                            null-ordering)]
    (f left-col right-col)))
