(ns core2.expression.macro
  (:require [core2.expression.walk :as walk]
            [core2.types :as types]))

(defmulti macroexpand1-call
  (fn [{:keys [f] :as call-expr}]
    (keyword (name f)))
  :default ::default)

(defmethod macroexpand1-call ::default [expr] expr)

(defn macroexpand1l-call [{:keys [f args] :as expr}]
  (if (> (count args) 2)
    {:op :call, :f f
     :args [(update expr :args butlast)
            (last args)]}
    expr))

(defn macroexpand1r-call [{:keys [f args] :as expr}]
  (if (> (count args) 2)
    {:op :call, :f f
     :args [(first args)
            (update expr :args rest)]}
    expr))

(doseq [f #{:+ :- :* :/}]
  (defmethod macroexpand1-call f [expr] (macroexpand1l-call expr)))

(doseq [f #{:and :or}]
  (defmethod macroexpand1-call f [expr] (macroexpand1r-call expr)))

(doseq [f #{:< :<= := :!= :>= :>}]
  (defmethod macroexpand1-call f [{:keys [args] :as expr}]
    (if (> (count args) 2)
      {:op :call, :f :and
       :args (for [args (partition 2 1 args)]
               {:op :call, :f f, :args args})}
      expr)))

(def ^:private nil-literal
  {:op :literal, :literal nil, :literal-type types/null-type})

(defmethod macroexpand1-call :cond [{:keys [args]}]
  (case (count args)
    0 nil-literal
    1 (first args) ; unlike Clojure, we allow a default expr at the end
    (let [[test expr & more-args] args]
      {:op :if
       :pred {:op :call, :f :true?
              :args [test]}
       :then expr
       :else {:op :call, :f :cond, :args more-args}})))

(defmethod macroexpand1-call :case [{:keys [args]}]
  (let [[expr & clauses] args
        local (gensym 'case)]
    {:op :let
     :local local
     :expr expr
     :body {:op :call, :f :cond
            :args (->> (for [[test expr] (partition-all 2 clauses)]
                         (if-not expr
                           [test] ; default case
                           [{:op :call, :f :=,
                             :args [{:op :local, :local local} test]}
                            expr]))
                       (mapcat identity))}}))

(defmethod macroexpand1-call :coalesce [{:keys [args]}]
  (case (count args)
    0 nil-literal
    1 (first args)
    (let [local (gensym 'coalesce)]
      {:op :if-some
       :local local
       :expr (first args)
       :then {:op :local, :local local}
       :else {:op :call, :f :coalesce, :args (rest args)}})))

(defn macroexpand-expr [expr]
  (loop [{:keys [op] :as expr} expr]
    (if-not (= :call op)
      expr
      (let [new-expr (macroexpand1-call expr)]
        (if (identical? expr new-expr)
          new-expr
          (recur new-expr))))))

(defn macroexpand-all [expr]
  (walk/prewalk-expr macroexpand-expr expr))
