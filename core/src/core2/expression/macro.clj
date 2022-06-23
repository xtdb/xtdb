(ns core2.expression.macro
  (:require [core2.expression.walk :as walk]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti macroexpand1-call
  (fn [{:keys [f] :as call-expr} opts]
    (keyword (name f)))
  :default ::default)

(defmethod macroexpand1-call ::default [expr _] expr)

(defn macroexpand1l-call [{:keys [f args] :as expr} _]
  (if (> (count args) 2)
    {:op :call, :f f
     :args [(update expr :args butlast)
            (last args)]}
    expr))

(defn macroexpand1r-call [{:keys [f args] :as expr} _]
  (if (> (count args) 2)
    {:op :call, :f f
     :args [(first args)
            (update expr :args rest)]}
    expr))

(doseq [f #{:+ :- :* :/ :min :max}]
  (defmethod macroexpand1-call f [expr opts] (macroexpand1l-call expr opts)))

(doseq [[f id] #{[:and true] [:or false]}]
  (defmethod macroexpand1-call f [{:keys [args] :as expr} opts]
    (case (count args)
      0 {:op :literal, :literal id}
      1 (first args)
      2 expr
      (macroexpand1r-call expr opts))))

(doseq [f #{:< :<= := :!= :>= :>}]
  (defmethod macroexpand1-call f [{:keys [args] :as expr} _]
    (case (count args)
      (0 1) {:op :literal, :literal (not= f :!=)}
      2 expr

      {:op :call, :f :and
       :args (for [args (partition 2 1 args)]
               {:op :call, :f f, :args args})})))

(def ^:private nil-literal
  {:op :literal, :literal nil})

(defmethod macroexpand1-call :cond [{:keys [args]} _]
  (case (count args)
    0 nil-literal
    1 (first args) ; unlike Clojure, we allow a default expr at the end
    (let [[test expr & more-args] args]
      {:op :if
       :pred {:op :call, :f :true?
              :args [test]}
       :then expr
       :else {:op :call, :f :cond, :args more-args}})))

(defmethod macroexpand1-call :case [{:keys [args]} _]
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

(defmethod macroexpand1-call :coalesce [{:keys [args]} _]
  (case (count args)
    0 nil-literal
    1 (first args)
    (let [local (gensym 'coalesce)]
      {:op :if-some
       :local local
       :expr (first args)
       :then {:op :local, :local local}
       :else {:op :call, :f :coalesce, :args (rest args)}})))

(defmethod macroexpand1-call :nullif [{[x y] :args} _]
  (let [local (gensym 'nullif)]
    {:op :let
     :local local
     :expr x
     :body {:op :if
            :pred {:op :call, :f :true?
                   :args [{:op :call, :f :=,
                           :args [{:op :local, :local local} y]}]}
            :then nil-literal
            :else {:op :local, :local local}}}))

;; SQL:2011 §8.3
(defmethod macroexpand1-call :between [{[x left right :as args] :args, :as expr} {:keys [gensym]}]
  (assert (= 3 (count args)) (format "`between` expects 3 args: '%s'" (pr-str expr)))

  ;; TODO hiding `x` behind a local might mean we don't use metadata when we could.
  (let [local (gensym 'between)
        local-expr {:op :local, :local local}]
    {:op :let, :local local, :expr x
     :body {:op :call, :f :and
            :args [{:op :call, :f :>=, :args [local-expr left]}
                   {:op :call, :f :<=, :args [local-expr right]}]}}))

(defmethod macroexpand1-call :between-symmetric [{[x left right :as args] :args, :as expr} {:keys [gensym]}]
  (assert (= 3 (count args)) (format "`between-symmetric` expects 3 args: '%s'" (pr-str expr)))

  (let [local (gensym 'between-symmetric)
        local-expr {:op :local, :local local}]
    {:op :let, :local local, :expr x
     :body {:op :call, :f :or
            :args [{:op :call, :f :between, :args [local-expr left right]}
                   {:op :call, :f :between, :args [local-expr right left]}]}}))

(defn macroexpand-expr [expr opts]
  (loop [{:keys [op] :as expr} expr]
    (if-not (= :call op)
      expr
      (let [new-expr (macroexpand1-call expr opts)]
        (if (identical? expr new-expr)
          new-expr
          (recur new-expr))))))

(defn macroexpand-all [expr]
  (let [opts {:gensym (let [counter (int-array [0])]
                        (fn [sym]
                          (let [x (aget counter 0)]
                            (aset counter 0 (inc x))
                            (symbol (str sym x)))))}]
    (walk/prewalk-expr #(macroexpand-expr % opts) expr)))
