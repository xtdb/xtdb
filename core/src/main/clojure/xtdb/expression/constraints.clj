(ns xtdb.expression.constraints
  "Extracts concrete constraints (equality values, range bounds) from EE expressions.
  Shares `normalise-bool-args` with expression/metadata — same principle of parsing
  EE expressions for structured use, but resolving to concrete values rather than codegen."
  (:require [xtdb.expression :as expr])
  (:import (xtdb.arrow RelationReader VectorReader)))

(set! *unchecked-math* :warn-on-boxed)

(defn normalise-bool-args
  "Classifies the two arguments of a comparison expression.
  Returns [:col-val col-expr val-expr], [:val-col col-expr val-expr], [:constant], or nil."
  [[{x-op :op, :as x-expr} {y-op :op, :as y-expr}]]
  (case [x-op y-op]
    ([:param :param] [:literal :literal] [:literal :param] [:param :literal]) [:constant]
    ([:variable :literal] [:variable :param]) [:col-val x-expr y-expr]
    ([:literal :variable] [:param :variable]) [:val-col y-expr x-expr]
    nil))

(defn resolve-value
  "Resolves an AST value node (:literal or :param) to a concrete Java value."
  [expr ^RelationReader args]
  (case (:op expr)
    :literal (:literal expr)
    :param (when args
             (when-let [^VectorReader col (.vectorForOrNull args (str (:param expr)))]
               (.getObject col 0)))
    nil))

(def ^:private flip-op
  {:< :>, :<= :>=, :> :<, :>= :<=})

(defn- extract-comparison-bound
  "Given a comparison AST node, returns a partial bounds map or nil."
  [expr ^RelationReader args]
  (when (and (= :call (:op expr))
             (#{:>= :> :<= :<} (:f expr)))
    (when-let [[tag _col-expr val-expr] (normalise-bool-args (:args expr))]
      (when-let [v (resolve-value val-expr args)]
        (let [v (long v)
              f (if (= tag :val-col) (flip-op (:f expr)) (:f expr))]
          (case f
            :>= {:from v}
            :> {:from (inc v)}
            :<= {:to (inc v)}
            :< {:to v}))))))

(defn extract-range
  "Extracts [from, to) range bounds from a select expression form.
  Handles `BETWEEN` (which macro-expands to `and` + `>=` + `<=`) and compound comparisons.
  Returns [from to) pair or nil."
  [select-form env ^RelationReader args]
  (let [expr (-> (expr/form->expr select-form env) expr/prepare-expr)]
    (when (and (= :call (:op expr)) (= :and (:f expr)))
      (let [bounds (reduce (fn [acc sub-expr]
                             (merge acc (extract-comparison-bound sub-expr args)))
                           {} (:args expr))]
        (when (and (:from bounds) (:to bounds))
          [(:from bounds) (:to bounds)])))))

(defn extract-equality
  "Extracts an equality value from a select expression form.
  Handles both `(== col val)` and `(== val col)` orderings.
  Returns the concrete value or nil."
  [select-form env ^RelationReader args]
  (let [expr (-> (expr/form->expr select-form env) expr/prepare-expr)]
    (when (and (= :call (:op expr)) (= :== (:f expr)))
      (when-let [[tag _col-expr val-expr] (normalise-bool-args (:args expr))]
        (case tag
          (:col-val :val-col) (resolve-value val-expr args)
          nil)))))
