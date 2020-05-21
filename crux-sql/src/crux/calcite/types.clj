(ns crux.calcite.types
  (:import org.apache.calcite.rex.RexCall
           org.apache.calcite.linq4j.tree.Expression))

(defrecord ArithmeticFunction [sym op operands])

(defrecord CruxKeywordFn [^RexCall r])

(defrecord SQLCondition [c clauses])

(defrecord SQLPredicate [op operands])

(defrecord CalciteLambda [sym ^Expression e operands])
