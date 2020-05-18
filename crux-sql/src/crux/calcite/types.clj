(ns crux.calcite.types
  (:import org.apache.calcite.rex.RexCall
           org.apache.calcite.linq4j.tree.Expression))

(defrecord SQLFunction [sym op operands])

(defrecord CruxKeywordFn [^RexCall r])

(defrecord SQLCondition [c clauses])

(defrecord SQLPredicate [op operands])

(defrecord WrapLiteral [l])

(defrecord SqlLambda [sym ^Expression e operands])
