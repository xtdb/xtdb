(ns crux.calcite.types
  (:import org.apache.calcite.rex.RexCall))

(defrecord SQLFunction [sym op operands])

(defrecord CruxKeywordFn [^RexCall r])

(defrecord SQLCondition [c clauses])

(defrecord SQLPredicate [op operands])
