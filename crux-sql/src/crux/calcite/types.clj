(ns crux.calcite.types
  (:import org.apache.calcite.rex.RexCall
           org.apache.calcite.linq4j.tree.Expression))

(defrecord CruxKeywordFn [^RexCall r])

(defrecord SQLCondition [c clauses])

(defrecord SQLPredicate [op operands])

(defrecord ArbitraryFn [op operands])
