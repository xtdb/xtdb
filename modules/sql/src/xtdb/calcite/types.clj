(ns xtdb.calcite.types)

(defrecord SQLCondition [c clauses])

(defrecord SQLPredicate [op operands])

(defrecord ArbitraryFn [op operands])
