(ns xtdb.xtql.ast
  "The XTQL AST: parsed XTQL expressions, bindings and temporal filters.

  Built by the parser (`xtdb.xtql`) from EDN or inlined `XTQL(...)` SQL, and compiled to
  a logical plan by `xtdb.xtql.plan`. The query-clause nodes (From, Where, ...) live in the
  parser alongside the parsing logic; this namespace holds the leaf expression AST.")

;; expressions
(defrecord NullExpr [])
(defrecord BoolExpr [bool])
(defrecord LongExpr [lng])
(defrecord DoubleExpr [dbl])
(defrecord ObjExpr [obj])
(defrecord LogicVarExpr [lv])
(defrecord ParamExpr [v])
(defrecord CallExpr [f args])
(defrecord GetExpr [expr field])
(defrecord SubqueryExpr [query args])
(defrecord ExistsExpr [query args])
(defrecord PullExpr [query args])
(defrecord PullManyExpr [query args])
(defrecord ListExpr [elements])
(defrecord SetExpr [elements])
(defrecord MapExpr [elements])

(def null-expr (->NullExpr))
(def true-expr (->BoolExpr true))
(def false-expr (->BoolExpr false))

;; bindings
(defrecord Binding [binding expr])

;; temporal filters
(defrecord AllTime [])
(defrecord At [at])
(defrecord In [from to])

(def all-time (->AllTime))
