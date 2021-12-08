(ns core2.sql
  (:require [clojure.java.io :as io]
            [instaparse.core :as insta]))

(set! *unchecked-math* :warn-on-boxed)

;; Instaparse SQL:2011 parser generator from the official grammar:

(def parse-sql2011
  (insta/parser
   (io/resource "core2/sql/SQL2011.ebnf")
   :auto-whitespace (insta/parser "whitespace = #'\\s+' | #'\\s*--[^\r\n]*\\s*' | #'\\s*/[*].*?([*]/\\s*|$)'")
   :string-ci true))

(def parse
  (memoize
   (fn self
     ([s]
      (self s :direct_sql_data_statement))
     ([s start-rule]
      (parse-sql2011 s :start start-rule)))))

(declare annotate-tree)

(defn- annotate-vec
  ([ctx tree]
   (annotate-vec identity ctx tree))
  ([f ctx tree]
   (reduce
    (fn [[ctx acc] x]
      (let [[ctx x] (f (annotate-tree ctx x))]
        [ctx (conj acc x)]))
    [ctx (empty tree)]
    tree)))

(defmulti annotate-tree (fn [ctx tree]
                          (if (vector? tree)
                            (first tree)
                            ::annotate-tree-single-value)))

(defmethod annotate-tree :default [ctx tree]
  (annotate-vec ctx tree))

(defmethod annotate-tree ::annotate-tree-single-value [ctx tree]
  [ctx tree])

(defmethod annotate-tree :table_or_query_name [ctx tree]
  [(assoc ctx :current-table (first (filterv string? (flatten tree))))
   tree])

(defmethod annotate-tree :correlation_name [ctx tree]
  [(assoc ctx :current-table (first (filterv string? (flatten tree))))
   tree])

(defmethod annotate-tree :column_reference [ctx tree]
  [(update ctx :columns conj (filterv string? (flatten tree)))
   tree])

(defmethod annotate-tree :table_reference_list [ctx tree]
  (annotate-vec
   (fn [[ctx tree]]
     [(if-let [current-table (:current-table ctx)]
        (update ctx :tables conj current-table)
        ctx)
      tree])
   ctx
   tree))

(defmethod annotate-tree :query_expression [ctx tree]
  (let [new-ctx {:tables #{} :columns #{}}
        [{:keys [current-table tables columns]} tree] (annotate-vec new-ctx tree)
        correlated-columns (set (for [c columns
                                      :when (and (next c) (not (contains? tables (first c))))]
                                  c))
        scope (when current-table
                {::scope {:tables tables
                          :columns columns
                          :correlated-columns correlated-columns}})]
    [ctx (with-meta tree (merge (meta tree) scope))]))

(comment
  (time
   (parse-sql2011
    "SELECT * FROM user WHERE user.id = TIME '20:00:00.000' ORDER BY id DESC"
    :start :direct_sql_data_statement))

  (time
   (parse
    "SELECT * FROM user WHERE user.id = TIME '20:00:00.000' ORDER BY id DESC"))

  (time
   (parse-sql2011
    "TIME '20:00:00.000'"
    :start :literal))

  (time
   (parse-sql2011
    "TIME (3) WITH TIME ZONE"
    :start :data_type))

  (parse-sql2011
   "GRAPH_TABLE ( myGraph,
    MATCH
      (Creator IS Person WHERE Creator.email = :email1)
      -[ IS Created ]->
      (M IS Message)
      <-[ IS Commented ]-
      (Commenter IS Person WHERE Commenter.email = :email2)
    COLUMNS ( M.creationDate, M.content )
    )"
   :start :graph_table)

  (time
   (parse-sql2011
    "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)"
    :start :direct_sql_data_statement))

  (time
   (parse-sql2011
    "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 ORDER BY 1"
    :start :direct_sql_data_statement)))

;; SQL:2011 official grammar:

;; https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html

;; Repository has both grammars for 1992, 1999, 2003, 2006, 2008, 2011
;; and 2016 and draft specifications for 1992, 1999, 2003, 2006 and
;; 2011: https://github.com/JakeWheat/sql-overview

;; See also Date, SQL and Relational Theory, p. 455-458, A Simplified
;; BNF Grammar

;; High level SQL grammar, from
;; https://calcite.apache.org/docs/reference.html

;; SQLite grammar:
;; https://github.com/bkiers/sqlite-parser/blob/master/src/main/antlr4/nl/bigo/sqliteparser/SQLite.g4
;; https://www.sqlite.org/lang_select.html

;; SQL BNF from the spec:
;; https://ronsavage.github.io/SQL/

;; PostgreSQL Antlr4 grammar:
;; https://github.com/tshprecher/antlr_psql/tree/master/antlr4

;; PartiQL: SQL-compatible access to relational, semi-structured, and nested data.
;; https://partiql.org/assets/PartiQL-Specification.pdf

;; SQL-99 Complete, Really
;; https://crate.io/docs/sql-99/en/latest/index.html

;; Why CockroachDB and PostgreSQL Are Compatible
;; https://www.cockroachlabs.com/blog/why-postgres/

;; GQL Parser
;; https://github.com/OlofMorra/GQL-parser

;; Graph database applications with SQL/PGQ
;; https://download.oracle.com/otndocs/products/spatial/pdf/AnD2020/AD_Develop_Graph_Apps_SQL_PGQ.pdf
