(ns core2.sql
  (:require [clojure.java.io :as io]
            [clojure.zip :as zip]
            [core2.rewrite :as rew]
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
      (self s :directly_executable_statement))
     ([s start-rule]
      (parse-sql2011 s :start start-rule)))))

;; Rewrite to annotate variable scopes.

(declare ->root-annotation-ctx)

(defn- ->scope-annotation [{:keys [tables with columns] :as ctx}]
  (let [known-tables (set (for [{:keys [table-or-query-name correlation-name]} tables]
                            (or correlation-name table-or-query-name)))
        correlated-columns (set (for [c columns
                                      :when (and (next c) (not (contains? known-tables (first c))))]
                                  c))]
    {:tables tables
     :columns columns
     :with with
     :correlated-columns correlated-columns}))

(defn- swap-select-list-and-table-expression [query-specification]
  (let [[xs ys] (split-at (- (count query-specification) 2) query-specification)]
    (vec (concat xs (reverse ys)))))

(def ^:private root-annotation-rules
  {:table_primary
   (rew/->scoped {:table_name (rew/->text :table-or-query-name)
                  :query_name (rew/->text :table-or-query-name)
                  :correlation_name (rew/->text :correlation-name)}
                 (fn [loc old-ctx]
                   (rew/conj-ctx loc :tables (select-keys old-ctx [:table-or-query-name :correlation-name]))))

   :column_reference
   (rew/->before (fn [loc _]
                   (rew/conj-ctx loc :columns (rew/text-nodes loc))))

   :with_list_element
   (rew/->scoped {:query_name (rew/->text :query-name)}
                 (fn [loc {:keys [query-name] :as old-ctx}]
                   (rew/conj-ctx loc :with query-name)))

   ;; NOTE: this temporary swap is done so table_expression is walked
   ;; first, ensuring its variables are in scope by the time
   ;; select_list is walked.
   :query_specification
   (rew/->around (fn move-select-list-after-table-expression [loc _]
                   (zip/edit loc swap-select-list-and-table-expression))
                 (fn restore-select-list [loc _]
                   (zip/edit loc swap-select-list-and-table-expression)))

   :query_expression
   (rew/->scoped {}
                 (fn [_]
                   (->root-annotation-ctx))
                 (fn [loc old-ctx]
                   (zip/edit loc vary-meta assoc ::scope (->scope-annotation old-ctx))))})

(defn- ->root-annotation-ctx []
  {:tables #{} :columns #{} :with #{} :rules root-annotation-rules})

(defn annotate-tree [tree]
  (rew/rewrite-tree tree (->root-annotation-ctx)))

(comment
  (time
   (parse-sql2011
    "SELECT * FROM user WHERE user.id = TIME '20:00:00.000' ORDER BY id DESC"
    :start :directly_executable_statement))

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
    :start :directly_executable_statement))

  (time
   (parse-sql2011
    "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 ORDER BY 1"
    :start :directly_executable_statement)))

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
