(ns core2.sql
  (:require [clojure.java.io :as io]
            [clojure.walk :as w]
            [instaparse.core :as insta]))

(set! *unchecked-math* :warn-on-boxed)

;; Instaparse SQL:2011 parser generator from the official grammar:

(def parse-sql2011
  (insta/parser
   (io/resource "core2/sql/SQL2011.ebnf")
   :auto-whitespace (insta/parser "whitespace = #'\\s+' | #'\\s*--[^\r\n]*\\s*' | #'\\s*/[*].*?([*]/\\s*|$)'")
   :string-ci true))

(def ^:private delimiter-set
  #{[:colon ":"]
    [:quote "'"]
    [:double_quote "'"]
    [:period "."]
    [:comma ","]
    [:left_paren "("]
    [:right_paren ")"]
    [:left_bracket "["]
    [:right_bracket "]"]
    [:left_bracket_trigraph "??("]
    [:right_bracket_trigraph "??)"]
    [:left_brace "{"]
    [:right_brace "}"]})

(def ^:private keep-tag-set
  #{:character_string_literal
    :binary_string_literal
    :signed_numeric_literal
    :exact_numeric_literal
    :approximate_numeric_literal
    :boolean_literal
    :date_literal
    :time_literal
    :timestamp_literal
    :interval_literal
    :year_month_literal
    :day_time_literal
    :host_parameter_name
    :identifier
    :identifier_chain
    :character_string_type
    :binary_string_type
    :numeric_type
    :boolean_type
    :datetime_type
    :interval_type})

(defn cst->ast [x]
  (w/postwalk
   (fn [x]
     (if (vector? x)
       (let [;; remove delimiters
             x (filterv (complement delimiter-set) x)]
         ;; single child
         (if (and (= (count x) 2)
                  (vector? (second x)))
           (if (contains? keep-tag-set (first (second x)))
             ;; replace this with child
             (second x)
             ;; replace this with child with this name
             (into [(first x)] (rest (second x))))
           x))
       x))
   x))

(def parse
  (memoize
   (fn self
     ([s]
      (self s :query_expression))
     ([s start-rule]
      (cst->ast
       (parse-sql2011 s :start start-rule))))))

(comment
  (time
   (cst->ast
    (parse-sql2011
     "SELECT * FROM user WHERE user.id = TIME '20:00:00.000' ORDER BY id DESC"
     :start :query_expression)))

  (time
   (parse
    "SELECT * FROM user WHERE user.id = TIME '20:00:00.000' ORDER BY id DESC"))

  (time
   (cst->ast
    (parse-sql2011
     "TIME '20:00:00.000'"
     :start :literal)))

  (time
   (cst->ast
    (parse-sql2011
     "TIME (3) WITH TIME ZONE"
     :start :data_type)))

  (cst->ast
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
    :start :graph_table)))

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
