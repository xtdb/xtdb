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
    :exact_numeric_literal
    :boolean_literal
    :regular_identifier
    :delimited_identifier})

(defn simplify-ast [x]
  (w/postwalk
   (fn [x]
     ;; remove keywords
     (if (and (vector? x) (> (count x) 2))
       (filterv (complement string?) x)
       x))
   (w/postwalk
    (fn [x]
      (if (vector? x)
        ;; remove delimiters
        (let [x (filterv (complement delimiter-set) x)]
          ;; single child
          (if (and (= 2 (count x))
                   (vector? (second x)))
            (if (and (= 2 (count (second x)))
                     (not (contains? keep-tag-set (first (second x)))))
              ;; replace this with child with this name
              [(first x) (second (second x))]
              ;; replace this with child
              (second x))
            x))
        x))
    x)))

(def parse
  (memoize
   (fn self
     ([s]
      (self s :query_expression))
     ([s start-rule]
      (simplify-ast
       (parse-sql2011 s :start start-rule))))))

(comment
  (time
   (simplify-ast
    (parse-sql2011
     "SELECT * FROM user WHERE user.id = TIME '20:00:00.000' ORDER BY id DESC"
     :start :query_expression)))

  (time
   (parse
    "SELECT * FROM user WHERE user.id = TIME '20:00:00.000' ORDER BY id DESC"))

  (time
   (simplify-ast
    (parse-sql2011
     "TIME '20:00:00.000'"
     :start :literal))))

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
