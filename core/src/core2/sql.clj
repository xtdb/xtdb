(ns core2.sql
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.zip :as zip]
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

(defprotocol Rule
  (--> [_ loc])
  (<-- [_ loc]))

(defn- ->ctx [loc]
  (first (::ctx (meta loc))))

(defn- zip-dispatch [loc direction-fn]
  (let [tree (zip/node loc)
        rule-kw (when (vector? tree)
                  (first tree))]
    (if-let [rule (get-in (meta loc) [::ctx 0 :rules rule-kw])]
      (direction-fn rule loc)
      loc)))

(defn- vary-ctx [loc f & args]
  (vary-meta loc update-in [::ctx 0] (fn [ctx]
                                       (apply f ctx args))))

(defn- conj-ctx [loc k v]
  (vary-ctx loc update k conj v))

(defn- pop-ctx [loc]
  (vary-meta loc update ::ctx (comp vec rest)))

(defn- push-ctx
  ([loc rule-overrides]
   (push-ctx loc rule-overrides (->ctx loc)))
  ([loc rule-overrides ctx]
   (vary-meta loc update ::ctx (partial into [(update ctx :rules merge rule-overrides)]))))

(defn- text-nodes [loc]
  (filterv string? (flatten (zip/node loc))))

(defn- ->text-rule [kw]
  (reify Rule
    (--> [_ loc]
      (vary-ctx loc assoc kw (str/join (text-nodes loc))))

    (<-- [_ loc] loc)))

(defn rewrite-tree [tree ctx]
  (loop [loc (vary-meta (zip/vector-zip tree) assoc ::ctx [ctx])]
    (if (zip/end? loc)
      (zip/root loc)
      (let [loc (zip-dispatch loc -->)]
        (recur (cond
                 (zip/branch? loc)
                 (zip/down loc)

                 (seq (zip/rights loc))
                 (zip/right (zip-dispatch loc <--))

                 :else
                 (loop [p loc]
                   (let [p (zip-dispatch p <--)]
                     (if-let [parent (zip/up p)]
                       (if (seq (zip/rights parent))
                         (zip/right (zip-dispatch parent <--))
                         (recur parent))
                       [(zip/node p) :end])))))))))

(defn- ->root-annotation-ctx [root-annotation-rules]
  {:tables #{} :columns #{} :with #{} :rules root-annotation-rules})

(declare root-annotation-rules)

(def ^:private root-annotation-rules
  {:table_primary
   (reify Rule
     (--> [_ loc]
       (push-ctx loc {:table_or_query_name (->text-rule :table-or-query-name)
                      :correlation_name (->text-rule :correlation-name)}))

     (<-- [_ loc]
       (-> (pop-ctx loc)
           (conj-ctx :tables (select-keys (->ctx loc) [:table-or-query-name :correlation-name])))))

   :column_reference
   (reify Rule
     (--> [_ loc]
       (conj-ctx loc :columns (text-nodes loc)))

     (<-- [_ loc] loc))

   :with_list_element
   (reify Rule
     (--> [_ loc]
       (push-ctx loc {:query_name (->text-rule :query-name)}))

     (<-- [_ loc]
       (-> (pop-ctx loc)
           (conj-ctx :with (:query-name (->ctx loc))))))

   :query_expression
   (reify Rule
     (--> [_ loc]
       (push-ctx loc {} (->root-annotation-ctx root-annotation-rules)))

     (<-- [_ loc]
       (let [{:keys [tables with columns]} (->ctx loc)
             known-tables (set (for [{:keys [table-or-query-name correlation-name]} tables]
                                 (or correlation-name table-or-query-name)))
             correlated-columns (set (for [c columns
                                           :when (and (next c) (not (contains? known-tables (first c))))]
                                       c))
             scope {:tables tables
                    :columns columns
                    :with with
                    :correlated-columns correlated-columns}]
         (-> (pop-ctx loc)
             (zip/edit vary-meta assoc ::scope scope)))))})

(defn annotate-tree [tree]
  (rewrite-tree tree (->root-annotation-ctx root-annotation-rules)))

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
