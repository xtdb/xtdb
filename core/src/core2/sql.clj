(ns core2.sql
  (:require [clojure.java.io :as io]
            [clojure.zip :as z]
            [core2.rewrite :as r]
            [instaparse.core :as insta]
            [instaparse.failure]))

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
      (vary-meta (parse-sql2011 s :start start-rule) assoc ::sql s)))))

(defn- skip-whitespace ^long [^String s ^long n]
  (if (Character/isWhitespace (.charAt s n))
    (recur s (inc n))
    n))

(defn- ->line-info-str [loc]
  (let [{:core2.sql/keys [sql]} (meta (z/root loc))
        {:instaparse.gll/keys [start-index]} (meta (z/node loc))
        start-index (skip-whitespace sql start-index)
        {:keys [line column]} (instaparse.failure/index->line-column start-index sql)]
    (format "at line %d, column %d" line column)))

;; Very rough draft attribute grammar for SQL semantics.

;; TODO:
;; - too complex threading logic.
;; - unclear?
;; - mutable ids, use references instead?
;; - join tables should have proper env calculated.

(defn- extend-env [env {:keys [correlation-name] :as table}]
  (assoc env correlation-name table))

(defn- check-duplicates [label ag xs]
  (vec (for [[x ^long freq] (frequencies xs)
             :when (> freq 1)]
         (format (str label " duplicated: %s %s")
                 x (->line-info-str ag)))))

;; Attributes

;; Ids

(def ^:private ^:dynamic *next-id* (atom 0))

(defn- ->id [_]
  (swap! *next-id* inc))

;; Identifiers

(defn- identifier [ag]
  (case (r/ctor ag)
    (:table_name
     :query_name
     :correlation_name
     :column_name
     :identifier) (identifier (r/$ ag 1))
    :regular_identifier (r/lexeme ag 1)
    nil))

(defn- identifiers [ag]
  (case (r/ctor ag)
    (:column_reference
     :basic_identifier_chain) (identifiers (r/$ ag 1))
    (:column_name_list
     :identifier_chain)
    (letfn [(step [_ ag]
              (case (r/ctor ag)
                :regular_identifier [(r/lexeme ag 1)]
                []))]
      ((r/full-td-tu (r/mono-tuz step)) ag))))

(declare env)

(defn- column-reference [ag]
  (case (r/ctor ag)
    :column_reference (let [identifiers (identifiers ag)
                            qualified? (> (count identifiers) 1)
                            table-id (when qualified?
                                       (get-in (env ag) [(first identifiers) :id]))]
                        (cond-> {:identifiers identifiers
                                 :qualified? qualified?}
                          table-id (assoc :table-id table-id)))))

(defn- local-column-references [ag]
  (letfn [(step [_ ag]
            (case (r/ctor ag)
              :column_reference [(column-reference ag)]
              :subquery []
              nil))]
    ((r/stop-td-tu (r/mono-tuz step)) ag)))

(defn- table-or-query-name [ag]
  (case (r/ctor ag)
    (:table_factor
     :table_primary
     :with_list_element) (table-or-query-name (r/$ ag 1))
    (:table_name
     :query_name) (identifier ag)
    nil))

(defn- correlation-name [ag]
  (case (r/ctor ag)
    :table_factor (correlation-name (r/$ ag 1))
    :table_primary (or (correlation-name (r/$ ag -1))
                       (correlation-name (r/$ ag -2))
                       (table-or-query-name ag))
    :correlation_name (identifier ag)
    nil))

;; CTEs

(declare cteo ctei)

(defn- cte [ag]
  (case (r/ctor ag)
    :with_list_element {:query-name (table-or-query-name ag)
                        :id (->id ag)}))
(defn- local-ctes [ag]
  (letfn [(step [_ ag]
            (case (r/ctor ag)
              :with_list_element [(cte ag)]
              :subquery []
              nil))]
    ((r/stop-td-tu (r/mono-tuz step)) ag)))

(defn- cte-env [ag]
  (case (r/ctor ag)
    :query_expression_body (cteo (r/parent ag))
    :with_list_element (if (= "RECURSIVE" (r/lexeme (r/parent (r/parent ag)) 2))
                         (cteo (r/parent ag))
                         (ctei (r/prev ag)))
    (r/inherit ag)))

;; Inherited
(defn- ctei [ag]
  (case (r/ctor ag)
    :query_expression (cte-env (r/parent ag))
    :with_list_element (let [{:keys [query-name] :as cte} (cte ag)]
                         (assoc (ctei (r/prev ag)) query-name cte))
    (r/inherit ag)))

;; Synthesised
(defn- cteo [ag]
  (case (r/ctor ag)
    :query_expression (if (= :with_clause (r/ctor (r/$ ag 1)))
                        (cteo (r/$ ag 1))
                        (ctei ag))
    :with_clause (cteo (r/$ ag -1))
    :with_list (ctei (r/$ ag -1))
    nil))

;; Tables

(defn- table [ag]
  (case (r/ctor ag)
    :table_factor (when-not (= :parenthesized_joined_table (r/ctor (r/$ ag 1)))
                    (let [table-name (table-or-query-name ag)
                          correlation-name (correlation-name ag)
                          cte-id (get-in (cte-env ag) [table-name :id])]
                      (cond-> {:table-or-query-name (or table-name correlation-name)
                               :correlation-name correlation-name
                               :id (->id ag)}
                        cte-id (assoc :cte-id cte-id))))))

(defn- local-tables [ag]
  (letfn [(step [_ ag]
            (case (r/ctor ag)
              :table_factor (if (= :parenthesized_joined_table (r/ctor (r/$ ag 1)))
                              (local-tables (r/$ ag 1))
                              [(table ag)])
              :subquery []
              nil))]
    ((r/stop-td-tu (r/mono-tuz step)) ag)))

(declare dclo dcli)

(defn- env [ag]
  (case (r/ctor ag)
    (:select_list
     :where_clause
     :group_by_clause
     :having_clause
     :order_by_clause) (dclo (r/parent ag))
    (:collection_derived_table
     :join_condition
     :lateral_derived_table) (dcli (r/parent ag))
    (r/inherit ag)))

;; Inherited
(defn- dcli [ag]
  (case (r/ctor ag)
    :table_expression (env (r/parent ag))
    :table_factor (if (= :parenthesized_joined_table (r/ctor (r/$ ag 1)))
                    (dcli (r/$ ag 1))
                    (extend-env (dcli (r/prev ag)) (table ag)))
    (:cross_join
     :natural_join
     :qualified_join) (reduce extend-env (dcli (r/prev ag)) (local-tables ag))
    (r/inherit ag)))

;; Synthesised
(defn- dclo [ag]
  (case (r/ctor ag)
    (:query_expression_body
     :query_term
     :query_primary
     :table_expression) (dclo (r/$ ag 1))
    :query_specification (dclo (r/$ ag -1))
    :from_clause (dclo (r/$ ag 2))
    :table_reference_list (dcli (r/$ ag -1))))

;; Errors

(defn- errs [ag]
  (letfn [(step [_ ag]
            (case (r/ctor ag)
              :table_reference_list (check-duplicates "Table variable" ag (map :correlation-name (local-tables ag)))
              :with_list (check-duplicates "CTE query name" ag (map :query-name (local-ctes ag)))
              :column_name_list (check-duplicates "Column name" ag (identifiers ag))
              :column_reference (let [{:keys [identifiers qualified? table-id]} (column-reference ag)]
                                  (cond
                                    (not qualified?)
                                    [(format "XTDB requires fully-qualified columns: %s %s"
                                             (first identifiers) (->line-info-str ag))]

                                    (not table-id)
                                    [(format "Table not in scope: %s %s"
                                             (first identifiers) (->line-info-str ag))]))
              []))]
    ((r/full-td-tu (r/mono-tuz step)) ag)))

;; Scopes

(defn- scope [ag]
  (case (r/ctor ag)
    :query_expression (let [id (->id ag)
                            parent-id (:id (scope (r/parent ag)))
                            ctes (local-ctes ag)
                            tables (local-tables ag)]
                        (cond-> {:id id
                                 :ctes (zipmap (map :query-name ctes) ctes)
                                 :tables (zipmap (map :correlation-name tables) tables)
                                 :columns (set (local-column-references ag))}
                          parent-id (assoc :parent-id parent-id)))
    (r/inherit ag)))
(defn- scopes [ag]
  (letfn [(step [_ ag]
            (case (r/ctor ag)
              :query_expression [(scope ag)]
              []))]
    ((r/full-td-tu (r/mono-tuz step)) ag)))

;; API

(defn analyze-query [query]
  (r/with-memoized-attributes [#'->id
                               #'local-ctes
                               #'cte-env
                               #'ctei
                               #'cteo
                               #'local-tables
                               #'env
                               #'dclo
                               #'dcli]
    #(binding [*next-id* (atom 0)]
       (let [ag (z/vector-zip query)]
         {:scopes (scopes ag)
          :errs (errs ag)}))))

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
