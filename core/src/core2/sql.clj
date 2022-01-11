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
;; - should really be modular and not single letfn.

(defn- extend-env [env {:keys [correlation-name] :as table}]
  (assoc env correlation-name table))

(defn analyze-query [query]
  (let [next-id (atom 0)
        ->id (memoize
              (fn [_]
                (swap! next-id inc)))]
    (letfn [(env [ag]
              (case (r/ctor ag)
                (:select_list
                 :where_clause
                 :group_by_clause
                 :having_clause
                 :order_by_clause) (dclo (r/parent ag))
                :qualified_join (-> (extend-env (env (r/parent ag)) (table (r/$ ag 1)))
                                    (extend-env (table (r/$ ag -2))))
                :lateral_derived_table (dcli (r/parent ag))
                (some-> (r/parent ag) (env))))
            (dcli [ag]
              (case (r/ctor ag)
                :table_expression (env (r/parent ag))
                :table_factor (if (= :parenthesized_joined_table (r/ctor (r/$ ag 1)))
                                (dcli (r/$ ag 1))
                                (extend-env (dcli (r/prev ag)) (table ag)))
                :cross_join (-> (extend-env (dcli (r/prev ag)) (table (r/$ ag 1)))
                                (extend-env (table (r/$ ag -1))))
                :qualified_join (-> (extend-env (dcli (r/prev ag)) (table (r/$ ag 1)))
                                    (extend-env (table (r/$ ag -2))))
                :natural_join (-> (extend-env (dcli (r/prev ag)) (table (r/$ ag 1)))
                                  (extend-env (table (r/$ ag -1))))
                (some-> (r/parent ag) (dcli))))
            (dclo [ag]
              (case (r/ctor ag)
                (:query_expression_body
                 :query_term
                 :query_primary
                 :table_expression) (dclo (r/$ ag 1))
                :query_specification (dclo (r/$ ag -1))
                :from_clause (dclo (r/$ ag 2))
                :table_reference_list (dcli (r/$ ag -1))))
            (ctei [ag]
              (case (r/ctor ag)
                :query_expression (cte-env (r/parent ag))
                :with_list_element (let [{:keys [query-name] :as cte} (cte ag)]
                                     (assoc (ctei (r/prev ag)) query-name cte))
                (some-> (r/parent ag) (ctei))))
            (cteo [ag]
              (case (r/ctor ag)
                :query_expression (cteo (r/$ ag 1))
                :with_clause (cteo (r/$ ag -1))
                :with_list (ctei (r/$ ag -1))
                nil))
            (cte-env [ag]
              (case (r/ctor ag)
                :query_expression_body (cteo (r/parent ag))
                :with_list_element (ctei (r/prev ag))
                (some-> (r/parent ag) (cte-env))))
            (cte [ag]
              :with_list_element {:query-name (table-or-query-name ag)
                                  :id (->id ag)})
            (ctes [ag]
              (case (r/ctor ag)
                :subquery []
                :with_list_element [(cte ag)]
                (r/use-attributes ctes into ag)))
            (identifier [ag]
              (case (r/ctor ag)
                (:table_name
                 :query_name
                 :correlation_name
                 :identifier) (identifier (r/$ ag 1))
                :regular_identifier (r/lexme ag 1)))
            (identifiers [ag]
              (case (r/ctor ag)
                (:column_reference
                 :basic_identifier_chain) (identifiers (r/$ ag 1))
                :identifier_chain (r/use-attributes identifier ag)))
            (table [ag]
              :table_factor (let [table-name (table-or-query-name ag)
                                  correlation-name (correlation-name ag)
                                  cte-id (get-in (cte-env ag) [table-name :id])]
                              (cond-> {:table-or-query-name (or table-name correlation-name)
                                       :correlation-name correlation-name
                                       :id (->id ag)}
                                cte-id (assoc :cte-id cte-id))))
            (tables [ag]
              (case (r/ctor ag)
                :subquery []
                :table_factor [(table ag)]
                (r/use-attributes tables into ag)))
            (column [ag]
              (case (r/ctor ag)
                :column_reference (let [identifiers (identifiers ag)
                                        qualified? (> (count identifiers) 1)
                                        table-id (when qualified?
                                                   (get-in (env ag) [(first identifiers) :id]))]
                                    (cond-> {:identifiers identifiers
                                             :qualified? qualified?}
                                      table-id (assoc :table-id table-id)))))
            (columns [ag]
              (case (r/ctor ag)
                :subquery []
                :column_reference [(column ag)]
                (r/use-attributes columns into ag)))
            (errs [ag]
              (case (r/ctor ag)
                :column_reference (let [{:keys [identifiers qualified? table-id]} (column ag)]
                                    (cond
                                      (not qualified?)
                                      [(format "XTDB requires fully-qualified columns: %s %s"
                                               (first identifiers) (->line-info-str ag))]

                                      (not table-id)
                                      [(format "Table not in scope: %s %s"
                                               (first identifiers) (->line-info-str ag))]))
                (r/use-attributes errs into ag)))
            (table-or-query-name [ag]
              (case (r/ctor ag)
                (:table_factor
                 :table_primary
                 :with_list_element) (table-or-query-name (r/$ ag 1))
                (:table_name
                 :query_name) (identifier ag)
                nil))
            (correlation-name [ag]
              (case (r/ctor ag)
                :table_factor (correlation-name (r/$ ag 1))
                :table_primary (or (correlation-name (r/$ ag -1))
                                   (correlation-name (r/$ ag -2))
                                   (table-or-query-name ag))
                :correlation_name (identifier ag)
                nil))
            (scope [ag]
              (case (r/ctor ag)
                :query_expression (let [ctes (ctes ag)
                                        tables (tables ag)]
                                    {:ctes (zipmap (map :query-name ctes) ctes)
                                     :tables (zipmap (map :correlation-name tables) tables)
                                     :columns (set (columns ag))})))
            (scopes [ag]
              (case (r/ctor ag)
                :query_expression (into [(scope ag)] (r/use-attributes scopes into ag) )
                (r/use-attributes scopes into ag)))]
      {:scopes (scopes (z/vector-zip query))
       :errs (errs (z/vector-zip query))})))

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
