(ns core2.sql
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
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
      (vary-meta (parse-sql2011 (str/trimr s) :start start-rule) assoc ::sql s)))))

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

(defn- ->src-str [loc]
  (let [{:core2.sql/keys [sql]} (meta (z/root loc))
        {:instaparse.gll/keys [start-index end-index]} (meta (z/node loc))
        start-index (skip-whitespace sql start-index)]
    (subs sql start-index end-index)))

;; Very rough draft attribute grammar for SQL semantics.

;; TODO:
;; - too complex threading logic.
;; - unclear?
;; - mutable ids, use references instead?
;; - join tables should have proper env calculated and not use local-tables.

(defn- enter-env-scope [env]
  (cons {} env))

(defn- extend-env [[s & ss :as env] k v]
  (cons (update s k conj v) ss))

(defn- local-env [[s]]
  s)

(defn- parent-env [[_ & ss]]
  ss)

(defn- find-decl [[s & ss :as env] k]
  (when s
    (or (first (get s k))
        (recur ss k))))

;; Attributes

(declare cte-env env)

;; Ids

(defn- id [ag]
  (case (r/ctor ag)
    :table_primary (if (= :parenthesized_joined_table (r/ctor (r/$ ag 1)))
                     (id (z/prev ag))
                     (inc ^long (id (z/prev ag))))
    (:query_expression
     :with_list_element) (inc ^long (id (z/prev ag)))
    (or (some-> (z/prev ag) (id)) 0)))

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
    (:grouping_column_reference
     :column_reference
     :basic_identifier_chain) (identifiers (r/$ ag 1))
    (:column_name_list
     :identifier_chain)
    (letfn [(step [_ ag]
              (case (r/ctor ag)
                :regular_identifier [(r/lexeme ag 1)]
                []))]
      ((r/full-td-tu (r/mono-tuz step)) ag))))

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

(defn- cte [ag]
  (case (r/ctor ag)
    :with_list_element {:query-name (table-or-query-name ag)
                        :id (id ag)}))

;; Inherited
(defn- ctei [ag]
  (case (r/ctor ag)
    :query_expression (enter-env-scope (cte-env (r/parent ag)))
    :with_list_element (let [cte-env (ctei (r/left-or-parent ag))
                             {:keys [query-name] :as cte} (cte ag)]
                         (extend-env cte-env query-name cte))
    (r/inherit ag)))

;; Synthesised
(defn- cteo [ag]
  (case (r/ctor ag)
    :query_expression (if (= :with_clause (r/ctor (r/$ ag 1)))
                        (cteo (r/$ ag 1))
                        (ctei ag))
    :with_clause (cteo (r/$ ag -1))
    :with_list (ctei (r/$ ag -1))))

(defn- cte-env [ag]
  (case (r/ctor ag)
    :query_expression (cteo ag)
    :with_list_element (if (= "RECURSIVE" (r/lexeme (r/parent (r/parent ag)) 2))
                         (cteo (r/parent ag))
                         (ctei (r/left-or-parent ag)))
    (r/inherit ag)))

;; Tables

(defn- table [ag]
  (case (r/ctor ag)
    :table_primary (when-not (= :parenthesized_joined_table (r/ctor (r/$ ag 1)))
                     (let [table-name (table-or-query-name ag)
                           correlation-name (correlation-name ag)
                           cte-id (:id (find-decl (cte-env ag) table-name))]
                       (cond-> {:table-or-query-name (or table-name correlation-name)
                                :correlation-name correlation-name
                                :id (id ag)}
                         cte-id (assoc :cte-id cte-id))))))

(defn- local-tables [ag]
  (letfn [(step [_ ag]
            (case (r/ctor ag)
              :table_primary (if (= :parenthesized_joined_table (r/ctor (r/$ ag 1)))
                               (local-tables (r/$ ag 1))
                               [(table ag)])
              :subquery []
              nil))]
    ((r/stop-td-tu (r/mono-tuz step)) ag)))

;; Inherited
(defn- dcli [ag]
  (case (r/ctor ag)
    :query_expression (enter-env-scope (env (r/parent ag)))
    (:table_factor
     :cross_join
     :natural_join
     :qualified_join) (reduce (fn [acc {:keys [correlation-name] :as table}]
                                (extend-env acc correlation-name table))
                              (dcli (r/left-or-parent ag))
                              (local-tables ag))
    (r/inherit ag)))

;; Synthesised
(defn- dclo [ag]
  (case (r/ctor ag)
    :query_expression (if (= :with_clause (r/ctor (r/$ ag 1)))
                        (dclo (r/$ ag 2))
                        (dclo (r/$ ag 1)))
    (:query_expression_body
     :query_term
     :query_primary
     :table_expression) (dclo (r/$ ag 1))
    :query_specification (dclo (r/$ ag -1))
    :from_clause (dclo (r/$ ag 2))
    :table_reference_list (dcli (r/$ ag -1))))

(defn- env [ag]
  (case (r/ctor ag)
    (:query_expression
     :query_specification) (dclo ag)
    :from_clause (parent-env (dclo ag))
    (:collection_derived_table
     :join_condition
     :lateral_derived_table) (dcli (r/parent ag))
    (r/inherit ag)))

;; Column references

(defn- grouping-column-references [ag]
  (letfn [(step [_ ag]
            (case (r/ctor ag)
              :grouping_column_reference [(identifiers ag)]
              []))]
    ((r/full-td-tu (r/mono-tuz step)) ag)))

(defn- group-env [ag]
  (case (r/ctor ag)
    :query_specification (letfn [(step [_ ag]
                                   (case (r/ctor ag)
                                     :aggregate_function [[]]
                                     :group_by_clause [(grouping-column-references ag)]
                                     :subquery []
                                     nil))]
                           (cond-> {:grouping-columns (last (sort-by count ((r/stop-td-tu (r/mono-tuz step)) ag)))
                                    :group-column-reference-type :ordinary
                                    :column-reference-type :ordinary}))
    (:select_list
     :having_clause) (let [{:keys [grouping-columns] :as group-env} (group-env (r/parent ag))]
                       (cond-> group-env
                         grouping-columns (assoc :group-column-reference-type :group-invariant
                                                 :column-reference-type :invalid-group-invariant)))
    :aggregate_function (assoc (group-env (r/parent ag))
                               :column-reference-type :within-group-varying)
    (r/inherit ag)))

(defn- column-reference [ag]
  (case (r/ctor ag)
    :column_reference (let [identifiers (identifiers ag)
                            qualified? (> (count identifiers) 1)
                            table-id (when qualified?
                                       (:id (find-decl (env ag) (first identifiers))))
                            {:keys [grouping-columns
                                    group-column-reference-type
                                    column-reference-type]} (group-env ag)
                            column-reference-type (if (contains? (set grouping-columns) identifiers)
                                                    group-column-reference-type
                                                    column-reference-type)]
                        (cond-> {:identifiers identifiers
                                 :qualified? qualified?
                                 :type column-reference-type}
                          table-id (assoc :table-id table-id)))))

(defn- local-column-references [ag]
  (letfn [(step [_ ag]
            (case (r/ctor ag)
              :column_reference [(column-reference ag)]
              :subquery []
              nil))]
    ((r/stop-td-tu (r/mono-tuz step)) ag)))

;; Errors

(defn- local-names [[s :as env] k]
  (->> (mapcat val s)
       (map k)))

(defn- check-duplicates [label ag xs]
  (vec (for [[x ^long freq] (frequencies xs)
             :when (> freq 1)]
         (format (str label " duplicated: %s %s")
                 x (->line-info-str ag)))))

(defn- check-unsigned-integer [label ag]
  (when-not (r/zmatch ag
              [:signed_numeric_literal
               [:exact_numeric_literal
                [:unsigned_integer _]]] true
              [:host_parameter_name _] true)
    [(format (str label " must be an integer: %s %s")
             (->src-str ag) (->line-info-str ag))]))

(defn- check-aggregate-or-subquery [label ag]
  (letfn [(step [_ inner-ag]
            (case (r/ctor inner-ag)
              :set_function_specification
              [(format (str label " cannot contain aggregate functions: %s %s")
                       (->src-str ag) (->line-info-str ag))]
              :query_expression
              [(format (str label " cannot contain nested queries: %s %s")
                       (->src-str ag) (->line-info-str ag))]
              nil))]
    ((r/stop-td-tu (r/mono-tuz step)) ag)))

(defn- errs [ag]
  (letfn [(step [_ ag]
            (case (r/ctor ag)
              :table_reference_list (check-duplicates "Table variable" ag
                                                      (local-names (dclo ag) :correlation-name))
              :with_list (check-duplicates "CTE query name" ag
                                           (local-names (cteo ag) :query-name))
              :column_name_list (check-duplicates "Column name" ag (identifiers ag))
              :column_reference (let [{:keys [identifiers qualified? table-id type] :as cr} (column-reference ag)]
                                  (cond
                                    (not qualified?)
                                    [(format "XTDB requires fully-qualified columns: %s %s"
                                             (->src-str ag) (->line-info-str ag))]

                                    (not table-id)
                                    [(format "Table not in scope: %s %s"
                                             (first identifiers) (->line-info-str ag))]

                                    (= :invalid-group-invariant type)
                                    [(format "Column reference is not a grouping column: %s %s"
                                             (->src-str ag) (->line-info-str ag))]))
              :aggregate_function (check-aggregate-or-subquery "Aggregate functions" ag)
              :sort_specification (check-aggregate-or-subquery "Sort specifications" ag)
              :fetch_first_row_count (check-unsigned-integer "Fetch first row count" (r/$ ag 1))
              :offset_row_count (check-unsigned-integer "Offset row count" (r/$ ag 1))
              []))]
    ((r/full-td-tu (r/mono-tuz step)) ag)))

;; Scopes

(defn- scope [ag]
  (case (r/ctor ag)
    :query_expression (let [parent-id (:id (scope (r/parent ag)))]
                        (cond-> {:id (id ag)
                                 :ctes (local-env (cteo ag))
                                 :tables (local-env (dclo ag))
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
  (r/with-memoized-attributes [#'id
                               #'ctei
                               #'cteo
                               #'cte-env
                               #'dcli
                               #'dclo
                               #'env
                               #'group-env
                               #'column-reference
                               #'scope]
    #(let [ag (z/vector-zip query)]
       {:scopes (scopes ag)
        :errs (errs ag)})))

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

;; Dependent join symbols:  ⧑ ⧔ ⯈
